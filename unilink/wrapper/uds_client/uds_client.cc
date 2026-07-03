/*
 * Copyright 2025 Jinwoo Sung
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "unilink/wrapper/uds_client/uds_client.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/config/uds_config.hpp"
#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/transport/uds/uds_client.hpp"

namespace unilink {
namespace wrapper {

struct UdsClient::Impl {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  std::string socket_path_;
  std::shared_ptr<interface::Channel> channel_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};
  std::thread external_thread_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::vector<std::promise<bool>> pending_promises_;
  std::atomic<bool> started_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  MessageHandler data_handler_{nullptr};
  BatchMessageHandler data_batch_handler_{nullptr};
  ConnectionHandler connect_handler_{nullptr};
  ConnectionHandler disconnect_handler_{nullptr};
  ErrorHandler error_handler_{nullptr};
  std::function<void(size_t)> bp_handler_{nullptr};
  MessageHandler message_handler_{nullptr};
  BatchMessageHandler message_batch_handler_{nullptr};

  std::shared_ptr<framer::IFramer> framer_{nullptr};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  std::atomic<bool> auto_start_ = false;
  std::chrono::milliseconds retry_interval_{base::constants::DEFAULT_RETRY_INTERVAL_MS};
  int max_retries_ = -1;
  std::chrono::milliseconds connection_timeout_{5000};
  size_t backpressure_threshold_ = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  // Atomic rather than mutex-guarded: read from the send()/send_line() fast
  // path on arbitrary caller threads while the setter can be called
  // concurrently from any other thread (#436).
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};

  explicit Impl(const std::string& socket_path) : socket_path_(socket_path), started_(false) {}

  Impl(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc)
      : socket_path_(socket_path),
        external_ioc_(std::move(external_ioc)),
        use_external_context_(external_ioc_ != nullptr),
        manage_external_context_(false),
        started_(false) {}

  explicit Impl(std::shared_ptr<interface::Channel> channel)
      : socket_path_(""), channel_(std::move(channel)), started_(false) {
    setup_internal_handlers();
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void fulfill_all_locked(bool value) {
    for (auto& p : pending_promises_) {
      try {
        p.set_value(value);
      } catch (...) {
      }
    }
    pending_promises_.clear();
  }

  void flush_batches() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!data_batch_queue_.empty()) {
      auto handler = data_batch_handler_;
      auto batch = std::move(data_batch_queue_);
      data_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        handler(batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = message_batch_handler_;
      auto batch = std::move(message_batch_queue_);
      message_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        handler(batch);
        lock.lock();
      }
    }
    if (batch_timer_) {
      batch_timer_->cancel();
    }
  }

  void schedule_batch_timer() {
    if (!batch_timer_) return;
    batch_timer_->expires_after(max_batch_latency_);
    batch_timer_->async_wait(
        [this, weak_alive = std::weak_ptr<bool>(alive_marker_)](const boost::system::error_code& ec) {
          if (ec) return;
          auto alive = weak_alive.lock();
          if (!alive) return;
          flush_batches();
        });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (channel_ && channel_->is_connected()) {
      started_.store(true);
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }

    std::promise<bool> p;
    auto f = p.get_future();
    pending_promises_.emplace_back(std::move(p));

    if (started_.load()) {
      return f;
    }

    if (!channel_) {
      config::UdsClientConfig cfg;
      cfg.socket_path = socket_path_;
      cfg.retry_interval_ms = static_cast<unsigned>(retry_interval_.count());
      cfg.max_retries = max_retries_;
      cfg.connection_timeout_ms = static_cast<unsigned>(connection_timeout_.count());
      cfg.backpressure_threshold = backpressure_threshold_;
      cfg.backpressure_strategy = backpressure_strategy_;

      if (use_external_context_) {
        channel_ = factory::ChannelFactory::create(cfg, external_ioc_);
        if (manage_external_context_ && !external_thread_.joinable()) {
          if (external_ioc_ && external_ioc_->stopped()) {
            external_ioc_->restart();
          }
          work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
              boost::asio::make_work_guard(*external_ioc_));
          external_thread_ = std::thread([this]() {
            try {
              external_ioc_->run();
            } catch (...) {
            }
          });
        }
      } else {
        channel_ = factory::ChannelFactory::create(cfg);
      }
      setup_internal_handlers();
    }
    started_.store(true);

    lock.unlock();  // UNLOCK BEFORE START
    channel_->start();
    lock.lock();

    return f;
  }

  void stop() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!started_.load()) {
      fulfill_all_locked(false);
      return;
    }
    started_.store(false);
    bp_cv_.notify_all();

    if (batch_timer_) {
      batch_timer_->cancel();
      batch_timer_.reset();
    }

    // RELEASE LOCK before calling channel_->stop() because it might trigger
    // callbacks that try to acquire this same lock (e.g., on_state -> fulfill_all)
    if (channel_) {
      channel_->on_bytes(nullptr);
      channel_->on_state(nullptr);
      lock.unlock();
      channel_->stop();
      lock.lock();
    }

    if (work_guard_) {
      work_guard_.reset();
    }

    if (use_external_context_ && manage_external_context_ && external_ioc_) {
      external_ioc_->stop();
    }

    if (external_thread_.joinable()) {
      if (std::this_thread::get_id() != external_thread_.get_id()) {
        lock.unlock();  // RELEASE LOCK BEFORE JOINING
        external_thread_.join();
        lock.lock();  // RE-ACQUIRE
      } else {
        external_thread_.detach();
      }
    }

    fulfill_all_locked(false);
    channel_.reset();
    if (framer_) {
      framer_->reset();
    }
  }

  bool send(std::string_view data) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) return send_blocking(data);
    return try_send(data);
  }

  // channel_->on_backpressure() calls bp_cv_.notify_all() from the transport's io_context
  // thread without holding bp_mutex_ (backpressure_active_ is a plain atomic on the transport
  // side, not guarded by bp_mutex_ at all). That makes a classic lost-wakeup race possible: a
  // waiter can check the predicate, find it still blocking, and be in the process of
  // registering to wait when the notify fires - in the rare case that race is lost, an
  // unbounded wait() would block forever. Poll with a bounded timeout instead so a missed
  // notify only costs a short delay rather than a permanent hang (see #427, #431).
  void wait_for_backpressure_clear(std::unique_lock<std::mutex>& bp_lock) {
    auto predicate = [this] {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      return !started_.load() || !channel_ || !channel_->is_connected() || !channel_->is_backpressure_active();
    };
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), predicate)) {
    }
  }

  bool send_move(std::vector<uint8_t>&& data) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) {
      std::unique_lock<std::mutex> bp_lock(bp_mutex_);
      wait_for_backpressure_clear(bp_lock);
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
      return channel_->async_write_move(std::move(data));
    }
    return try_send_move(std::move(data));
  }

  bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (!data || data->empty()) return false;
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) {
      std::unique_lock<std::mutex> bp_lock(bp_mutex_);
      wait_for_backpressure_clear(bp_lock);
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
      return channel_->async_write_shared(std::move(data));
    }
    return try_send_shared(std::move(data));
  }

  bool send_line(std::string_view line) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) return send_line_blocking(line);
    return try_send_line(line);
  }

  bool try_send_line(std::string_view line) { return try_send(std::string(line) + "\n"); }

  bool send_blocking(std::string_view data) {
    std::unique_lock<std::mutex> bp_lock(bp_mutex_);
    wait_for_backpressure_clear(bp_lock);
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
    return channel_->async_write_copy(
        memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  }

  bool try_send(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_ && channel_->is_connected()) {
      return channel_->async_try_write_copy(
          memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
    }
    return false;
  }

  bool try_send_move(std::vector<uint8_t>&& data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_ && channel_->is_connected()) {
      return channel_->async_try_write_move(std::move(data));
    }
    return false;
  }

  bool try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (!data || data->empty()) return false;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_ && channel_->is_connected()) {
      return channel_->async_try_write_shared(std::move(data));
    }
    return false;
  }

  bool send_line_blocking(std::string_view line) { return send_blocking(std::string(line) + "\n"); }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return channel_ ? channel_->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_) channel_->reset_stats();
  }

  void setup_internal_handlers() {
    if (!channel_) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(channel_->get_executor());

    std::weak_ptr<bool> weak_alive = alive_marker_;

    channel_->on_state([this, weak_alive](base::LinkState state) {
      auto alive = weak_alive.lock();
      if (!alive) return;

      if (state == base::LinkState::Connected) {
        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(true);
          handler = connect_handler_;
        }
        if (handler) {
          handler(ConnectionContext(0));
        }
      } else if (state == base::LinkState::Error) {
        ErrorHandler handler;
        std::shared_ptr<interface::Channel> channel_snapshot;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          handler = error_handler_;
          channel_snapshot = channel_;
        }
        if (handler) {
          bool handled = false;
          if (auto transport = std::dynamic_pointer_cast<transport::UdsClient>(channel_snapshot)) {
            if (auto info = transport->last_error_info()) {
              handler(diagnostics::to_error_context(*info));
              handled = true;
            }
          }
          if (!handled) {
            handler(ErrorContext(ErrorCode::IoError, "Connection error"));
          }
        }
      } else if (state == base::LinkState::Closed || state == base::LinkState::Idle) {
        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          handler = disconnect_handler_;
        }
        if (handler) {
          handler(ConnectionContext(0));
        }
      }
    });

    channel_->on_bytes([this, weak_alive](memory::ConstByteSpan data) {
      auto alive = weak_alive.lock();
      if (!alive) return;

      std::shared_ptr<framer::IFramer> framer_to_push;
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (data_batch_handler_) {
        data_batch_queue_.emplace_back(0, memory::SafeDataBuffer(data));
        if (data_batch_queue_.size() >= max_batch_size_) {
          auto handler = data_batch_handler_;
          auto batch = std::move(data_batch_queue_);
          data_batch_queue_.clear();
          lock.unlock();
          handler(batch);
          lock.lock();
        } else if (data_batch_queue_.size() == 1) {
          schedule_batch_timer();
        }
      } else {
        MessageHandler handler = data_handler_;
        if (handler) {
          lock.unlock();
          handler(MessageContext(0, memory::SafeDataBuffer(data)));
          lock.lock();
        }
      }

      if (framer_) {
        framer_to_push = framer_;
      }
      lock.unlock();
      if (framer_to_push) framer_to_push->push_bytes(data);
    });

    channel_->on_backpressure([this, weak_alive](size_t queued) {
      bp_cv_.notify_all();
      auto alive = weak_alive.lock();
      if (!alive) return;
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        handler = bp_handler_;
      }
      if (handler) handler(queued);
    });
  }

  // Attach the stored message_handler_ or message_batch_handler_ to framer_->on_message().
  // Must be called with mutex_ already held.
  void attach_framer_callback() {
    if (!framer_) return;
    framer_->on_message([this](memory::ConstByteSpan msg) {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (message_batch_handler_) {
        message_batch_queue_.emplace_back(0, memory::SafeDataBuffer(msg));
        if (message_batch_queue_.size() >= max_batch_size_) {
          auto handler = message_batch_handler_;
          auto batch = std::move(message_batch_queue_);
          message_batch_queue_.clear();
          lock.unlock();
          handler(batch);
        } else if (message_batch_queue_.size() == 1) {
          schedule_batch_timer();
        }
        return;
      }

      MessageHandler handler = message_handler_;
      if (handler) {
        lock.unlock();
        handler(MessageContext(0, memory::SafeDataBuffer(msg)));
      }
    });
  }

  void set_framer(std::unique_ptr<framer::IFramer> framer) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    framer_ = std::shared_ptr<framer::IFramer>(std::move(framer));
    if (framer_ && (message_handler_ || message_batch_handler_)) attach_framer_callback();
  }

  void on_message(MessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_handler_ = std::move(handler);
    if (framer_) attach_framer_callback();
  }

  void on_message_batch(BatchMessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_batch_handler_ = std::move(handler);
    if (framer_) attach_framer_callback();
  }
};

UdsClient::UdsClient(const std::string& socket_path) : impl_(std::make_unique<Impl>(socket_path)) {}

UdsClient::UdsClient(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc)
    : impl_(std::make_unique<Impl>(socket_path, std::move(external_ioc))) {}

UdsClient::UdsClient(std::shared_ptr<interface::Channel> channel) : impl_(std::make_unique<Impl>(std::move(channel))) {}

UdsClient::~UdsClient() = default;

UdsClient::UdsClient(UdsClient&&) noexcept = default;
UdsClient& UdsClient::operator=(UdsClient&&) noexcept = default;

std::future<bool> UdsClient::start() { return impl_->start(); }

void UdsClient::stop() { impl_->stop(); }

bool UdsClient::send(std::string_view data) { return impl_->send(data); }
bool UdsClient::try_send(std::string_view data) { return impl_->try_send(data); }
bool UdsClient::send_move(std::vector<uint8_t>&& data) { return impl_->send_move(std::move(data)); }
bool UdsClient::try_send_move(std::vector<uint8_t>&& data) { return impl_->try_send_move(std::move(data)); }
bool UdsClient::send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->send_shared(std::move(data));
}
bool UdsClient::try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->try_send_shared(std::move(data));
}

bool UdsClient::send_line(std::string_view line) { return impl_->send_line(line); }
bool UdsClient::try_send_line(std::string_view line) { return impl_->try_send_line(line); }

bool UdsClient::send_blocking(std::string_view data) { return impl_->send_blocking(data); }

bool UdsClient::send_line_blocking(std::string_view line) { return impl_->send_line_blocking(line); }

bool UdsClient::connected() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->channel_ && impl_->channel_->is_connected();
}
RuntimeStats UdsClient::stats() const { return impl_->stats(); }
void UdsClient::reset_stats() { impl_->reset_stats(); }

UdsClient& UdsClient::on_data(MessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_handler_ = std::move(handler);
  return *this;
}

UdsClient& UdsClient::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = std::move(h);
  return *this;
}

UdsClient& UdsClient::on_connect(ConnectionHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connect_handler_ = std::move(handler);
  return *this;
}

UdsClient& UdsClient::on_disconnect(ConnectionHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->disconnect_handler_ = std::move(handler);
  return *this;
}

UdsClient& UdsClient::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->error_handler_ = std::move(h);
  return *this;
}

UdsClient& UdsClient::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->bp_handler_ = std::move(h);
  return *this;
}

UdsClient& UdsClient::framer(std::unique_ptr<framer::IFramer> f) {
  impl_->set_framer(std::move(f));
  return *this;
}

UdsClient& UdsClient::on_message(MessageHandler h) {
  impl_->on_message(std::move(h));
  return *this;
}

UdsClient& UdsClient::on_message_batch(BatchMessageHandler h) {
  impl_->on_message_batch(std::move(h));
  return *this;
}

UdsClient& UdsClient::auto_start(bool manage) {
  impl_->auto_start_.store(manage);
  if (impl_->auto_start_.load() && !impl_->started_.load()) {
    start();
  }
  return *this;
}

UdsClient& UdsClient::retry_interval(std::chrono::milliseconds interval) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->retry_interval_ = interval;
  if (impl_->channel_) {
    auto transport_client = std::dynamic_pointer_cast<transport::UdsClient>(impl_->channel_);
    if (transport_client) transport_client->set_retry_interval(static_cast<unsigned int>(interval.count()));
  }
  return *this;
}

UdsClient& UdsClient::max_retries(int max_retries) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_retries_ = max_retries;
  return *this;
}

UdsClient& UdsClient::connection_timeout(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connection_timeout_ = timeout;
  return *this;
}

UdsClient& UdsClient::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_threshold_ = threshold;
  return *this;
}

UdsClient& UdsClient::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_strategy_ = strategy;
  if (impl_->channel_) {
    auto transport_client = std::dynamic_pointer_cast<transport::UdsClient>(impl_->channel_);
    if (transport_client) transport_client->set_backpressure_strategy(strategy);
  }
  return *this;
}

UdsClient& UdsClient::manage_external_context(bool manage) {
  impl_->manage_external_context_.store(manage);
  return *this;
}

UdsClient& UdsClient::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

UdsClient& UdsClient::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
