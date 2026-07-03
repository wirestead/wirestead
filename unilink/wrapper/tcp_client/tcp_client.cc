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

#include "unilink/wrapper/tcp_client/tcp_client.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/config/tcp_client_config.hpp"
#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/transport/tcp_client/tcp_client.hpp"

namespace unilink {
namespace wrapper {

struct TcpClient::Impl {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  std::string host_;
  uint16_t port_;
  std::shared_ptr<interface::Channel> channel_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};
  std::jthread external_thread_;
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
  std::chrono::milliseconds idle_timeout_{0};
  IdleTimeoutAction idle_timeout_action_{IdleTimeoutAction::Reconnect};
  size_t backpressure_threshold_{base::constants::DEFAULT_BACKPRESSURE_THRESHOLD};
  // Atomic rather than mutex-guarded: read from the send()/send_line() fast
  // path on arbitrary caller threads while the setter can be called
  // concurrently from any other thread (#436).
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};
  bool tcp_no_delay_ = true;
  bool keep_alive_ = false;
  size_t send_buffer_size_ = 0;
  size_t receive_buffer_size_ = 0;

  Impl(const std::string& host, uint16_t port) : host_(host), port_(port), started_(false) {}

  Impl(const std::string& host, uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc)
      : host_(host),
        port_(port),
        external_ioc_(std::move(external_ioc)),
        use_external_context_(external_ioc_ != nullptr),
        manage_external_context_(false),
        started_(false) {}

  explicit Impl(std::shared_ptr<interface::Channel> channel)
      : host_(""), port_(0), channel_(std::move(channel)), started_(false) {
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
    pending_promises_.push_back(std::move(p));
    if (started_.load()) return f;

    if (!alive_marker_) {
      alive_marker_ = std::make_shared<bool>(true);
    }

    if (!channel_) {
      config::TcpClientConfig config;
      config.host = host_;
      config.port = port_;
      config.retry_interval_ms = static_cast<unsigned int>(retry_interval_.count());
      config.max_retries = max_retries_;
      config.connection_timeout_ms = static_cast<unsigned>(connection_timeout_.count());
      config.idle_timeout_ms = static_cast<unsigned>(idle_timeout_.count());
      config.idle_timeout_action = idle_timeout_action_;
      config.backpressure_threshold = backpressure_threshold_;
      config.backpressure_strategy = backpressure_strategy_;
      config.tcp_no_delay = tcp_no_delay_;
      config.keep_alive = keep_alive_;
      config.send_buffer_size = send_buffer_size_;
      config.receive_buffer_size = receive_buffer_size_;
      channel_ = factory::ChannelFactory::create(config, external_ioc_);
      setup_internal_handlers();
    }

    started_.store(true);
    channel_->start();
    if (use_external_context_.load() && manage_external_context_.load() && !external_thread_.joinable()) {
      if (external_ioc_ && external_ioc_->stopped()) {
        external_ioc_->restart();
      }
      work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          external_ioc_->get_executor());
      external_thread_ = std::jthread([this, ioc = external_ioc_](std::stop_token st) {
        try {
          while (!st.stop_requested() && started_.load() && !ioc->stopped()) {
            if (ioc->run_one_for(std::chrono::milliseconds(50)) == 0) {
              std::this_thread::yield();
            }
          }
        } catch (...) {
        }
      });
    }
    return f;
  }

  void stop() {
    bool should_join = false;
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load()) {
        fulfill_all_locked(false);
        return;
      }
      started_.store(false);
      bp_cv_.notify_all();
      alive_marker_.reset();
      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }
      if (channel_) {
        auto ch = channel_;
        lock.unlock();
        ch->stop();
        lock.lock();
        if (channel_ == ch) {
          channel_->on_bytes(nullptr);
          channel_->on_state(nullptr);
        }
      }
      if (use_external_context_.load() && manage_external_context_.load()) {
        if (work_guard_) work_guard_.reset();
        if (external_ioc_) external_ioc_->stop();
        should_join = true;
      }
      fulfill_all_locked(false);
    }
    if (should_join && external_thread_.joinable()) {
      try {
        if (std::this_thread::get_id() != external_thread_.get_id()) {
          external_thread_.request_stop();
          external_thread_.join();
        } else {
          external_thread_.detach();
        }
      } catch (...) {
      }
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    channel_.reset();
    if (framer_) framer_->reset();
  }

  bool try_send(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_ && channel_->is_connected()) {
      auto binary_view = base::safe_convert::string_to_bytes(data);
      return channel_->async_try_write_copy(memory::ConstByteSpan(binary_view.first, binary_view.second));
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
      return !started_.load() || !channel_ || !channel_->is_backpressure_active();
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
    if (!started_.load() || !channel_) return false;
    auto binary_view = base::safe_convert::string_to_bytes(data);
    return channel_->async_write_copy(memory::ConstByteSpan(binary_view.first, binary_view.second));
  }

  bool send_line_blocking(std::string_view line) { return send_blocking(std::string(line) + "\n"); }

  bool connected() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return channel_ && channel_->is_connected();
  }

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

    channel_->on_state([this, weak_alive](base::LinkState state) {
      auto alive = weak_alive.lock();
      if (!alive) return;
      ConnectionHandler connect_handler;
      ConnectionHandler disconnect_handler;
      ErrorHandler error_handler;
      std::shared_ptr<interface::Channel> channel_snapshot;

      if (state == base::LinkState::Connected) {
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(true);
          connect_handler = connect_handler_;
        }
        if (connect_handler) connect_handler(ConnectionContext(0));
      } else if (state == base::LinkState::Closed || state == base::LinkState::Error) {
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          if (state == base::LinkState::Closed) {
            disconnect_handler = disconnect_handler_;
          } else {
            error_handler = error_handler_;
            channel_snapshot = channel_;
          }
        }
        if (state == base::LinkState::Closed && disconnect_handler) {
          disconnect_handler(ConnectionContext(0));
        } else if (state == base::LinkState::Error && error_handler) {
          bool handled = false;
          if (auto transport = std::dynamic_pointer_cast<transport::TcpClient>(channel_snapshot)) {
            if (auto info = transport->last_error_info()) {
              error_handler(diagnostics::to_error_context(*info));
              handled = true;
            }
          }
          if (!handled) {
            error_handler(ErrorContext(ErrorCode::IoError, "Connection error"));
          }
        }
      }
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

TcpClient::TcpClient(const std::string& h, uint16_t p) : impl_(std::make_unique<Impl>(h, p)) {}
TcpClient::TcpClient(const std::string& h, uint16_t p, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_unique<Impl>(h, p, ioc)) {}
TcpClient::TcpClient(std::shared_ptr<interface::Channel> ch) : impl_(std::make_unique<Impl>(ch)) {}
TcpClient::~TcpClient() = default;

TcpClient::TcpClient(TcpClient&&) noexcept = default;
TcpClient& TcpClient::operator=(TcpClient&&) noexcept = default;

std::future<bool> TcpClient::start() { return impl_->start(); }
void TcpClient::stop() { impl_->stop(); }
bool TcpClient::send(std::string_view data) { return impl_->send(data); }
bool TcpClient::try_send(std::string_view data) { return impl_->try_send(data); }
bool TcpClient::send_line(std::string_view line) { return impl_->send_line(line); }
bool TcpClient::try_send_line(std::string_view line) { return impl_->try_send_line(line); }
bool TcpClient::send_move(std::vector<uint8_t>&& data) { return impl_->send_move(std::move(data)); }
bool TcpClient::try_send_move(std::vector<uint8_t>&& data) { return impl_->try_send_move(std::move(data)); }
bool TcpClient::send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->send_shared(std::move(data));
}
bool TcpClient::try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->try_send_shared(std::move(data));
}
bool TcpClient::send_blocking(std::string_view data) { return impl_->send_blocking(data); }
bool TcpClient::send_line_blocking(std::string_view line) { return impl_->send_line_blocking(line); }
bool TcpClient::connected() const { return get_impl()->connected(); }
RuntimeStats TcpClient::stats() const { return get_impl()->stats(); }
void TcpClient::reset_stats() { impl_->reset_stats(); }

TcpClient& TcpClient::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_handler_ = std::move(h);
  return *this;
}
TcpClient& TcpClient::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = std::move(h);
  return *this;
}
TcpClient& TcpClient::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connect_handler_ = std::move(h);
  return *this;
}
TcpClient& TcpClient::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->disconnect_handler_ = std::move(h);
  return *this;
}
TcpClient& TcpClient::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->error_handler_ = std::move(h);
  return *this;
}

TcpClient& TcpClient::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->bp_handler_ = std::move(h);
  return *this;
}

TcpClient& TcpClient::framer(std::unique_ptr<framer::IFramer> f) {
  impl_->set_framer(std::move(f));
  return *this;
}
TcpClient& TcpClient::on_message(MessageHandler h) {
  impl_->on_message(std::move(h));
  return *this;
}
TcpClient& TcpClient::on_message_batch(BatchMessageHandler h) {
  impl_->on_message_batch(std::move(h));
  return *this;
}

TcpClient& TcpClient::auto_start(bool m) {
  impl_->auto_start_.store(m);
  if (impl_->auto_start_.load() && !impl_->started_.load()) start();
  return *this;
}

TcpClient& TcpClient::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

TcpClient& TcpClient::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

TcpClient& TcpClient::retry_interval(std::chrono::milliseconds i) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->retry_interval_ = i;
  if (impl_->channel_) {
    auto transport_client = std::dynamic_pointer_cast<transport::TcpClient>(impl_->channel_);
    if (transport_client) transport_client->set_retry_interval(static_cast<unsigned int>(i.count()));
  }
  return *this;
}

TcpClient& TcpClient::max_retries(int m) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_retries_ = m;
  if (impl_->channel_) {
    auto transport = std::dynamic_pointer_cast<transport::TcpClient>(impl_->channel_);
    if (transport) transport->set_max_retries(m);
  }
  return *this;
}
TcpClient& TcpClient::connection_timeout(std::chrono::milliseconds t) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connection_timeout_ = t;
  if (impl_->channel_) {
    auto transport = std::dynamic_pointer_cast<transport::TcpClient>(impl_->channel_);
    if (transport) transport->set_connection_timeout(static_cast<unsigned>(t.count()));
  }
  return *this;
}

TcpClient& TcpClient::idle_timeout(std::chrono::milliseconds t) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->idle_timeout_ = t;
  if (impl_->channel_) {
    auto transport = std::dynamic_pointer_cast<transport::TcpClient>(impl_->channel_);
    if (transport) transport->set_idle_timeout(static_cast<unsigned>(t.count()));
  }
  return *this;
}

TcpClient& TcpClient::idle_timeout_action(IdleTimeoutAction action) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->idle_timeout_action_ = action;
  if (impl_->channel_) {
    auto transport = std::dynamic_pointer_cast<transport::TcpClient>(impl_->channel_);
    if (transport) transport->set_idle_timeout_action(action);
  }
  return *this;
}

TcpClient& TcpClient::manage_external_context(bool m) {
  impl_->manage_external_context_.store(m);
  return *this;
}
TcpClient& TcpClient::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_threshold_ = threshold;
  return *this;
}
TcpClient& TcpClient::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_strategy_ = strategy;
  if (impl_->channel_) {
    auto* tc = dynamic_cast<transport::TcpClient*>(impl_->channel_.get());
    if (tc) tc->set_backpressure_strategy(strategy);
  }
  return *this;
}

TcpClient& TcpClient::tcp_no_delay(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->tcp_no_delay_ = enable;
  return *this;
}

TcpClient& TcpClient::keep_alive(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->keep_alive_ = enable;
  return *this;
}

TcpClient& TcpClient::send_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->send_buffer_size_ = bytes;
  return *this;
}

TcpClient& TcpClient::receive_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->receive_buffer_size_ = bytes;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
