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

#include "unilink/wrapper/udp/udp.hpp"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wconversion"
#endif

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/transport/udp/udp.hpp"

namespace unilink {
namespace wrapper {

struct UdpClient::Impl {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  config::UdpConfig cfg;
  std::shared_ptr<interface::Channel> channel;
  std::shared_ptr<boost::asio::io_context> external_ioc;
  std::atomic<bool> use_external_context{false};
  std::atomic<bool> manage_external_context{false};
  std::thread external_thread;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard;

  // Event handlers (Context based)
  MessageHandler data_handler{nullptr};
  BatchMessageHandler data_batch_handler_{nullptr};
  ConnectionHandler connect_handler{nullptr};
  ConnectionHandler disconnect_handler{nullptr};
  ErrorHandler error_handler{nullptr};
  std::function<void(size_t)> bp_handler{nullptr};
  MessageHandler message_handler{nullptr};
  BatchMessageHandler message_batch_handler_{nullptr};

  std::shared_ptr<framer::IFramer> framer{nullptr};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  std::atomic<bool> auto_start_{false};
  std::vector<std::promise<bool>> pending_promises;
  std::atomic<bool> started_{false};
  std::shared_ptr<bool> alive_marker{std::make_shared<bool>(true)};

  // False only for the dependency-injected-channel constructor below, where
  // the caller owns the channel's identity and lifecycle (e.g. tests
  // injecting a fake channel) - stop() must not discard and factory-rebuild
  // a channel it didn't create itself.
  bool factory_managed_channel_ = true;

  explicit Impl(const config::UdpConfig& config) : cfg(config) {}
  Impl(const config::UdpConfig& config, std::shared_ptr<boost::asio::io_context> ioc)
      : cfg(config), external_ioc(std::move(ioc)), use_external_context(external_ioc != nullptr) {}
  explicit Impl(std::shared_ptr<interface::Channel> ch) : channel(std::move(ch)), factory_managed_channel_(false) {
    setup_internal_handlers();
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void fulfill_all_locked(bool value) {
    for (auto& promise : pending_promises) {
      try {
        promise.set_value(value);
      } catch (...) {
      }
    }
    pending_promises.clear();
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
        [this, weak_alive = std::weak_ptr<bool>(alive_marker)](const boost::system::error_code& ec) {
          if (ec) return;
          auto alive = weak_alive.lock();
          if (!alive) return;
          flush_batches();
        });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (channel && channel->is_connected()) {
      started_.store(true);
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }

    std::promise<bool> p;
    auto future = p.get_future();
    pending_promises.emplace_back(std::move(p));

    if (started_.load()) {
      return future;
    }

    if (!channel) {
      channel = factory::ChannelFactory::create(cfg, external_ioc);
      setup_internal_handlers();
    }
    started_.store(true);

    lock.unlock();
    channel->start();
    if (use_external_context && manage_external_context && !external_thread.joinable()) {
      if (external_ioc->stopped()) external_ioc->restart();
      work_guard = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          boost::asio::make_work_guard(*external_ioc));
      external_thread = std::thread([this, ioc = external_ioc]() {
        try {
          ioc->run();
        } catch (...) {
        }
      });
    }
    return future;
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

    if (channel) {
      lock.unlock();
      channel->stop();
      // Clear callbacks after stop() rather than before: the transport-level
      // fix (#436) already synchronizes callback reads against these
      // setters, but clearing after stop() means no in-flight handler can
      // observe a null callback mid-shutdown in the first place - belt and
      // braces once the underlying race is fixed at the source.
      channel->on_bytes(nullptr);
      channel->on_state(nullptr);
      channel->on_backpressure(nullptr);
      lock.lock();
      if (factory_managed_channel_) {
        // Fully release the channel rather than reusing it: start()'s
        // `if (!channel)` guard is what re-runs setup_internal_handlers() and
        // rebuilds config from the (possibly changed) staged fields. Reusing
        // a stopped channel left every handler nulled forever - including
        // the one that fulfills the start() future - so a restart would
        // hang (jwsung91/unilink#444). Injected channels (factory_managed_
        // channel_ == false) are exempt: the caller owns that channel's
        // identity, so we must not discard and factory-rebuild it.
        channel.reset();
      }
    }

    if (work_guard) {
      work_guard.reset();
    }

    if (use_external_context && manage_external_context && external_thread.joinable()) {
      if (external_ioc) {
        external_ioc->stop();
      }
      if (std::this_thread::get_id() != external_thread.get_id()) {
        lock.unlock();
        external_thread.join();
        lock.lock();
      } else {
        external_thread.detach();
      }
    }

    fulfill_all_locked(false);

    if (framer) {
      framer->reset();
    }
  }

  bool send(std::string_view data) {
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable) return send_blocking(data);
    return try_send(data);
  }

  bool send_move(std::vector<uint8_t>&& data) {
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable) {
      std::unique_lock<std::mutex> bp_lock(bp_mutex_);
      wait_for_backpressure_clear(bp_lock);
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load() || !channel || !channel->is_connected()) return false;
      return channel->async_write_move(std::move(data));
    }
    return try_send_move(std::move(data));
  }

  bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (!data || data->empty()) return false;
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable) {
      std::unique_lock<std::mutex> bp_lock(bp_mutex_);
      wait_for_backpressure_clear(bp_lock);
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load() || !channel || !channel->is_connected()) return false;
      return channel->async_write_shared(std::move(data));
    }
    return try_send_shared(std::move(data));
  }

  bool send_line(std::string_view line) {
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable) return send_line_blocking(line);
    return try_send_line(line);
  }

  bool try_send_line(std::string_view line) { return try_send(std::string(line) + "\n"); }

  bool send_blocking(std::string_view data) {
    std::unique_lock<std::mutex> bp_lock(bp_mutex_);
    wait_for_backpressure_clear(bp_lock);
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!started_.load() || !channel || !channel->is_connected()) return false;
    auto binary_view = base::safe_convert::string_to_bytes(data);
    return channel->async_write_copy(memory::ConstByteSpan(binary_view.first, binary_view.second));
  }

  // channel->on_backpressure() calls bp_cv_.notify_all() from the transport's io_context
  // thread without holding bp_mutex_ (backpressure_active_ is a plain atomic on the transport
  // side, not guarded by bp_mutex_ at all). That makes a classic lost-wakeup race possible: a
  // waiter can check the predicate, find it still blocking, and be in the process of
  // registering to wait when the notify fires - in the rare case that race is lost, an
  // unbounded wait() would block forever. Poll with a bounded timeout instead so a missed
  // notify only costs a short delay rather than a permanent hang (see #427).
  void wait_for_backpressure_clear(std::unique_lock<std::mutex>& bp_lock) {
    auto predicate = [this] {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      return !started_.load() || !channel || !channel->is_connected() || !channel->is_backpressure_active();
    };
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), predicate)) {
    }
  }

  bool try_send(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel && channel->is_connected()) {
      auto binary_view = base::safe_convert::string_to_bytes(data);
      return channel->async_try_write_copy(memory::ConstByteSpan(binary_view.first, binary_view.second));
    }
    return false;
  }

  bool try_send_move(std::vector<uint8_t>&& data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel && channel->is_connected()) {
      return channel->async_try_write_move(std::move(data));
    }
    return false;
  }

  bool try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (!data || data->empty()) return false;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel && channel->is_connected()) {
      return channel->async_try_write_shared(std::move(data));
    }
    return false;
  }

  bool send_line_blocking(std::string_view line) { return send_blocking(std::string(line) + "\n"); }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return channel ? channel->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel) channel->reset_stats();
  }

  void setup_internal_handlers() {
    if (!channel) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(channel->get_executor());

    std::weak_ptr<bool> weak_alive = alive_marker;

    channel->on_bytes([this, weak_alive](memory::ConstByteSpan data) {
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
        MessageHandler handler = data_handler;
        if (handler) {
          lock.unlock();
          handler(MessageContext(0, memory::SafeDataBuffer(data)));
          lock.lock();
        }
      }

      if (framer) {
        framer_to_push = framer;
      }
      lock.unlock();
      if (framer_to_push) framer_to_push->push_bytes(data);
    });

    channel->on_backpressure([this, weak_alive](size_t queued) {
      bp_cv_.notify_all();
      auto alive = weak_alive.lock();
      if (!alive) return;
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        handler = bp_handler;
      }
      if (handler) handler(queued);
    });

    channel->on_state([this, weak_alive](base::LinkState state) {
      auto alive = weak_alive.lock();
      if (!alive) return;

      switch (state) {
        case base::LinkState::Connected:
        case base::LinkState::Listening: {
          ConnectionHandler handler;
          {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            fulfill_all_locked(true);
            handler = connect_handler;
          }
          if (handler) handler(ConnectionContext(0));
          break;
        }
        case base::LinkState::Closed:
        case base::LinkState::Error:
        case base::LinkState::Idle: {
          ConnectionHandler disconnect_handler_snapshot;
          ErrorHandler error_handler_snapshot;
          {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            fulfill_all_locked(false);
            if (state == base::LinkState::Error) {
              error_handler_snapshot = error_handler;
            } else {
              disconnect_handler_snapshot = disconnect_handler;
            }
          }
          if (disconnect_handler_snapshot) {
            disconnect_handler_snapshot(ConnectionContext(0));
          }
          if (error_handler_snapshot) {
            error_handler_snapshot(ErrorContext(ErrorCode::IoError, "Connection error"));
          }
          break;
        }
        default:
          break;
      }
    });
  }

  void attach_framer_callback() {
    if (!framer) return;
    framer->on_message([this](memory::ConstByteSpan msg) {
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

      MessageHandler handler = message_handler;
      if (handler) {
        lock.unlock();
        handler(MessageContext(0, memory::SafeDataBuffer(msg)));
      }
    });
  }

  void set_framer(std::unique_ptr<framer::IFramer> f) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    framer = std::shared_ptr<framer::IFramer>(std::move(f));
    if (framer && (message_handler || message_batch_handler_)) attach_framer_callback();
  }

  void on_message(MessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_handler = std::move(handler);
    if (framer) attach_framer_callback();
  }

  void on_message_batch(BatchMessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_batch_handler_ = std::move(handler);
    if (framer) attach_framer_callback();
  }
};

UdpClient::UdpClient(const config::UdpConfig& cfg) : impl_(std::make_unique<Impl>(cfg)) {}
UdpClient::UdpClient(const config::UdpConfig& cfg, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_unique<Impl>(cfg, ioc)) {}
UdpClient::UdpClient(std::shared_ptr<interface::Channel> ch) : impl_(std::make_unique<Impl>(ch)) {}
UdpClient::~UdpClient() = default;

UdpClient::UdpClient(UdpClient&&) noexcept = default;
UdpClient& UdpClient::operator=(UdpClient&&) noexcept = default;

std::future<bool> UdpClient::start() { return impl_->start(); }
void UdpClient::stop() { impl_->stop(); }
bool UdpClient::send(std::string_view data) { return impl_->send(data); }
bool UdpClient::try_send(std::string_view data) { return impl_->try_send(data); }
bool UdpClient::send_line(std::string_view line) { return impl_->send_line(line); }
bool UdpClient::try_send_line(std::string_view line) { return impl_->try_send_line(line); }
bool UdpClient::send_move(std::vector<uint8_t>&& data) { return impl_->send_move(std::move(data)); }
bool UdpClient::try_send_move(std::vector<uint8_t>&& data) { return impl_->try_send_move(std::move(data)); }
bool UdpClient::send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->send_shared(std::move(data));
}
bool UdpClient::try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->try_send_shared(std::move(data));
}
bool UdpClient::send_blocking(std::string_view data) { return impl_->send_blocking(data); }
bool UdpClient::send_line_blocking(std::string_view line) { return impl_->send_line_blocking(line); }
bool UdpClient::connected() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->channel && impl_->channel->is_connected();
}
RuntimeStats UdpClient::stats() const { return impl_->stats(); }
void UdpClient::reset_stats() { impl_->reset_stats(); }

UdpClient& UdpClient::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_handler = std::move(h);
  return *this;
}
UdpClient& UdpClient::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = std::move(h);
  return *this;
}
UdpClient& UdpClient::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connect_handler = std::move(h);
  return *this;
}
UdpClient& UdpClient::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->disconnect_handler = std::move(h);
  return *this;
}
UdpClient& UdpClient::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->error_handler = std::move(h);
  return *this;
}

UdpClient& UdpClient::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->bp_handler = std::move(h);
  return *this;
}

UdpClient& UdpClient::framer(std::unique_ptr<framer::IFramer> f) {
  impl_->set_framer(std::move(f));
  return *this;
}
UdpClient& UdpClient::on_message(MessageHandler h) {
  impl_->on_message(std::move(h));
  return *this;
}
UdpClient& UdpClient::on_message_batch(BatchMessageHandler h) {
  impl_->on_message_batch(std::move(h));
  return *this;
}

UdpClient& UdpClient::auto_start(bool m) {
  impl_->auto_start_.store(m);
  if (impl_->auto_start_.load() && !impl_->started_.load()) start();
  return *this;
}

UdpClient& UdpClient::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->cfg.backpressure_threshold = threshold;
  return *this;
}

UdpClient& UdpClient::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->cfg.backpressure_strategy = strategy;
  if (impl_->channel) {
    if (auto* uc = dynamic_cast<transport::UdpChannel*>(impl_->channel.get())) {
      uc->set_backpressure_strategy(strategy);
    }
  }
  return *this;
}

UdpClient& UdpClient::send_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->cfg.send_buffer_size = bytes;
  return *this;
}

UdpClient& UdpClient::receive_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->cfg.receive_buffer_size = bytes;
  return *this;
}

UdpClient& UdpClient::manage_external_context(bool m) {
  impl_->manage_external_context.store(m);
  return *this;
}

UdpClient& UdpClient::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

UdpClient& UdpClient::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
