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

#include "unilink/wrapper/udp/udp_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/transport/udp/udp.hpp"

namespace unilink {
namespace wrapper {

namespace {
// std::hash<boost::asio::ip::udp::endpoint> is not available before Boost 1.74.
// Provide a portable hash by combining the raw address bytes and port.
struct UdpEndpointHash {
  std::size_t operator()(const boost::asio::ip::udp::endpoint& ep) const noexcept {
    std::size_t seed = 0;
    auto combine = [&](std::size_t v) { seed ^= v + 0x9e3779b9u + (seed << 6) + (seed >> 2); };
    if (ep.address().is_v4()) {
      for (auto byte : ep.address().to_v4().to_bytes()) {
        combine(std::hash<unsigned char>{}(byte));
      }
    } else {
      for (auto byte : ep.address().to_v6().to_bytes()) {
        combine(std::hash<unsigned char>{}(byte));
      }
    }
    combine(std::hash<unsigned short>{}(ep.port()));
    return seed;
  }
};
}  // namespace

struct UdpServer::Impl {
  config::UdpConfig cfg;
  std::shared_ptr<transport::UdpChannel> channel;
  std::shared_ptr<boost::asio::io_context> external_ioc;
  std::atomic<bool> use_external_context{false};
  std::atomic<bool> manage_external_context{false};
  std::jthread external_thread;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard;

  mutable std::shared_mutex mutex;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  std::vector<std::promise<bool>> pending_promises;
  std::atomic<bool> started{false};
  std::atomic<bool> is_listening{false};

  // Virtual Session Management
  struct SessionEntry {
    boost::asio::ip::udp::endpoint endpoint;
    std::shared_ptr<framer::IFramer> framer;
    std::chrono::steady_clock::time_point last_seen;
  };
  ClientId next_client_id{1};
  std::unordered_map<boost::asio::ip::udp::endpoint, ClientId, UdpEndpointHash> endpoint_to_id;
  std::unordered_map<ClientId, SessionEntry> sessions;
  std::chrono::milliseconds session_timeout{0};  // 0 = disabled
  std::unique_ptr<boost::asio::steady_timer> reaper_timer;
  std::atomic<bool> auto_start{false};
  std::atomic<bool> client_limit_enabled{false};
  std::atomic<size_t> max_clients_limit{0};

  ConnectionHandler on_connect{nullptr};
  ConnectionHandler on_disconnect{nullptr};
  MessageHandler on_data{nullptr};
  BatchMessageHandler on_data_batch_{nullptr};
  ErrorHandler on_error{nullptr};
  std::function<void(size_t)> bp_handler{nullptr};
  FramerFactory framer_factory{nullptr};
  MessageHandler on_message{nullptr};
  BatchMessageHandler on_message_batch_{nullptr};

  std::shared_ptr<bool> is_alive{std::make_shared<bool>(true)};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  explicit Impl(const config::UdpConfig& config) : cfg(config) {}
  Impl(const config::UdpConfig& config, std::shared_ptr<boost::asio::io_context> ioc)
      : cfg(config), external_ioc(std::move(ioc)), use_external_context(external_ioc != nullptr) {}
  explicit Impl(std::shared_ptr<interface::Channel> ch)
      : channel(std::dynamic_pointer_cast<transport::UdpChannel>(ch)) {
    setup_internal_handlers();
  }

  ~Impl() {
    *is_alive = false;
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
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (!data_batch_queue_.empty()) {
      auto handler = on_data_batch_;
      auto batch = std::move(data_batch_queue_);
      data_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        handler(batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = on_message_batch_;
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
    batch_timer_->async_wait([this, alive = std::weak_ptr<bool>(is_alive)](const boost::system::error_code& ec) {
      auto lock = alive.lock();
      if (!lock || !(*lock)) return;

      if (!ec) {
        flush_batches();
      }
    });
  }

  void schedule_reaper() {
    if (!started.load() || !reaper_timer || session_timeout.count() <= 0) return;

    // Run reaper at interval proportional to timeout (min 100ms, max 5s)
    auto interval =
        std::max(std::chrono::milliseconds(100), std::min(std::chrono::milliseconds(5000), session_timeout / 2));

    reaper_timer->expires_after(interval);
    reaper_timer->async_wait([this, alive = std::weak_ptr<bool>(is_alive)](const boost::system::error_code& ec) {
      auto lock = alive.lock();
      if (!lock || !(*lock)) return;

      if (!ec) {
        run_reaper();
        schedule_reaper();
      }
    });
  }

  void run_reaper() {
    if (session_timeout.count() <= 0) return;

    std::vector<std::pair<ClientId, std::string>> to_remove_with_info;
    auto now = std::chrono::steady_clock::now();

    ConnectionHandler disconnect_handler;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      for (auto it = sessions.begin(); it != sessions.end();) {
        if (now - it->second.last_seen > session_timeout) {
          std::string info =
              fmt::format("{}:{}", it->second.endpoint.address().to_string(), it->second.endpoint.port());
          endpoint_to_id.erase(it->second.endpoint);
          to_remove_with_info.push_back({it->first, info});
          it = sessions.erase(it);
        } else {
          ++it;
        }
      }
      disconnect_handler = on_disconnect;
    }

    // Call disconnect handlers outside the lock
    if (disconnect_handler) {
      for (auto const& [id, info] : to_remove_with_info) {
        disconnect_handler(ConnectionContext(id, info));
      }
    }
  }

  void setup_internal_handlers() {
    if (!channel) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(channel->get_executor());

    channel->on_bytes_from([this](memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& ep) {
      ClientId client_id = 0;
      bool is_new = false;
      ConnectionHandler connect_handler_copy{nullptr};

      {
        std::unique_lock<std::shared_mutex> lock(mutex);
        auto it = endpoint_to_id.find(ep);
        if (it == endpoint_to_id.end()) {
          if (client_limit_enabled.load() && sessions.size() >= max_clients_limit.load()) {
            return;
          }
          client_id = next_client_id++;
          endpoint_to_id[ep] = client_id;
          SessionEntry entry;
          entry.endpoint = ep;
          entry.last_seen = std::chrono::steady_clock::now();
          is_new = true;

          // Create framer for new session
          if (framer_factory) {
            auto framer = framer_factory();
            if (framer) {
              framer->on_message([this, client_id](memory::ConstByteSpan msg) {
                std::unique_lock<std::shared_mutex> lock(mutex);
                if (on_message_batch_) {
                  message_batch_queue_.emplace_back(client_id, memory::SafeDataBuffer(msg));
                  if (message_batch_queue_.size() >= max_batch_size_) {
                    auto handler = on_message_batch_;
                    auto batch = std::move(message_batch_queue_);
                    message_batch_queue_.clear();
                    lock.unlock();
                    handler(batch);
                  } else if (message_batch_queue_.size() == 1) {
                    schedule_batch_timer();
                  }
                  return;
                }

                MessageHandler on_message_handler = on_message;
                if (on_message_handler) {
                  lock.unlock();
                  on_message_handler(MessageContext(client_id, memory::SafeDataBuffer(msg)));
                }
              });
              entry.framer = std::move(framer);
            }
          }
          sessions[client_id] = std::move(entry);
        } else {
          client_id = it->second;
          sessions[client_id].last_seen = std::chrono::steady_clock::now();
        }
        connect_handler_copy = on_connect;
      }

      if (is_new && connect_handler_copy) {
        connect_handler_copy(ConnectionContext(client_id, fmt::format("{}:{}", ep.address().to_string(), ep.port())));
      }

      {
        std::unique_lock<std::shared_mutex> lock(mutex);
        if (on_data_batch_) {
          data_batch_queue_.emplace_back(client_id, memory::SafeDataBuffer(data));
          if (data_batch_queue_.size() >= max_batch_size_) {
            auto handler = on_data_batch_;
            auto batch = std::move(data_batch_queue_);
            data_batch_queue_.clear();
            lock.unlock();
            handler(batch);
          } else if (data_batch_queue_.size() == 1) {
            schedule_batch_timer();
          }
        } else {
          MessageHandler data_handler_copy = on_data;
          if (data_handler_copy) {
            lock.unlock();
            data_handler_copy(MessageContext(client_id, memory::SafeDataBuffer(data)));
            lock.lock();
          }
        }
      }

      // Push to framer
      std::shared_ptr<framer::IFramer> target_framer;
      {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = sessions.find(client_id);
        if (it != sessions.end()) {
          target_framer = it->second.framer;
        }
      }
      if (target_framer) {
        target_framer->push_bytes(data);
      }
    });

    channel->on_backpressure([this](size_t queued) {
      bp_cv_.notify_all();
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex);
        handler = bp_handler;
      }
      if (handler) handler(queued);
    });

    channel->on_state([this](base::LinkState state) {
      ErrorHandler error_handler_copy{nullptr};
      if (state == base::LinkState::Listening || state == base::LinkState::Connected) {
        is_listening.store(true);
        std::unique_lock<std::shared_mutex> lock(mutex);
        fulfill_all_locked(true);
      } else if (state == base::LinkState::Error || state == base::LinkState::Closed ||
                 state == base::LinkState::Idle) {
        is_listening.store(false);
        std::unique_lock<std::shared_mutex> lock(mutex);
        fulfill_all_locked(false);
        if (state == base::LinkState::Error) {
          error_handler_copy = on_error;
        }
      }

      if (error_handler_copy) {
        error_handler_copy(ErrorContext(ErrorCode::IoError, "Server error"));
      }
    });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (is_listening.load()) {
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }

    std::promise<bool> p;
    auto fut = p.get_future();
    pending_promises.emplace_back(std::move(p));

    if (started.exchange(true)) {
      return fut;
    }

    if (!channel) {
      channel = std::dynamic_pointer_cast<transport::UdpChannel>(factory::ChannelFactory::create(cfg, external_ioc));
      setup_internal_handlers();
    }

    lock.unlock();
    channel->start();

    lock.lock();
    if (use_external_context.load() && manage_external_context.load() && !external_thread.joinable()) {
      if (external_ioc->stopped()) external_ioc->restart();
      work_guard = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          external_ioc->get_executor());
      external_thread = std::jthread([ioc = external_ioc](std::stop_token st) {
        try {
          std::stop_callback cb(st, [ioc] { ioc->stop(); });
          ioc->run();
        } catch (...) {
        }
      });
    }

    if (channel && session_timeout.count() > 0) {
      reaper_timer = std::make_unique<boost::asio::steady_timer>(channel->get_executor());
      schedule_reaper();
    }

    return fut;
  }

  void stop() {
    bool should_join = false;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      if (!started.exchange(false)) {
        is_listening.store(false);
        fulfill_all_locked(false);
        return;
      }
      bp_cv_.notify_all();

      if (reaper_timer) {
        reaper_timer->cancel();
        reaper_timer.reset();
      }

      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }

      if (channel) {
        lock.unlock();
        channel->stop();
        // Clear callbacks after stop() rather than before: the
        // transport-level fix (#436) already synchronizes callback reads
        // against these setters, but clearing after stop() means no
        // in-flight handler can observe a null callback mid-shutdown in the
        // first place - belt and braces once the underlying race is fixed
        // at the source. Also now clears on_backpressure, which this path
        // previously never did at all.
        channel->on_bytes_from(nullptr);
        channel->on_state(nullptr);
        channel->on_backpressure(nullptr);
        lock.lock();
      }

      if (use_external_context.load() && manage_external_context.load()) {
        if (external_ioc) external_ioc->stop();
        should_join = true;
      }

      is_listening.store(false);
      endpoint_to_id.clear();
      sessions.clear();
      next_client_id = 1;
      fulfill_all_locked(false);
    }

    if (should_join && external_thread.joinable()) {
      try {
        if (std::this_thread::get_id() != external_thread.get_id()) {
          external_thread.request_stop();
          external_thread.join();
        } else {
          external_thread.detach();
        }
      } catch (...) {
      }
    }
    std::unique_lock<std::shared_mutex> lock(mutex);
    channel.reset();
  }

  bool send_to(ClientId client_id, std::string_view data) {
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable)
      return send_to_blocking(client_id, data);
    return try_send_to(client_id, data);
  }

  bool try_broadcast(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (!channel) return false;
    bool sent = false;
    auto bytes = base::safe_convert::string_to_bytes(data);
    for (const auto& [id, entry] : sessions) {
      sent |= channel->async_try_write_to(memory::ConstByteSpan(bytes.first, bytes.second), entry.endpoint);
    }
    return sent;
  }

  bool broadcast(std::string_view data) { return try_broadcast(data); }

  // channel->on_backpressure() calls bp_cv_.notify_all() from the transport's io_context
  // thread without holding bp_mutex_ (backpressure_active_ is a plain atomic on the transport
  // side, not guarded by bp_mutex_ at all). That makes a classic lost-wakeup race possible: a
  // waiter can check the predicate, find it still blocking, and be in the process of
  // registering to wait when the notify fires - in the rare case that race is lost, an
  // unbounded wait() would block forever. Poll with a bounded timeout instead so a missed
  // notify only costs a short delay rather than a permanent hang (see #427, #431).
  bool send_to_blocking(ClientId client_id, std::string_view data) {
    std::unique_lock<std::mutex> bp_lock(bp_mutex_);
    auto predicate = [this] {
      std::shared_lock<std::shared_mutex> lock(mutex);
      return !started.load() || !channel || !channel->is_backpressure_active();
    };
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), predicate)) {
    }
    return try_send_to(client_id, data);
  }

  bool try_send_to(ClientId client_id, std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto it = sessions.find(client_id);
    if (it == sessions.end() || !channel) return false;

    auto bytes = base::safe_convert::string_to_bytes(data);
    return channel->async_try_write_to(memory::ConstByteSpan(bytes.first, bytes.second), it->second.endpoint);
  }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return channel ? channel->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (channel) channel->reset_stats();
  }
};

UdpServer::UdpServer(uint16_t port) {
  config::UdpConfig cfg;
  cfg.local_port = port;
  impl_ = std::make_unique<Impl>(cfg);
}

UdpServer::UdpServer(const config::UdpConfig& cfg) : impl_(std::make_unique<Impl>(cfg)) {}

UdpServer::UdpServer(const config::UdpConfig& cfg, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_unique<Impl>(cfg, ioc)) {}

UdpServer::UdpServer(std::shared_ptr<interface::Channel> ch) : impl_(std::make_unique<Impl>(std::move(ch))) {}

UdpServer::~UdpServer() = default;

UdpServer::UdpServer(UdpServer&&) noexcept = default;
UdpServer& UdpServer::operator=(UdpServer&&) noexcept = default;

std::future<bool> UdpServer::start() { return impl_->start(); }
void UdpServer::stop() { impl_->stop(); }
bool UdpServer::listening() const { return impl_->is_listening.load(); }
RuntimeStats UdpServer::stats() const { return impl_->stats(); }
void UdpServer::reset_stats() { impl_->reset_stats(); }

bool UdpServer::broadcast(std::string_view data) { return impl_->broadcast(data); }
bool UdpServer::try_broadcast(std::string_view data) { return impl_->try_broadcast(data); }
bool UdpServer::send_to(ClientId client_id, std::string_view data) { return impl_->send_to(client_id, data); }
bool UdpServer::try_send_to(ClientId client_id, std::string_view data) { return impl_->try_send_to(client_id, data); }

bool UdpServer::send_to_blocking(ClientId client_id, std::string_view data) {
  return impl_->send_to_blocking(client_id, data);
}

bool UdpServer::broadcast_line(std::string_view line) { return broadcast(std::string(line) + "\n"); }
bool UdpServer::send_to_line(ClientId client_id, std::string_view line) {
  return send_to(client_id, std::string(line) + "\n");
}
bool UdpServer::try_broadcast_line(std::string_view line) { return try_broadcast(std::string(line) + "\n"); }
bool UdpServer::try_send_to_line(ClientId client_id, std::string_view line) {
  return try_send_to(client_id, std::string(line) + "\n");
}

UdpServer& UdpServer::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_connect = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_disconnect = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_data = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_data_batch_ = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_error = std::move(h);
  return *this;
}

UdpServer& UdpServer::framer(FramerFactory factory) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->framer_factory = std::move(factory);
  return *this;
}

UdpServer& UdpServer::on_message(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_message = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_message_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_message_batch_ = std::move(h);
  return *this;
}

size_t UdpServer::client_count() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  return impl_->endpoint_to_id.size();
}

std::vector<ClientId> UdpServer::connected_clients() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  std::vector<ClientId> ids;
  ids.reserve(impl_->sessions.size());
  for (const auto& [id, entry] : impl_->sessions) {
    ids.push_back(id);
  }
  return ids;
}

UdpServer& UdpServer::auto_start(bool m) {
  impl_->auto_start.store(m);
  if (impl_->auto_start.load() && !impl_->started.load()) {
    start();
  }
  return *this;
}

UdpServer& UdpServer::bind_address(const std::string& address) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.bind_address = address;
  return *this;
}

UdpServer& UdpServer::idle_timeout(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->session_timeout = timeout;
  if (impl_->session_timeout.count() <= 0) {
    if (impl_->reaper_timer) {
      impl_->reaper_timer->cancel();
      impl_->reaper_timer.reset();
    }
    return *this;
  }
  if (impl_->started.load() && impl_->channel && !impl_->reaper_timer) {
    impl_->reaper_timer = std::make_unique<boost::asio::steady_timer>(impl_->channel->get_executor());
    impl_->schedule_reaper();
  }
  return *this;
}

UdpServer& UdpServer::on_backpressure(std::function<void(size_t)> handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->bp_handler = std::move(handler);
  return *this;
}

UdpServer& UdpServer::max_clients(size_t max) {
  if (max == 0) {
    impl_->client_limit_enabled.store(false);
    impl_->max_clients_limit.store(0);
  } else {
    impl_->client_limit_enabled.store(true);
    impl_->max_clients_limit.store(max);
  }
  return *this;
}

UdpServer& UdpServer::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.backpressure_threshold = threshold;
  return *this;
}

UdpServer& UdpServer::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.backpressure_strategy = strategy;
  if (impl_->channel) {
    impl_->channel->set_backpressure_strategy(strategy);
  }
  return *this;
}

UdpServer& UdpServer::send_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.send_buffer_size = bytes;
  return *this;
}

UdpServer& UdpServer::receive_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.receive_buffer_size = bytes;
  return *this;
}

UdpServer& UdpServer::manage_external_context(bool m) {
  impl_->manage_external_context.store(m);
  return *this;
}

UdpServer& UdpServer::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->max_batch_size_ = size;
  return *this;
}

UdpServer& UdpServer::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
