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

#include "unilink/wrapper/tcp_server/tcp_server.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/config/tcp_server_config.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/transport/tcp_server/tcp_server.hpp"
#include "unilink/wrapper/error_context_builder.hpp"

namespace unilink {
namespace wrapper {

struct TcpServer::Impl {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  uint16_t port_;
  std::string bind_address_{"0.0.0.0"};
  std::shared_ptr<interface::Channel> channel_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};
  std::jthread external_thread_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::vector<std::promise<bool>> pending_promises_;
  std::atomic<bool> started_{false};
  std::atomic<bool> is_listening_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  // Configuration
  std::atomic<bool> auto_start_{false};
  std::atomic<bool> shared_context_{false};
  std::atomic<bool> port_retry_enabled_{false};
  std::atomic<int> max_port_retries_{3};
  std::atomic<int> port_retry_interval_ms_{1000};
  std::atomic<int> idle_timeout_ms_{0};
  std::atomic<bool> client_limit_enabled_{false};
  std::atomic<size_t> max_clients_{0};
  std::atomic<size_t> backpressure_threshold_{base::constants::DEFAULT_BACKPRESSURE_THRESHOLD};
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};
  std::atomic<bool> tcp_no_delay_{true};
  std::atomic<bool> keep_alive_{false};
  std::atomic<size_t> send_buffer_size_{0};
  std::atomic<size_t> receive_buffer_size_{0};

  ConnectionHandler on_client_connect_{nullptr};
  ConnectionHandler on_disconnect_{nullptr};
  MessageHandler on_data_{nullptr};
  BatchMessageHandler data_batch_handler_{nullptr};
  ErrorHandler on_error_{nullptr};
  std::function<void(size_t)> on_backpressure_{nullptr};
  FramerFactory framer_factory_{nullptr};
  MessageHandler on_message_{nullptr};
  BatchMessageHandler message_batch_handler_{nullptr};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  std::unordered_map<ClientId, std::shared_ptr<framer::IFramer>> framers_;
  // Cached transport pointer — set once in start(), avoids repeated dynamic_cast.
  std::shared_ptr<transport::TcpServer> transport_cache_;

  explicit Impl(uint16_t port)
      : port_(port),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(0),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {}

  Impl(uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc)
      : port_(port),
        external_ioc_(std::move(external_ioc)),
        use_external_context_(external_ioc_ != nullptr),
        manage_external_context_(false),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(0),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {}

  explicit Impl(std::shared_ptr<interface::Channel> channel)
      : port_(0),
        channel_(std::move(channel)),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(0),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {
    transport_cache_ = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
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
    if (is_listening_.load()) {
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }
    std::promise<bool> p;
    auto f = p.get_future();
    pending_promises_.push_back(std::move(p));
    if (started_.exchange(true)) return f;

    if (!channel_) {
      config::TcpServerConfig config;
      config.bind_address = bind_address_;
      config.port = port_;
      config.enable_port_retry = port_retry_enabled_.load();
      config.max_port_retries = max_port_retries_.load();
      config.port_retry_interval_ms = port_retry_interval_ms_.load();
      config.idle_timeout_ms = idle_timeout_ms_.load();
      config.backpressure_threshold = backpressure_threshold_.load();
      config.backpressure_strategy = backpressure_strategy_.load();
      config.tcp_no_delay = tcp_no_delay_.load();
      config.keep_alive = keep_alive_.load();
      config.send_buffer_size = send_buffer_size_.load();
      config.receive_buffer_size = receive_buffer_size_.load();
      config.use_shared_context = shared_context_.load();

      channel_ = factory::ChannelFactory::create(config, external_ioc_);
      transport_cache_ = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
      setup_internal_handlers();

      if (client_limit_enabled_.load()) {
        auto transport_server = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
        if (transport_server) {
          transport_server->set_client_limit(max_clients_.load());
        }
      }
    }
    lock.unlock();
    channel_->start();
    if (use_external_context_.load() && manage_external_context_.load() && !external_thread_.joinable()) {
      if (external_ioc_ && external_ioc_->stopped()) {
        external_ioc_->restart();
      }
      work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          boost::asio::make_work_guard(*external_ioc_));
      external_thread_ = std::jthread([ioc = external_ioc_](std::stop_token st) {
        try {
          std::stop_callback cb(st, [ioc] { ioc->stop(); });
          ioc->run();
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
      if (!started_.exchange(false)) {
        is_listening_.store(false);
        fulfill_all_locked(false);
        return;
      }
      bp_cv_.notify_all();
      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }
      if (channel_) {
        channel_->on_bytes(nullptr);
        channel_->on_state(nullptr);
        channel_->on_backpressure(nullptr);
        auto transport_server = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
        if (transport_server) transport_server->request_stop();
        lock.unlock();
        channel_->stop();
        lock.lock();
      }
      if (use_external_context_.load() && manage_external_context_.load()) {
        if (work_guard_) work_guard_.reset();
        if (external_ioc_) external_ioc_->stop();
        should_join = true;
      }
      // #444: transport_cache_ is a separate cached shared_ptr to the same
      // transport object channel_ points at - without this, send_to()/
      // broadcast()/max_clients() could still reach the stopped transport
      // via transport_cache_ even after channel_.reset() below. framers_
      // must also be cleared so a restart doesn't resume per-client framing
      // state from stale ClientIds (mirrors UdsServer's existing behavior).
      transport_cache_.reset();
      framers_.clear();
      fulfill_all_locked(false);
      is_listening_.store(false);
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
  }

  bool try_send_to(ClientId client_id, std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto& ts = transport_cache_;
    return ts ? ts->try_send_to_client(client_id, data) : false;
  }

  bool try_broadcast(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto& ts = transport_cache_;
    return ts ? ts->broadcast(data) : false;
  }

  bool send_to(ClientId client_id, std::string_view data) {
    if (backpressure_strategy_.load() == base::constants::BackpressureStrategy::Reliable)
      return send_to_blocking(client_id, data);
    return try_send_to(client_id, data);
  }

  bool broadcast(std::string_view data) {
    // In order to avoid Head-Of-Line blocking where a single slow client
    // blocks the entire broadcast loop, we delegate to try_broadcast (async fan-out)
    // even in Reliable mode. Transport-level backpressure will still protect the queues.
    return try_broadcast(data);
  }

  // channel_->on_backpressure()/session-level backpressure callbacks call bp_cv_.notify_all()
  // from the transport's io_context thread without holding bp_mutex_ - a classic lost-wakeup
  // race is possible: a waiter can check the predicate, find it still blocking, and be in the
  // process of registering to wait when the notify fires. Poll with a bounded timeout instead
  // of an unbounded wait() so a missed notify only costs a short delay rather than a
  // permanent hang (see #427, #431).
  bool send_to_blocking(ClientId client_id, std::string_view data) {
    std::unique_lock<std::mutex> lock(bp_mutex_);
    auto predicate = [this, client_id]() {
      std::shared_lock<std::shared_mutex> rlock(mutex_);
      const auto& ts = transport_cache_;
      return !started_.load() || !ts || !ts->is_backpressure_active(client_id);
    };
    while (!bp_cv_.wait_for(lock, std::chrono::milliseconds(50), predicate)) {
    }
    std::shared_lock<std::shared_mutex> rlock(mutex_);
    const auto& ts = transport_cache_;
    return ts ? ts->send_to_client(client_id, data) : false;
  }

  void setup_internal_handlers() {
    if (!channel_) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(channel_->get_executor());

    std::weak_ptr<bool> weak_alive = alive_marker_;
    auto transport_server = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
    if (transport_server) {
      transport_server->on_multi_connect([this, weak_alive](ClientId id, const std::string& info) {
        auto alive = weak_alive.lock();
        if (!alive) return;

        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          if (framer_factory_) {
            auto framer = framer_factory_();
            if (framer) {
              auto shared_framer = std::shared_ptr<framer::IFramer>(std::move(framer));
              shared_framer->on_message([this, id](memory::ConstByteSpan msg) {
                // #441: snapshot under a shared_lock (pure read), build the
                // copy before taking the exclusive lock for queue mutation.
                bool batch_mode;
                MessageHandler on_message_handler;
                {
                  std::shared_lock<std::shared_mutex> lock(mutex_);
                  batch_mode = static_cast<bool>(message_batch_handler_);
                  on_message_handler = on_message_;
                }

                if (batch_mode) {
                  MessageContext ctx(id, memory::SafeDataBuffer(msg));
                  BatchMessageHandler flush_handler;
                  std::vector<MessageContext> batch;
                  {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    message_batch_queue_.emplace_back(std::move(ctx));
                    if (message_batch_queue_.size() >= max_batch_size_) {
                      flush_handler = message_batch_handler_;
                      batch = std::move(message_batch_queue_);
                      message_batch_queue_.clear();
                    } else if (message_batch_queue_.size() == 1) {
                      schedule_batch_timer();
                    }
                  }
                  if (flush_handler) flush_handler(batch);
                  return;
                }

                if (on_message_handler) {
                  on_message_handler(MessageContext(id, memory::SafeDataBuffer(msg)));
                }
              });
              framers_[id] = std::move(shared_framer);
            }
          }
          handler = on_client_connect_;
        }
        if (handler) handler(ConnectionContext(id, info));
      });
      transport_server->on_multi_data([this, weak_alive](ClientId id, memory::ConstByteSpan data_span) {
        auto alive = weak_alive.lock();
        if (!alive) return;

        // #441: snapshot the handler/framer pointers under a shared_lock
        // (not unique_lock) - this is a pure read, matching try_send's
        // locking level so it no longer blocks concurrent sends even
        // briefly.
        bool batch_mode;
        MessageHandler handler;
        std::shared_ptr<framer::IFramer> framer;
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          batch_mode = static_cast<bool>(data_batch_handler_);
          handler = on_data_;
          auto it = framers_.find(id);
          if (it != framers_.end()) {
            framer = it->second;
          }
        }

        if (batch_mode) {
          // #441: build the copy before taking the exclusive lock, so the
          // lock is only held for the queue mutation itself, not the
          // allocation.
          MessageContext ctx(id, memory::SafeDataBuffer(data_span));
          BatchMessageHandler flush_handler;
          std::vector<MessageContext> batch;
          {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            data_batch_queue_.emplace_back(std::move(ctx));
            if (data_batch_queue_.size() >= max_batch_size_) {
              flush_handler = data_batch_handler_;
              batch = std::move(data_batch_queue_);
              data_batch_queue_.clear();
            } else if (data_batch_queue_.size() == 1) {
              schedule_batch_timer();
            }
          }
          if (flush_handler) flush_handler(batch);
        } else if (handler) {
          handler(MessageContext(id, memory::SafeDataBuffer(data_span)));
        }

        if (framer) framer->push_bytes(data_span);
      });
      transport_server->on_multi_disconnect([this, weak_alive](ClientId id) {
        auto alive = weak_alive.lock();
        if (!alive) return;

        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          framers_.erase(id);
          handler = on_disconnect_;
        }
        if (handler) handler(ConnectionContext(id));
      });

      transport_server->on_backpressure([this, weak_alive](size_t queued) {
        bp_cv_.notify_all();
        auto alive = weak_alive.lock();
        if (!alive) return;
        std::function<void(size_t)> handler;
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          handler = on_backpressure_;
        }
        if (handler) handler(queued);
      });
    }
    channel_->on_state([this, weak_alive](base::LinkState state) {
      auto alive = weak_alive.lock();
      if (!alive) return;

      if (state == base::LinkState::Listening) {
        is_listening_.store(true);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        fulfill_all_locked(true);
      } else if (state == base::LinkState::Error || state == base::LinkState::Closed ||
                 state == base::LinkState::Idle) {
        ErrorHandler handler;
        is_listening_.store(false);
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          if (state == base::LinkState::Error) {
            handler = on_error_;
          }
        }
        if (handler) {
          handler(channel_ ? detail::build_error_context(*channel_, "Server error")
                           : ErrorContext(ErrorCode::IoError, "Server error"));
        }
      }
    });
  }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return channel_ ? channel_->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (channel_) channel_->reset_stats();
  }
};

TcpServer::TcpServer(uint16_t port) : impl_(std::make_unique<Impl>(port)) {}
TcpServer::TcpServer(uint16_t port, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_unique<Impl>(port, ioc)) {}
TcpServer::TcpServer(std::shared_ptr<interface::Channel> ch) : impl_(std::make_unique<Impl>(ch)) {}
TcpServer::~TcpServer() = default;

TcpServer::TcpServer(TcpServer&&) noexcept = default;
TcpServer& TcpServer::operator=(TcpServer&&) noexcept = default;

std::future<bool> TcpServer::start() { return impl_->start(); }
void TcpServer::stop() { impl_->stop(); }
bool TcpServer::listening() const { return get_impl()->is_listening_.load(); }
RuntimeStats TcpServer::stats() const { return get_impl()->stats(); }
void TcpServer::reset_stats() { impl_->reset_stats(); }

bool TcpServer::broadcast(std::string_view data) { return impl_->broadcast(data); }
bool TcpServer::try_broadcast(std::string_view data) { return impl_->try_broadcast(data); }
bool TcpServer::send_to(ClientId client_id, std::string_view data) { return impl_->send_to(client_id, data); }
bool TcpServer::try_send_to(ClientId client_id, std::string_view data) { return impl_->try_send_to(client_id, data); }

bool TcpServer::send_to_blocking(ClientId client_id, std::string_view data) {
  return impl_->send_to_blocking(client_id, data);
}

bool TcpServer::broadcast_line(std::string_view line) { return broadcast(std::string(line) + "\n"); }
bool TcpServer::send_to_line(ClientId client_id, std::string_view line) {
  return send_to(client_id, std::string(line) + "\n");
}
bool TcpServer::try_broadcast_line(std::string_view line) { return try_broadcast(std::string(line) + "\n"); }
bool TcpServer::try_send_to_line(ClientId client_id, std::string_view line) {
  return try_send_to(client_id, std::string(line) + "\n");
}

TcpServer& TcpServer::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_client_connect_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_disconnect_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_data_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_error_ = std::move(h);
  return *this;
}

TcpServer& TcpServer::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_backpressure_ = std::move(h);
  return *this;
}

TcpServer& TcpServer::framer(FramerFactory factory) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->framer_factory_ = std::move(factory);
  return *this;
}

TcpServer& TcpServer::on_message(MessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_message_ = std::move(handler);
  return *this;
}

TcpServer& TcpServer::on_message_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->message_batch_handler_ = std::move(h);
  return *this;
}

size_t TcpServer::client_count() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  const auto& ts = get_impl()->transport_cache_;
  return ts ? ts->client_count() : 0;
}

std::vector<ClientId> TcpServer::connected_clients() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  const auto& ts = get_impl()->transport_cache_;
  return ts ? ts->connected_clients() : std::vector<ClientId>();
}

TcpServer& TcpServer::auto_start(bool m) {
  impl_->auto_start_.store(m);
  if (impl_->auto_start_.load() && !impl_->started_.load()) start();
  return *this;
}

TcpServer& TcpServer::bind_address(const std::string& address) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->bind_address_ = address;
  return *this;
}

TcpServer& TcpServer::shared_context(bool use_shared) {
  impl_->shared_context_.store(use_shared);
  return *this;
}

TcpServer& TcpServer::port_retry(bool e, int m, int i) {
  impl_->port_retry_enabled_.store(e);
  impl_->max_port_retries_.store(m);
  impl_->port_retry_interval_ms_.store(i);
  return *this;
}

TcpServer& TcpServer::idle_timeout(std::chrono::milliseconds timeout) {
  impl_->idle_timeout_ms_.store(static_cast<int>(timeout.count()));
  return *this;
}

TcpServer& TcpServer::max_clients(size_t max) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_clients_.store(max);
  if (max == 0) {
    impl_->client_limit_enabled_.store(false);
  } else {
    impl_->client_limit_enabled_.store(true);
  }
  if (impl_->transport_cache_) impl_->transport_cache_->set_client_limit(max);
  return *this;
}

TcpServer& TcpServer::backpressure_threshold(size_t threshold) {
  impl_->backpressure_threshold_.store(threshold);
  return *this;
}

TcpServer& TcpServer::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->backpressure_strategy_.store(strategy);
  return *this;
}

TcpServer& TcpServer::tcp_no_delay(bool enable) {
  impl_->tcp_no_delay_.store(enable);
  return *this;
}

TcpServer& TcpServer::keep_alive(bool enable) {
  impl_->keep_alive_.store(enable);
  return *this;
}

TcpServer& TcpServer::send_buffer_size(size_t bytes) {
  impl_->send_buffer_size_.store(bytes);
  return *this;
}

TcpServer& TcpServer::receive_buffer_size(size_t bytes) {
  impl_->receive_buffer_size_.store(bytes);
  return *this;
}

TcpServer& TcpServer::manage_external_context(bool m) {
  impl_->manage_external_context_.store(m);
  return *this;
}

TcpServer& TcpServer::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

TcpServer& TcpServer::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
