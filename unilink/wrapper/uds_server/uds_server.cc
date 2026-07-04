#include "unilink/wrapper/uds_server/uds_server.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <shared_mutex>
#include <stop_token>
#include <thread>

#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/factory/channel_factory.hpp"
#include "unilink/framer/line_framer.hpp"
#include "unilink/framer/packet_framer.hpp"
#include "unilink/transport/uds/uds_server.hpp"
#include "unilink/wrapper/callback_guard.hpp"
#include "unilink/wrapper/error_context_builder.hpp"

namespace unilink {
namespace wrapper {

struct UdsServer::Impl : public std::enable_shared_from_this<Impl> {
  std::string socket_path_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};

  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  std::shared_ptr<interface::Channel> server_;
  std::vector<std::promise<bool>> pending_promises_;
  std::jthread external_thread_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::atomic<bool> started_{false};
  std::atomic<bool> is_listening_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  // Configuration
  std::atomic<bool> auto_start_{false};
  std::atomic<int> idle_timeout_ms_{static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS)};
  std::atomic<size_t> max_clients_{0};
  std::atomic<bool> client_limit_enabled_{false};
  std::atomic<int> socket_permissions_{-1};
  std::atomic<size_t> backpressure_threshold_{base::constants::DEFAULT_BACKPRESSURE_THRESHOLD};
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};

  ConnectionHandler client_connect_handler_{nullptr};
  ConnectionHandler client_disconnect_handler_{nullptr};
  MessageHandler data_handler_{nullptr};
  BatchMessageHandler data_batch_handler_{nullptr};
  ErrorHandler error_handler_{nullptr};
  std::function<void(size_t)> on_backpressure_{nullptr};
  FramerFactory framer_factory_{nullptr};
  MessageHandler on_message_{nullptr};
  BatchMessageHandler on_message_batch_{nullptr};

  std::unordered_map<ClientId, std::shared_ptr<framer::IFramer>> framers_;

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  explicit Impl(const std::string& socket_path) : socket_path_(socket_path) {}

  Impl(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc)
      : socket_path_(socket_path),
        external_ioc_(std::move(external_ioc)),
        use_external_context_(external_ioc_ != nullptr),
        manage_external_context_(false) {}

  explicit Impl(std::shared_ptr<interface::Channel> channel) : socket_path_(""), server_(std::move(channel)) {
    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to UdsServer's own constructor,
    // which runs after impl_ is a fully-formed shared_ptr<Impl>.
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

  bool try_send_to(ClientId client_id, std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto ts = std::dynamic_pointer_cast<transport::UdsServer>(server_);
    return ts ? ts->try_send_to_client(client_id, data) : false;
  }

  bool try_broadcast(std::string_view data) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto ts = std::dynamic_pointer_cast<transport::UdsServer>(server_);
    return ts ? ts->broadcast(data) : false;
  }

  bool send_to(ClientId client_id, std::string_view data) {
    if (backpressure_strategy_.load() == base::constants::BackpressureStrategy::Reliable)
      return send_to_blocking(client_id, data);
    return try_send_to(client_id, data);
  }

  bool broadcast(std::string_view data) { return try_broadcast(data); }

  // channel_->on_backpressure()/session-level backpressure callbacks call bp_cv_.notify_all()
  // from the transport's io_context thread without holding bp_mutex_ - a classic lost-wakeup
  // race is possible: a waiter can check the predicate, find it still blocking, and be in the
  // process of registering to wait when the notify fires. Poll with a bounded timeout instead
  // of an unbounded wait() so a missed notify only costs a short delay rather than a
  // permanent hang (see #427, #431).
  //
  // Returns false without sending instead of waiting if called from the
  // channel's own io thread while backpressure is active for this client -
  // e.g. a blocking send_to() called from inside an on_data/on_message
  // callback. Clearing backpressure requires that same io thread to make
  // progress, so blocking here would deadlock forever rather than
  // eventually clear (#449).
  // #509: see identical rationale in wrapper/tcp_server/tcp_server.cc -
  // bounded retry rather than a single attempt after the wait exits.
  static constexpr int kMaxBlockingSendAttempts = 5;

  bool send_to_blocking(ClientId client_id, std::string_view data) {
    for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
      std::unique_lock<std::mutex> lock(bp_mutex_);
      auto predicate = [this, client_id]() {
        std::shared_lock<std::shared_mutex> rlock(mutex_);
        auto ts = std::dynamic_pointer_cast<transport::UdsServer>(server_);
        return !started_.load() || !ts || !ts->is_backpressure_active(client_id);
      };
      if (!predicate() && detail::in_data_callback()) return false;
      while (!bp_cv_.wait_for(lock, std::chrono::milliseconds(50), predicate)) {
      }
      lock.unlock();
      std::shared_lock<std::shared_mutex> rlock(mutex_);
      auto ts = std::dynamic_pointer_cast<transport::UdsServer>(server_);
      if (!ts) return false;
      if (ts->send_to_client(client_id, data)) return true;
    }
    return false;
  }

  void schedule_batch_timer() {
    if (!batch_timer_) return;
    batch_timer_->expires_after(max_batch_latency_);
    batch_timer_->async_wait([this, weak_impl = weak_from_this(),
                              weak_alive = std::weak_ptr<bool>(alive_marker_)](const boost::system::error_code& ec) {
      if (ec) return;
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
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

    if (!server_) {
      config::UdsServerConfig config;
      config.socket_path = socket_path_;
      config.idle_timeout_ms = idle_timeout_ms_.load();
      config.max_connections =
          static_cast<int>(std::min(max_clients_.load(), static_cast<size_t>(base::constants::MAX_MAX_CONNECTIONS)));
      config.backpressure_threshold = backpressure_threshold_.load();
      config.backpressure_strategy = backpressure_strategy_.load();
      config.socket_permissions = socket_permissions_.load();
      server_ = factory::ChannelFactory::create(config, external_ioc_);
      setup_internal_handlers();
    }

    lock.unlock();
    server_->start();

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
        bp_cv_.notify_all();
        is_listening_.store(false);
        fulfill_all_locked(false);
        return;
      }
      bp_cv_.notify_all();

      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }

      if (server_) {
        server_->on_bytes(nullptr);
        server_->on_state(nullptr);
        server_->on_backpressure(nullptr);
        lock.unlock();
        server_->stop();
        lock.lock();
      }

      if (use_external_context_.load() && manage_external_context_.load()) {
        if (work_guard_) work_guard_.reset();
        if (external_ioc_) external_ioc_->stop();
        should_join = true;
      }

      is_listening_.store(false);
      framers_.clear();
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
    server_.reset();
  }

  void setup_internal_handlers() {
    if (!server_) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(server_->get_executor());

    std::weak_ptr<bool> weak_alive = alive_marker_;
    std::weak_ptr<Impl> weak_impl = weak_from_this();

    auto transport_server = std::dynamic_pointer_cast<transport::UdsServer>(server_);
    if (transport_server) {
      transport_server->on_multi_connect([this, weak_impl, weak_alive](ClientId id, const std::string& info) {
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;

        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          if (framer_factory_) {
            framers_[id] = framer_factory_();
            attach_framer_callback(id);
          }
          handler = client_connect_handler_;
        }
        if (handler) handler(ConnectionContext(id, info));
      });
      transport_server->on_multi_data([this, weak_impl, weak_alive](ClientId id, memory::ConstByteSpan data_span) {
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;

        // #449: everything below runs synchronously on this io thread -
        // mark it so a blocking send_to() called from within one of these
        // callbacks fails fast instead of deadlocking.
        detail::CallbackGuard callback_guard;

        // #441: snapshot the handler/framer pointers under a shared_lock
        // (not unique_lock) - this is a pure read, matching try_send's
        // locking level so it no longer blocks concurrent sends even
        // briefly.
        bool batch_mode;
        MessageHandler handler;
        std::shared_ptr<framer::IFramer> framer_to_push;
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          batch_mode = static_cast<bool>(data_batch_handler_);
          handler = data_handler_;
          auto it = framers_.find(id);
          if (it != framers_.end()) {
            framer_to_push = it->second;
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

        if (framer_to_push) framer_to_push->push_bytes(data_span);
      });
      transport_server->on_multi_disconnect([this, weak_impl, weak_alive](ClientId id) {
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;

        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          framers_.erase(id);
          handler = client_disconnect_handler_;
        }
        if (handler) handler(ConnectionContext(id));
      });

      transport_server->on_backpressure([this, weak_impl, weak_alive](size_t queued) {
        bp_cv_.notify_all();
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
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

    server_->on_state([this, weak_impl, weak_alive](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
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
            handler = error_handler_;
          }
        }
        if (handler) {
          handler(server_ ? detail::build_error_context(*server_, "Server error")
                          : ErrorContext(ErrorCode::IoError, "Server error"));
        }
      }
    });
  }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return server_ ? server_->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (server_) server_->reset_stats();
  }

  void attach_framer_callback(ClientId id) {
    auto it = framers_.find(id);
    if (it == framers_.end()) return;

    it->second->on_message([this, id](memory::ConstByteSpan msg) {
      // #441: snapshot under a shared_lock (pure read), build the copy
      // before taking the exclusive lock for queue mutation.
      bool batch_mode;
      MessageHandler handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        batch_mode = static_cast<bool>(on_message_batch_);
        handler = on_message_;
      }

      if (batch_mode) {
        MessageContext ctx(id, memory::SafeDataBuffer(msg));
        BatchMessageHandler flush_handler;
        std::vector<MessageContext> batch;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          message_batch_queue_.emplace_back(std::move(ctx));
          if (message_batch_queue_.size() >= max_batch_size_) {
            flush_handler = on_message_batch_;
            batch = std::move(message_batch_queue_);
            message_batch_queue_.clear();
          } else if (message_batch_queue_.size() == 1) {
            schedule_batch_timer();
          }
        }
        if (flush_handler) flush_handler(batch);
        return;
      }

      if (handler) {
        handler(MessageContext(id, memory::SafeDataBuffer(msg)));
      }
    });
  }
};

UdsServer::UdsServer(const std::string& socket_path) : impl_(std::make_shared<Impl>(socket_path)) {}

UdsServer::UdsServer(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc)
    : impl_(std::make_shared<Impl>(socket_path, std::move(external_ioc))) {}

UdsServer::UdsServer(std::shared_ptr<interface::Channel> channel) : impl_(std::make_shared<Impl>(std::move(channel))) {
  impl_->setup_internal_handlers();
}

UdsServer::~UdsServer() = default;

UdsServer::UdsServer(UdsServer&&) noexcept = default;
UdsServer& UdsServer::operator=(UdsServer&&) noexcept = default;

std::future<bool> UdsServer::start() { return impl_->start(); }

void UdsServer::stop() { impl_->stop(); }

bool UdsServer::listening() const { return impl_->is_listening_.load(); }
RuntimeStats UdsServer::stats() const { return impl_->stats(); }
void UdsServer::reset_stats() { impl_->reset_stats(); }

bool UdsServer::broadcast(std::string_view data) { return impl_->broadcast(data); }
bool UdsServer::try_broadcast(std::string_view data) { return impl_->try_broadcast(data); }
bool UdsServer::send_to(ClientId client_id, std::string_view data) { return impl_->send_to(client_id, data); }
bool UdsServer::try_send_to(ClientId client_id, std::string_view data) { return impl_->try_send_to(client_id, data); }

bool UdsServer::send_to_blocking(ClientId client_id, std::string_view data) {
  return impl_->send_to_blocking(client_id, data);
}

bool UdsServer::broadcast_line(std::string_view line) { return broadcast(std::string(line) + "\n"); }
bool UdsServer::send_to_line(ClientId client_id, std::string_view line) {
  return send_to(client_id, std::string(line) + "\n");
}
bool UdsServer::try_broadcast_line(std::string_view line) { return try_broadcast(std::string(line) + "\n"); }
bool UdsServer::try_send_to_line(ClientId client_id, std::string_view line) {
  return try_send_to(client_id, std::string(line) + "\n");
}

UdsServer& UdsServer::on_connect(ConnectionHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->client_connect_handler_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_disconnect(ConnectionHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->client_disconnect_handler_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_data(MessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_handler_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_data_batch(BatchMessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_error(ErrorHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->error_handler_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_backpressure(std::function<void(size_t)> handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_backpressure_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::framer(FramerFactory factory) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->framer_factory_ = std::move(factory);
  return *this;
}

UdsServer& UdsServer::on_message(MessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_message_ = std::move(handler);
  return *this;
}

UdsServer& UdsServer::on_message_batch(BatchMessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_message_batch_ = std::move(handler);
  return *this;
}

size_t UdsServer::client_count() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  auto ts = std::dynamic_pointer_cast<transport::UdsServer>(impl_->server_);
  return ts ? ts->client_count() : 0;
}

std::vector<ClientId> UdsServer::connected_clients() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  auto ts = std::dynamic_pointer_cast<transport::UdsServer>(impl_->server_);
  return ts ? ts->connected_clients() : std::vector<ClientId>{};
}

UdsServer& UdsServer::auto_start(bool manage) {
  impl_->auto_start_.store(manage);
  if (impl_->auto_start_.load() && !impl_->started_.load()) {
    start();
  }
  return *this;
}

UdsServer& UdsServer::idle_timeout(std::chrono::milliseconds timeout) {
  impl_->idle_timeout_ms_.store(static_cast<int>(timeout.count()));
  return *this;
}

UdsServer& UdsServer::max_clients(size_t max) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_clients_.store(max);
  if (max == 0) {
    impl_->client_limit_enabled_.store(false);
  } else {
    impl_->client_limit_enabled_.store(true);
  }
  auto ts = std::dynamic_pointer_cast<transport::UdsServer>(impl_->server_);
  if (ts) ts->set_client_limit(max);
  return *this;
}

UdsServer& UdsServer::socket_permissions(int mode) {
  impl_->socket_permissions_.store(mode);
  return *this;
}

UdsServer& UdsServer::backpressure_threshold(size_t threshold) {
  impl_->backpressure_threshold_.store(threshold);
  return *this;
}

UdsServer& UdsServer::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->backpressure_strategy_.store(strategy);
  return *this;
}

UdsServer& UdsServer::manage_external_context(bool manage) {
  impl_->manage_external_context_.store(manage);
  return *this;
}

UdsServer& UdsServer::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

UdsServer& UdsServer::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace unilink
