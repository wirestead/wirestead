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

#include "wirestead/wrapper/uds_client/uds_client.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/config/uds_config.hpp"
#include "wirestead/diagnostics/error_mapping.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/error_context_builder.hpp"
#include "wirestead/wrapper/send_validation.hpp"

namespace wirestead {
namespace wrapper {

struct UdsClient::Impl : public std::enable_shared_from_this<Impl> {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  // D-1: admission gate for this object's user callbacks. Admission, the
  // running count and the closed flag are one decision, so a stop() cannot
  // observe an empty gate while a callback is about to start, and a callback
  // left over from a previous run is refused after a restart.
  detail::CallbackGate callback_gate_;
  std::mutex stop_finalize_mutex_;
  bool injected_channel_ = false;
  std::atomic<uint64_t> callback_generation_{0};

  // True when this thread is one the target's shutdown needs: a callback of
  // this object, or any thread currently running the external io_context this
  // channel was built on (which a callback of another channel sharing it is).
  // Such a caller requests the shutdown and returns; it cannot wait for work
  // its own thread has to perform.
  bool shutdown_needs_this_thread() const {
    if (callback_gate_.active_on_this_thread()) return true;
    if (external_ioc_ && external_ioc_->get_executor().running_in_this_thread()) return true;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!channel_) return false;
    const auto executor = channel_->get_executor();
    using IoExecutor = boost::asio::io_context::executor_type;
    if (auto* io = executor.target<IoExecutor>()) return io->running_in_this_thread();
    if (auto* strand = executor.target<boost::asio::strand<IoExecutor>>())
      return strand->get_inner_executor().running_in_this_thread();
    return false;
  }

  std::string socket_path_;
  std::shared_ptr<interface::Channel> channel_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};
  std::jthread external_thread_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::vector<std::promise<bool>> pending_promises_;
  std::atomic<bool> started_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  // Shared snapshots: the io thread copies one out per received chunk, and a
  // std::function copy allocates whenever the user handler outgrows its
  // small-object buffer. See interface::SharedCallback.
  interface::SharedCallback<MessageHandler> data_handler_;
  interface::SharedCallback<BatchMessageHandler> data_batch_handler_;
  ConnectionHandler connect_handler_{nullptr};
  ConnectionHandler disconnect_handler_{nullptr};
  ErrorHandler error_handler_{nullptr};
  std::function<void(size_t)> bp_handler_{nullptr};
  interface::SharedCallback<MessageHandler> message_handler_;
  interface::SharedCallback<BatchMessageHandler> message_batch_handler_;

  std::shared_ptr<framer::IFramer> framer_{nullptr};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  std::atomic<bool> auto_start_ = false;
  std::chrono::milliseconds retry_interval_{base::constants::DEFAULT_RETRY_INTERVAL_MS};
  int max_retries_ = base::constants::DEFAULT_MAX_RETRIES;
  std::chrono::milliseconds connection_timeout_{base::constants::DEFAULT_CONNECTION_TIMEOUT_MS};
  size_t backpressure_threshold_ = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  size_t read_buffer_size_ = base::constants::DEFAULT_READ_BUFFER_SIZE;
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
    injected_channel_ = true;
    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to UdsClient's own constructor,
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

  void flush_batches(uint64_t generation) {
    auto lease = callback_gate_.enter(generation);
    if (!lease.admitted()) return;
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!data_batch_queue_.empty()) {
      auto handler = data_batch_handler_;
      auto batch = std::move(data_batch_queue_);
      data_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("uds_client", "on_data_batch", handler, batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = message_batch_handler_;
      auto batch = std::move(message_batch_queue_);
      message_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("uds_client", "on_message_batch", handler, batch);
        lock.lock();
      }
    }
    if (batch_timer_) {
      batch_timer_->cancel();
    }
  }

  void schedule_batch_timer(uint64_t generation) {
    if (!batch_timer_) return;
    batch_timer_->expires_after(max_batch_latency_);
    batch_timer_->async_wait([this, generation, weak_impl = weak_from_this(),
                              weak_alive = std::weak_ptr<bool>(alive_marker_)](const boost::system::error_code& ec) {
      if (ec) return;
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      flush_batches(generation);
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

    if (!alive_marker_) alive_marker_ = std::make_shared<bool>(true);
    if (!channel_) {
      config::UdsClientConfig cfg;
      cfg.socket_path = socket_path_;
      cfg.retry_interval_ms = static_cast<unsigned>(retry_interval_.count());
      cfg.max_retries = max_retries_;
      cfg.connection_timeout_ms = static_cast<unsigned>(connection_timeout_.count());
      cfg.backpressure_threshold = backpressure_threshold_;
      cfg.read_buffer_size = read_buffer_size_;
      cfg.backpressure_strategy = backpressure_strategy_;

      channel_ = use_external_context_ ? factory::ChannelFactory::create(cfg, external_ioc_)
                                       : factory::ChannelFactory::create(cfg);
    }
    setup_internal_handlers();
    started_.store(true);
    if (use_external_context_.load() && manage_external_context_.load() && !external_thread_.joinable()) {
      if (external_ioc_ && external_ioc_->stopped()) {
        external_ioc_->restart();
      }
      work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          external_ioc_->get_executor());
      external_thread_ = std::jthread([ioc = external_ioc_](std::stop_token st) {
        wirestead::concurrency::run_io_thread_init();
        try {
          std::stop_callback cb(st, [ioc] { ioc->stop(); });
          ioc->run();
        } catch (...) {
        }
      });
    }
    auto channel_copy = channel_;
    lock.unlock();
    channel_copy->start();
    return f;
  }

  void stop() {
    const bool request_only = shutdown_needs_this_thread();
    callback_gate_.close();
    std::shared_ptr<interface::Channel> channel;
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      started_.store(false);
      bp_cv_.notify_all();
      fulfill_all_locked(false);
      channel = channel_;
    }
    // Do not hold wrapper locks while the transport waits for callbacks.
    if (channel) channel->stop();
    if (request_only) return;

    callback_gate_.wait_until_idle();
    // All outside callers pass this lock, including those that found the
    // channel already cleared. Joining and final state reset happen once.
    std::lock_guard<std::mutex> finalize_lock(stop_finalize_mutex_);
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      alive_marker_.reset();
      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }
      if (channel_) {
        channel_->on_bytes(nullptr);
        channel_->on_state(nullptr);
        channel_->on_backpressure(nullptr);
      }
      if (!injected_channel_) channel_.reset();
      data_batch_queue_.clear();
      message_batch_queue_.clear();
      if (framer_) framer_->reset();
    }
    if (use_external_context_.load() && manage_external_context_.load()) {
      if (work_guard_) work_guard_.reset();
      if (external_ioc_) external_ioc_->stop();
      if (external_thread_.joinable()) external_thread_.join();
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
  // Returns false instead of waiting if called from the channel's own io
  // thread while backpressure is active - e.g. a blocking send() called
  // from inside an on_data/on_message callback. Clearing backpressure
  // requires that same io thread to make progress, so blocking here would
  // deadlock forever rather than eventually clear (#449).
  bool wait_for_backpressure_clear(std::unique_lock<std::mutex>& bp_lock, size_t payload_size) {
    if (!detail::payload_needs_capacity(payload_size)) return true;
    auto predicate = [this, payload_size] {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      return !started_.load() || !channel_ || !channel_->is_connected() ||
             !detail::payload_needs_capacity(payload_size, channel_->write_queue_limit()) ||
             !channel_->is_backpressure_active();
    };
    if (predicate()) return true;
    if (detail::in_data_callback()) return false;
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), predicate)) {
    }
    return true;
  }

  // #509: see identical rationale in wrapper/tcp_client/tcp_client.cc -
  // wait_for_backpressure_clear()'s condition and the transport's own hard
  // queue-byte cap are different thresholds observed at different times, so
  // a single write attempt can spuriously fail right after the wait exits.
  // Bounded retry rather than unbounded, so a payload that can never fit
  // still fails in bounded time.
  static constexpr int kMaxBlockingSendAttempts = 5;

  bool send_move(std::vector<uint8_t>&& data) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) {
      for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        if (!wait_for_backpressure_clear(bp_lock, data.size())) return false;
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
        if (channel_->async_write_move(std::move(data))) return true;
      }
      return false;
    }
    return try_send_move(std::move(data));
  }

  bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (!data || data->empty()) return false;
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) {
      for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        if (!wait_for_backpressure_clear(bp_lock, data->size())) return false;
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
        if (channel_->async_write_shared(data)) return true;
      }
      return false;
    }
    return try_send_shared(std::move(data));
  }

  bool send_line(std::string_view line) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) return send_line_blocking(line);
    return try_send_line(line);
  }

  bool try_send_line(std::string_view line) { return try_send(std::string(line) + "\n"); }

  bool send_blocking(std::string_view data) {
    memory::ConstByteSpan span(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
      std::unique_lock<std::mutex> bp_lock(bp_mutex_);
      if (!wait_for_backpressure_clear(bp_lock, data.size())) return false;
      bp_lock.unlock();
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (!started_.load() || !channel_ || !channel_->is_connected()) return false;
      if (channel_->async_write_copy(span)) return true;
    }
    return false;
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
    std::weak_ptr<Impl> weak_impl = weak_from_this();
    const auto generation = callback_gate_.open_new_generation();
    callback_generation_.store(generation);

    channel_->on_state([this, generation, weak_impl, weak_alive](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;

      if (state == base::LinkState::Connected) {
        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(true);
          handler = connect_handler_;
        }
        detail::invoke_user_callback("uds_client", "on_connect", handler, ConnectionContext(0));
      } else if (state == base::LinkState::Error) {
        ErrorHandler handler;
        std::shared_ptr<interface::Channel> channel_snapshot;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          handler = error_handler_;
          channel_snapshot = channel_;
        }
        detail::invoke_user_callback("uds_client", "on_error", handler,
                                     channel_snapshot
                                         ? detail::build_error_context(*channel_snapshot, "Connection error")
                                         : ErrorContext(ErrorCode::IoError, "Connection error"));
      } else if (state == base::LinkState::Closed || state == base::LinkState::Idle) {
        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          handler = disconnect_handler_;
        }
        detail::invoke_user_callback("uds_client", "on_disconnect", handler, ConnectionContext(0));
      }
    });

    channel_->on_bytes([this, generation, weak_impl, weak_alive](memory::ConstByteSpan data) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;

      // #449: everything below runs synchronously on this io thread - mark
      // it so a blocking send() called from within one of these callbacks
      // fails fast instead of deadlocking.
      detail::CallbackGuard callback_guard;

      // #441: snapshot the handler/framer pointers under a shared_lock (not
      // unique_lock) - this is a pure read, matching try_send's locking
      // level so it no longer blocks concurrent sends even briefly.
      bool batch_mode;
      interface::SharedCallback<MessageHandler> handler;
      std::shared_ptr<framer::IFramer> framer_to_push;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        batch_mode = static_cast<bool>(data_batch_handler_);
        handler = data_handler_;
        framer_to_push = framer_;
      }

      if (batch_mode) {
        // #441: build the copy before taking the exclusive lock, so the
        // lock is only held for the queue mutation itself, not the
        // allocation.
        MessageContext ctx(0, memory::SafeDataBuffer(data));
        interface::SharedCallback<BatchMessageHandler> flush_handler;
        std::vector<MessageContext> batch;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          data_batch_queue_.emplace_back(std::move(ctx));
          if (data_batch_queue_.size() >= max_batch_size_) {
            flush_handler = data_batch_handler_;
            batch = std::move(data_batch_queue_);
            data_batch_queue_.clear();
          } else if (data_batch_queue_.size() == 1) {
            schedule_batch_timer(generation);
          }
        }
        detail::invoke_user_callback("uds_client", "on_data_batch", flush_handler, batch);
      } else {
        detail::invoke_user_callback("uds_client", "on_data", handler, MessageContext(0, data));
      }

      if (framer_to_push) framer_to_push->push_bytes(data);
    });

    channel_->on_backpressure([this, generation, weak_impl, weak_alive](size_t queued) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      bp_cv_.notify_all();
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        handler = bp_handler_;
      }
      detail::invoke_user_callback("uds_client", "on_backpressure", handler, queued);
    });
  }

  // Attach the stored message_handler_ or message_batch_handler_ to framer_->on_message().
  // Must be called with mutex_ already held.
  void attach_framer_callback() {
    if (!framer_) return;
    framer_->on_message([this](memory::ConstByteSpan msg) {
      // Framing runs under the enclosing data callback lease.
      const auto generation = callback_generation_.load();
      // #441: snapshot under a shared_lock (pure read), build the copy
      // before taking the exclusive lock for queue mutation.
      bool batch_mode;
      interface::SharedCallback<MessageHandler> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        batch_mode = static_cast<bool>(message_batch_handler_);
        handler = message_handler_;
      }

      if (batch_mode) {
        MessageContext ctx(0, memory::SafeDataBuffer(msg));
        interface::SharedCallback<BatchMessageHandler> flush_handler;
        std::vector<MessageContext> batch;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          message_batch_queue_.emplace_back(std::move(ctx));
          if (message_batch_queue_.size() >= max_batch_size_) {
            flush_handler = message_batch_handler_;
            batch = std::move(message_batch_queue_);
            message_batch_queue_.clear();
          } else if (message_batch_queue_.size() == 1) {
            schedule_batch_timer(generation);
          }
        }
        detail::invoke_user_callback("uds_client", "on_message_batch", flush_handler, batch);
        return;
      }

      detail::invoke_user_callback("uds_client", "on_message", handler, MessageContext(0, msg));
    });
  }

  void set_framer(std::unique_ptr<framer::IFramer> framer) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    framer_ = std::shared_ptr<framer::IFramer>(std::move(framer));
    if (framer_ && (message_handler_ || message_batch_handler_)) attach_framer_callback();
  }

  void on_message(MessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_handler_ = interface::share_callback(std::move(handler));
    if (framer_) attach_framer_callback();
  }

  void on_message_batch(BatchMessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_batch_handler_ = interface::share_callback(std::move(handler));
    if (framer_) attach_framer_callback();
  }
};

UdsClient::UdsClient(const std::string& socket_path) : impl_(std::make_shared<Impl>(socket_path)) {}

UdsClient::UdsClient(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc)
    : impl_(std::make_shared<Impl>(socket_path, std::move(external_ioc))) {}

UdsClient::UdsClient(std::shared_ptr<interface::Channel> channel) : impl_(std::make_shared<Impl>(std::move(channel))) {
  impl_->setup_internal_handlers();
}

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
  impl_->data_handler_ = interface::share_callback(std::move(handler));
  return *this;
}

UdsClient& UdsClient::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = interface::share_callback(std::move(h));
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

UdsClient& UdsClient::read_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->read_buffer_size_ = bytes;
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

size_t UdsClient::backpressure_threshold() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_threshold_;
}

base::constants::BackpressureStrategy UdsClient::backpressure_strategy() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_strategy_;
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
}  // namespace wirestead
