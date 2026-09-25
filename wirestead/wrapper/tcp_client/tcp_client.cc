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

#include "wirestead/wrapper/tcp_client/tcp_client.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/diagnostics/error_mapping.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/interface/connection_channel.hpp"
#include "wirestead/transport/tcp_client/detail/write_wait.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/error_context_builder.hpp"
#include "wirestead/wrapper/send_validation.hpp"

namespace wirestead {
namespace wrapper {

struct TcpClient::Impl : public std::enable_shared_from_this<Impl> {
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
  bool stop_requested_ = false;  // Protected by mutex_.
  std::atomic<unsigned> stop_callers_{0};
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
  bool tls_enabled_{false};
  std::string tls_ca_file_;
  std::chrono::milliseconds idle_timeout_{base::constants::DEFAULT_IDLE_TIMEOUT_MS};
  IdleTimeoutAction idle_timeout_action_{IdleTimeoutAction::Reconnect};
  size_t backpressure_threshold_{base::constants::DEFAULT_BACKPRESSURE_THRESHOLD};
  // Atomic rather than mutex-guarded: read from the send()/send_line() fast
  // path on arbitrary caller threads while the setter can be called
  // concurrently from any other thread (#436).
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};
  bool tcp_no_delay_ = base::constants::DEFAULT_TCP_NO_DELAY;
  bool keep_alive_ = base::constants::DEFAULT_KEEP_ALIVE;
  size_t send_buffer_size_ = 0;
  size_t receive_buffer_size_ = 0;
  size_t read_buffer_size_ = base::constants::DEFAULT_READ_BUFFER_SIZE;

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
    injected_channel_ = true;
    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to TcpClient's own constructor,
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
        detail::invoke_user_callback("tcp_client", "on_data_batch", handler, batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = message_batch_handler_;
      auto batch = std::move(message_batch_queue_);
      message_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("tcp_client", "on_message_batch", handler, batch);
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
      // #450: keep Impl alive for the duration of this callback - on an
      // externally-owned io_context, stop() doesn't join/wait for
      // in-flight handlers, so a bare `this` could otherwise dangle.
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      flush_batches(generation);
    });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stop_requested_ = false;
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
      config.tls_enabled = tls_enabled_;
      config.tls_ca_file = tls_ca_file_;
      config.idle_timeout_ms = static_cast<unsigned>(idle_timeout_.count());
      config.idle_timeout_action = idle_timeout_action_;
      config.backpressure_threshold = backpressure_threshold_;
      config.backpressure_strategy = backpressure_strategy_;
      config.tcp_no_delay = tcp_no_delay_;
      config.keep_alive = keep_alive_;
      config.send_buffer_size = send_buffer_size_;
      config.receive_buffer_size = receive_buffer_size_;
      config.read_buffer_size = read_buffer_size_;
      channel_ = factory::ChannelFactory::create(config, external_ioc_);
    }

    // D-1: the new run's generation is opened and the gate admits again in
    // one step, so no callback of the previous run can be admitted in
    // between. Handlers are (re-)registered for this generation, including on
    // an injected channel, where the same object serves every run.
    setup_internal_handlers();

    started_.store(true);
    channel_->start();
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
    return f;
  }

  void stop() {
    stop_callers_.fetch_add(1);
    struct StopCall {
      std::atomic<unsigned>& callers;
      ~StopCall() { callers.fetch_sub(1); }
    } stop_call{stop_callers_};
    const bool request_only = shutdown_needs_this_thread();
    callback_gate_.close();
    std::shared_ptr<interface::Channel> channel;
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      stop_requested_ = true;
      if (auto tcp = std::dynamic_pointer_cast<transport::TcpClient>(channel_)) tcp->cancel_write_waits();
      if (auto custom = std::dynamic_pointer_cast<interface::ConnectionChannel>(channel_)) custom->cancel_write_waits();
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

  // Caller holds mutex_. Native readiness remains part of transport admission.
  SendResult send_state(const std::shared_ptr<transport::TcpClient>& tcp, bool custom = false) {
    if (stop_callers_.load() != 0) return SendResult::reject(SendRejection::Stopping);
    if (!started_.load()) {
      if (stop_requested_) {
        if (!callback_gate_.idle()) return SendResult::reject(SendRejection::Stopping);
        if (tcp) {
          const auto state = tcp->write_state();
          if (!state.accepted() && state.reason() == SendRejection::Stopping) return state;
        }
      }
      return SendResult::reject(SendRejection::NotStarted);
    }
    if (!tcp && !custom) return SendResult::reject(SendRejection::NotReady);
    return SendResult::accept();
  }

  static bool finish_send(SendResult result) {
    if (auto hook = detail::g_tcp_send_result_hook.load()) hook(result);
    return result.accepted();
  }

  template <typename NativeWrite, typename FallbackWrite, typename CustomWrite>
  bool nonblocking_send(size_t size, bool best_effort_send, NativeWrite native_write, FallbackWrite fallback_write,
                        CustomWrite custom_write) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto tcp = std::dynamic_pointer_cast<transport::TcpClient>(channel_);
    auto custom = std::dynamic_pointer_cast<interface::ConnectionChannel>(channel_);
    // Legacy Channels retain their bool contract without invented reasons.
    if (channel_ && !tcp && !custom) return channel_->is_connected() && fallback_write(*channel_);
    const auto result = [&]() -> SendResult {
      auto validation = detail::validate_payload_size(size, channel_ ? channel_->write_queue_limit() : std::nullopt);
      if (!validation.accepted()) return validation;
      const auto state = send_state(tcp, custom != nullptr);
      if (!state.accepted()) return state;
      // Admission rechecks state and capacity together under the channel lock.
      auto admitted = custom ? custom_write(*custom) : native_write(*tcp);
      if (best_effort_send && !admitted.accepted() && admitted.reason() == SendRejection::WouldBlock)
        return SendResult::reject(SendRejection::QueueFull);
      return admitted;
    }();
    lock.unlock();
    return finish_send(result);
  }

  bool try_send(std::string_view data, bool best_effort_send = false) {
    auto binary_view = base::safe_convert::string_to_bytes(data);
    memory::ConstByteSpan span(binary_view.first, binary_view.second);
    return nonblocking_send(
        data.size(), best_effort_send, [&](auto& tcp) { return tcp.try_write_copy(span); },
        [&](auto& channel) { return channel.async_try_write_copy(span); },
        [&](auto& channel) { return channel.async_try_write_copy_result(span); });
  }

  bool try_send_move(std::vector<uint8_t>&& data, bool best_effort_send = false) {
    return nonblocking_send(
        data.size(), best_effort_send, [&](auto& tcp) { return tcp.try_write_move(std::move(data)); },
        [&](auto& channel) { return channel.async_try_write_move(std::move(data)); },
        [&](auto& channel) { return channel.async_try_write_move_result(std::move(data)); });
  }

  bool try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data, bool best_effort_send = false) {
    return nonblocking_send(
        data ? data->size() : 0, best_effort_send, [&](auto& tcp) { return tcp.try_write_shared(std::move(data)); },
        [&](auto& channel) { return data && !data->empty() && channel.async_try_write_shared(std::move(data)); },
        [&](auto& channel) { return channel.async_try_write_shared_result(std::move(data)); });
  }

  bool send(std::string_view data) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) return send_blocking(data);
    return try_send(data, true);
  }

  struct ConnectionPin {
    std::shared_ptr<interface::ConnectionChannel> custom;
    interface::ConnectionChannel::Connection custom_wait;
    std::shared_ptr<transport::TcpClient> tcp;
    std::shared_ptr<transport::detail::TcpWriteWait> wait;
  };

  bool connection_matches(const ConnectionPin& pin) const {
    return !pin.tcp || (pin.wait && pin.tcp->write_connection() == pin.wait->sequence);
  }

  // channel_->on_backpressure() calls bp_cv_.notify_all() from the transport's io_context
  // thread without holding bp_mutex_ (backpressure_active_ is a plain atomic on the transport
  // side, not guarded by bp_mutex_ at all). That makes a classic lost-wakeup race possible: a
  // waiter can check the predicate, find it still blocking, and be in the process of
  // registering to wait when the notify fires - in the rare case that race is lost, an
  // unbounded wait() would block forever. Poll with a bounded timeout instead so a missed
  // notify only costs a short delay rather than a permanent hang (see #427, #431).
  //
  // Callback scopes cannot wait for capacity: they may be running on the
  // executor needed to drain their own or another channel's queue (D-2).
  SendResult wait_for_backpressure_clear(std::unique_lock<std::mutex>& bp_lock, size_t payload_size,
                                         uint64_t generation, const ConnectionPin& connection) {
    if (connection.custom_wait) {
      auto outcome = connection.custom_wait->poll_capacity();
      if (outcome) return *outcome;
      if (detail::in_data_callback()) return SendResult::reject(SendRejection::WouldBlock);
      if (auto hook = detail::g_tcp_capacity_wait_hook.load()) hook();
      while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), [&] {
        outcome = connection.custom_wait->poll_capacity();
        return outcome.has_value();
      })) {
      }
      if (auto hook = detail::g_tcp_capacity_wait_result_hook.load()) hook(*outcome);
      return *outcome;
    }
    if (!detail::payload_needs_capacity(payload_size)) return SendResult::accept();
    auto immediate = [this, payload_size, generation, &connection] {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      return callback_generation_.load() != generation || !started_.load() || !channel_ || !channel_->is_connected() ||
             !connection_matches(connection) ||
             !detail::payload_needs_capacity(payload_size, channel_->write_queue_limit()) ||
             !channel_->is_backpressure_active();
    };
    // A bypass is not a completed capacity wait; final admission checks still apply.
    if (immediate()) return SendResult::accept();
    if (detail::in_data_callback()) return SendResult::reject(SendRejection::WouldBlock);
    if (auto hook = detail::g_tcp_capacity_wait_hook.load()) hook();
    std::optional<SendResult> outcome;
    auto released = [&] {
      if (outcome) return true;
      std::shared_lock<std::shared_mutex> lock(mutex_);
      if (connection.tcp) {
        // The connection record retains the first stop/loss cause. Sampling
        // readiness and capacity uses the same mutex as those terminal events.
        outcome = connection.tcp->poll_write_wait(connection.wait);
      } else if (callback_generation_.load() != generation || !started_.load()) {
        outcome = SendResult::reject(SendRejection::CancelledWhileWaiting);
      } else if (!channel_ || !channel_->is_connected()) {
        outcome = SendResult::reject(SendRejection::NotReady);
      } else if (!detail::payload_needs_capacity(payload_size, channel_->write_queue_limit()) ||
                 !channel_->is_backpressure_active()) {
        outcome = SendResult::accept();
      }
      return outcome.has_value();
    };
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), released)) {
    }
    if (auto hook = detail::g_tcp_capacity_wait_result_hook.load()) hook(*outcome);
    return *outcome;
  }

  // #509: high-water pressure and hard-limit reservations are different
  // thresholds. Capacity can be refilled between a wait and admission, so
  // retry transient native WouldBlock at most five times. Validation and
  // terminal state failures return immediately. Custom bool channels retain
  // their existing bounded retry behavior.
  static constexpr int kMaxBlockingSendAttempts = 5;

  template <typename NativeWrite, typename FallbackWrite, typename CustomWrite>
  bool blocking_send(size_t size, NativeWrite native_write, FallbackWrite fallback_write, CustomWrite custom_write) {
    uint64_t generation;
    ConnectionPin connection;
    bool custom;
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      generation = callback_generation_.load();
      connection.tcp = std::dynamic_pointer_cast<transport::TcpClient>(channel_);
      connection.custom = std::dynamic_pointer_cast<interface::ConnectionChannel>(channel_);
      custom = channel_ && !connection.tcp && !connection.custom;
    }
    if (custom) {
      // Preserve bool-only custom Channel admission, validation and accounting.
      for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        if (!wait_for_backpressure_clear(bp_lock, size, generation, connection).accepted()) return false;
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (callback_generation_.load() != generation || !started_.load() || !channel_ || !channel_->is_connected())
          return false;
        if (fallback_write(*channel_)) return true;
      }
      return false;
    }

    const auto result = [&]() -> SendResult {
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        // Select the run and native connection together at entry. Validation
        // precedes state and waiting, including the line delimiter and hard cap.
        generation = callback_generation_.load();
        connection.tcp = std::dynamic_pointer_cast<transport::TcpClient>(channel_);
        connection.custom = std::dynamic_pointer_cast<interface::ConnectionChannel>(channel_);
        auto validation = detail::validate_payload_size(size, channel_ ? channel_->write_queue_limit() : std::nullopt);
        if (!validation.accepted()) return validation;
        auto state = send_state(connection.tcp, connection.custom != nullptr);
        if (!state.accepted()) return state;
        if (connection.custom) {
          auto captured = connection.custom->capture_write_connection();
          if (auto reason = std::get_if<SendRejection>(&captured)) return SendResult::reject(*reason);
          connection.custom_wait = std::get<interface::ConnectionChannel::Connection>(std::move(captured));
          if (!connection.custom_wait) throw std::logic_error("ConnectionChannel returned a null connection");
        } else {
          state = connection.tcp->write_state();
          if (!state.accepted()) return state;
          connection.wait = connection.tcp->capture_write_wait();
          if (!connection.wait) return SendResult::reject(SendRejection::NotReady);
        }
      }
      for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        const auto released = wait_for_backpressure_clear(bp_lock, size, generation, connection);
        if (!released.accepted()) return released;  // Never overwrite the cause of release.
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto state = send_state(connection.tcp, connection.custom != nullptr);
        if (!state.accepted()) return state;
        if (callback_generation_.load() != generation) return SendResult::reject(SendRejection::NotReady);
        const auto admitted = connection.custom_wait ? custom_write(*connection.custom_wait)
                                                     : native_write(*connection.tcp, connection.wait->sequence);
        if (admitted.accepted() || admitted.reason() != SendRejection::WouldBlock) return admitted;
        // Only transient capacity refusal can be retried, never a terminal
        // result. Callback callers must return without entering another wait.
        if (detail::in_data_callback()) return admitted;
      }
      return SendResult::reject(SendRejection::WouldBlock);
    }();
    return finish_send(result);
  }

  bool send_move(std::vector<uint8_t>&& data) {
    if (backpressure_strategy_ != base::constants::BackpressureStrategy::Reliable)
      return try_send_move(std::move(data), true);
    return blocking_send(
        data.size(), [&](auto& tcp, uint64_t sequence) { return tcp.write_move(std::move(data), sequence); },
        [&](auto& channel) { return channel.async_write_move(std::move(data)); },
        [&](auto& connection) { return connection.write_move(std::move(data)); });
  }

  bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (backpressure_strategy_ != base::constants::BackpressureStrategy::Reliable)
      return try_send_shared(std::move(data), true);
    return blocking_send(
        data ? data->size() : 0, [&](auto& tcp, uint64_t sequence) { return tcp.write_shared(data, sequence); },
        [&](auto& channel) { return data && !data->empty() && channel.async_write_shared(data); },
        [&](auto& connection) { return connection.write_shared(data); });
  }

  bool send_line(std::string_view line) {
    if (backpressure_strategy_ == base::constants::BackpressureStrategy::Reliable) return send_line_blocking(line);
    return try_send_line(line, true);
  }

  bool try_send_line(std::string_view line, bool best_effort_send = false) {
    return try_send(std::string(line) + "\n", best_effort_send);
  }

  bool send_blocking(std::string_view data) {
    auto binary_view = base::safe_convert::string_to_bytes(data);
    memory::ConstByteSpan span(binary_view.first, binary_view.second);
    return blocking_send(
        data.size(), [&](auto& tcp, uint64_t sequence) { return tcp.write_copy(span, sequence); },
        [&](auto& channel) { return channel.async_write_copy(span); },
        [&](auto& connection) { return connection.write_copy(span); });
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
    std::weak_ptr<Impl> weak_impl = weak_from_this();
    // Opening the generation and admitting again are the same step, under the
    // gate's lock: a handler of an earlier run can never be admitted into
    // this one.
    const uint64_t generation = callback_gate_.open_new_generation();
    callback_generation_.store(generation);

    channel_->on_bytes([this, generation, weak_impl, weak_alive](memory::ConstByteSpan data) {
      // #450: keep Impl alive for the duration of this callback - on an
      // externally-owned io_context, stop() doesn't join/wait for in-flight
      // handlers, so a bare `this` could otherwise dangle.
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;

      // #449: everything below (data_handler_/data_batch_handler_, and
      // transitively message_handler_/message_batch_handler_ via
      // framer_to_push->push_bytes() below) runs synchronously on this io
      // thread. Mark it so a blocking send() called from within one of
      // these callbacks fails fast instead of deadlocking waiting for this
      // same thread to clear backpressure.
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
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
        detail::invoke_user_callback("tcp_client", "on_data_batch", flush_handler, batch);
      } else {
        detail::invoke_user_callback("tcp_client", "on_data", handler, MessageContext(0, data));
      }

      if (framer_to_push) framer_to_push->push_bytes(data);
    });

    channel_->on_state([this, generation, weak_impl, weak_alive](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
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
        detail::invoke_user_callback("tcp_client", "on_connect", connect_handler, ConnectionContext(0));
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
        if (state == base::LinkState::Closed) {
          detail::invoke_user_callback("tcp_client", "on_disconnect", disconnect_handler, ConnectionContext(0));
        } else if (state == base::LinkState::Error) {
          detail::invoke_user_callback("tcp_client", "on_error", error_handler,
                                       channel_snapshot
                                           ? detail::build_error_context(*channel_snapshot, "Connection error")
                                           : ErrorContext(ErrorCode::IoError, "Connection error"));
        }
      }
    });

    channel_->on_backpressure([this, generation, weak_impl, weak_alive](size_t queued) {
      bp_cv_.notify_all();
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        handler = bp_handler_;
      }
      detail::invoke_user_callback("tcp_client", "on_backpressure", handler, queued);
    });
  }

  void attach_framer_callback() {
    if (!framer_) return;
    // Framed messages are produced inside the on_bytes dispatch, which already
    // holds a lease for the run that is delivering them, so this reads the
    // current generation at call time. Capturing it here instead would pin
    // the generation of whenever the framer happened to be attached - before
    // the first start(), for a framer set on the builder.
    framer_->on_message([this](memory::ConstByteSpan msg) {
      const uint64_t generation = callback_generation_.load();
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
        detail::invoke_user_callback("tcp_client", "on_message_batch", flush_handler, batch);
        return;
      }

      detail::invoke_user_callback("tcp_client", "on_message", handler, MessageContext(0, msg));
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

TcpClient::TcpClient(const std::string& h, uint16_t p) : impl_(std::make_shared<Impl>(h, p)) {}
TcpClient::TcpClient(const std::string& h, uint16_t p, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_shared<Impl>(h, p, ioc)) {}
TcpClient::TcpClient(std::shared_ptr<interface::Channel> ch) : impl_(std::make_shared<Impl>(ch)) {
  impl_->setup_internal_handlers();
}
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
  impl_->data_handler_ = interface::share_callback(std::move(h));
  return *this;
}
TcpClient& TcpClient::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = interface::share_callback(std::move(h));
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
TcpClient& TcpClient::tls(const std::string& ca_file) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->tls_enabled_ = true;
  impl_->tls_ca_file_ = ca_file;
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

size_t TcpClient::backpressure_threshold() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_threshold_;
}

base::constants::BackpressureStrategy TcpClient::backpressure_strategy() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_strategy_;
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

TcpClient& TcpClient::read_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->read_buffer_size_ = bytes;
  return *this;
}

}  // namespace wrapper
}  // namespace wirestead
