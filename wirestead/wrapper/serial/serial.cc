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

#include "wirestead/wrapper/serial/serial.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <cctype>
#include <iostream>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/interface/connection_channel.hpp"
#include "wirestead/transport/serial/detail/write_wait.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/error_context_builder.hpp"
#include "wirestead/wrapper/send_validation.hpp"

namespace wirestead {
namespace wrapper {

namespace {
std::string to_lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
  return s;
}
}  // namespace

struct Serial::Impl : public std::enable_shared_from_this<Impl> {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  // D-1: admission gate for this object's user callbacks. Admission, the
  // running count and the closed flag are one decision, so a stop() cannot
  // observe an empty gate while a callback is about to start, and a callback
  // left over from a previous run is refused after a restart.
  detail::CallbackGate callback_gate_;
  std::mutex stop_finalize_mutex_;
  bool stop_requested_ = false;
  std::atomic<unsigned> stop_callers_{0};
  std::atomic<uint64_t> callback_generation_{0};

  // True when this thread is one the target's shutdown needs: a callback of
  // this object, or any thread currently running the external io_context this
  // channel was built on (which a callback of another channel sharing it is).
  // Such a caller requests the shutdown and returns; it cannot wait for work
  // its own thread has to perform.
  bool shutdown_needs_this_thread() const {
    if (callback_gate_.active_on_this_thread()) return true;
    if (external_ioc && external_ioc->get_executor().running_in_this_thread()) return true;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!channel) return false;
    const auto executor = channel->get_executor();
    using IoExecutor = boost::asio::io_context::executor_type;
    if (auto* io = executor.target<IoExecutor>()) return io->running_in_this_thread();
    if (auto* strand = executor.target<boost::asio::strand<IoExecutor>>())
      return strand->get_inner_executor().running_in_this_thread();
    return false;
  }

  std::string device;
  uint32_t baud_rate;
  std::shared_ptr<interface::Channel> channel;
  std::shared_ptr<boost::asio::io_context> external_ioc;
  std::atomic<bool> use_external_context{false};
  std::atomic<bool> manage_external_context{false};
  std::thread external_thread;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::vector<std::promise<bool>> pending_promises_;
  std::atomic<bool> started_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  // Event handlers (Context based)
  // Shared snapshots: the strand copies one out per received chunk, and a
  // std::function copy allocates whenever the user handler outgrows its
  // small-object buffer. See interface::SharedCallback.
  interface::SharedCallback<MessageHandler> data_handler;
  interface::SharedCallback<BatchMessageHandler> data_batch_handler_;
  ConnectionHandler connect_handler{nullptr};
  ConnectionHandler disconnect_handler{nullptr};
  ErrorHandler error_handler{nullptr};
  std::function<void(size_t)> bp_handler{nullptr};
  interface::SharedCallback<MessageHandler> message_handler;
  interface::SharedCallback<BatchMessageHandler> message_batch_handler_;

  std::shared_ptr<framer::IFramer> framer{nullptr};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  // Configuration
  std::atomic<bool> auto_start_ = false;
  std::atomic<bool> shared_context_{false};
  int data_bits = 8;
  int stop_bits = 1;
  std::string parity = "none";
  std::string flow_control = "none";
  bool reopen_on_error = true;
  size_t read_chunk = base::constants::DEFAULT_READ_BUFFER_SIZE;
  bool low_latency = true;
  config::SerialConfig::Rs485 rs485{};
  std::optional<bool> dtr;
  std::optional<bool> rts;
  std::chrono::milliseconds rx_idle_timeout{0};
  std::chrono::milliseconds retry_interval{base::constants::DEFAULT_RETRY_INTERVAL_MS};
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;

  // False only for the dependency-injected-channel constructor below, where
  // the caller owns the channel's identity and lifecycle (e.g. tests
  // injecting a fake channel) - stop() must not discard and factory-rebuild
  // a channel it didn't create itself.
  bool factory_managed_channel_ = true;

  Impl(const std::string& dev, uint32_t baud) : device(dev), baud_rate(baud) {}
  Impl(const std::string& dev, uint32_t baud, std::shared_ptr<boost::asio::io_context> ioc)
      : device(dev), baud_rate(baud), external_ioc(std::move(ioc)), use_external_context(external_ioc != nullptr) {}
  explicit Impl(std::shared_ptr<interface::Channel> ch) : channel(std::move(ch)), factory_managed_channel_(false) {
    if (!std::dynamic_pointer_cast<transport::Serial>(channel) &&
        !std::dynamic_pointer_cast<interface::ConnectionChannel>(channel))
      throw std::invalid_argument("Serial requires its native transport or a ConnectionChannel");

    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to Serial's own constructor, which
    // runs after impl_ is a fully-formed shared_ptr<Impl> (already called
    // there for this constructor, so nothing to add there).
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void fulfill_all_locked(bool value) {
    for (auto& promise : pending_promises_) {
      try {
        promise.set_value(value);
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
        detail::invoke_user_callback("serial", "on_data_batch", handler, batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = message_batch_handler_;
      auto batch = std::move(message_batch_queue_);
      message_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("serial", "on_message_batch", handler, batch);
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
    stop_requested_ = false;
    if (channel && channel->is_connected()) {
      started_.store(true);
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }

    std::promise<bool> p;
    auto future = p.get_future();
    pending_promises_.emplace_back(std::move(p));

    if (started_.load()) {
      return future;
    }

    if (!alive_marker_) alive_marker_ = std::make_shared<bool>(true);
    if (!channel) {
      channel = factory::ChannelFactory::create(build_config_locked(), external_ioc);
    }
    setup_internal_handlers();
    started_.store(true);

    auto channel_copy = channel;
    lock.unlock();
    channel_copy->start();
    if (use_external_context && manage_external_context && !external_thread.joinable()) {
      if (external_ioc && external_ioc->stopped()) {
        external_ioc->restart();
      }
      work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          boost::asio::make_work_guard(*external_ioc));
      external_thread = std::thread([ioc = external_ioc]() {
        wirestead::concurrency::run_io_thread_init();
        try {
          ioc->run();
        } catch (...) {
        }
      });
    }

    return future;
  }

  void stop() {
    stop_callers_.fetch_add(1);
    struct StopCall {
      std::atomic<unsigned>& callers;
      ~StopCall() { callers.fetch_sub(1); }
    } stop_call{stop_callers_};
    const bool request_only = shutdown_needs_this_thread();
    callback_gate_.close();
    std::shared_ptr<interface::Channel> channel_copy;
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      stop_requested_ = true;
      if (auto serial = std::dynamic_pointer_cast<transport::Serial>(channel)) serial->cancel_write_waits();
      if (auto custom = std::dynamic_pointer_cast<interface::ConnectionChannel>(channel)) custom->cancel_write_waits();
      started_.store(false);

      bp_cv_.notify_all();
      fulfill_all_locked(false);
      channel_copy = channel;
    }
    if (channel_copy) channel_copy->stop();
    if (request_only) return;

    callback_gate_.wait_until_idle();
    std::lock_guard<std::mutex> finalize_lock(stop_finalize_mutex_);
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      alive_marker_.reset();
      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }

      if (channel) {
        channel->on_bytes(nullptr);
        channel->on_state(nullptr);
        channel->on_backpressure(nullptr);
      }
      if (factory_managed_channel_) channel.reset();
      data_batch_queue_.clear();
      message_batch_queue_.clear();
      if (framer) framer->reset();
    }
    if (use_external_context && manage_external_context) {
      work_guard_.reset();
      if (external_ioc) external_ioc->stop();
      if (external_thread.joinable()) external_thread.join();
    }
  }

  // Caller holds mutex_. Native readiness remains part of transport admission.
  SendResult send_state(const std::shared_ptr<transport::Serial>& serial, bool custom = false) {
    if (stop_callers_.load() != 0) return SendResult::reject(SendRejection::Stopping);
    if (!started_.load()) {
      if (stop_requested_) {
        if (!callback_gate_.idle()) return SendResult::reject(SendRejection::Stopping);
        if (serial) {
          const auto state = serial->write_state();
          if (!state.accepted() && state.reason() == SendRejection::Stopping) return state;
        }
      }
      return SendResult::reject(SendRejection::NotStarted);
    }
    if (!serial && !custom) return SendResult::reject(SendRejection::NotReady);
    return SendResult::accept();
  }

  static SendResult finish_send(SendResult result) {
    if (auto hook = detail::g_serial_send_result_hook.load()) hook(result);
    return result;
  }

  template <typename NativeWrite, typename CustomWrite>
  SendResult nonblocking_send(size_t size, bool best_effort_send, NativeWrite native_write, CustomWrite custom_write) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto serial = std::dynamic_pointer_cast<transport::Serial>(channel);
    auto custom = serial ? nullptr : std::dynamic_pointer_cast<interface::ConnectionChannel>(channel);
    const auto result = [&]() -> SendResult {
      auto validation = detail::validate_payload_size(size, channel ? channel->write_queue_limit() : std::nullopt);
      if (!validation.accepted()) return validation;
      const auto state = send_state(serial, custom != nullptr);
      if (!state.accepted()) return state;
      // Admission rechecks state and capacity together under the channel lock.
      auto admitted = custom ? custom_write(*custom) : native_write(*serial);
      if (best_effort_send && !admitted.accepted() && admitted.reason() == SendRejection::WouldBlock)
        return SendResult::reject(SendRejection::QueueFull);
      return admitted;
    }();
    lock.unlock();
    return finish_send(result);
  }

  SendResult try_send(std::string_view data, bool best_effort_send = false) {
    auto binary_view = base::safe_convert::string_to_bytes(data);
    memory::ConstByteSpan span(binary_view.first, binary_view.second);
    return nonblocking_send(
        data.size(), best_effort_send, [&](auto& serial) { return serial.try_write_copy(span); },
        [&](auto& channel) { return channel.async_try_write_copy_result(span); });
  }

  SendResult try_send_move(std::vector<uint8_t>&& data, bool best_effort_send = false) {
    return nonblocking_send(
        data.size(), best_effort_send, [&](auto& serial) { return serial.try_write_move(std::move(data)); },
        [&](auto& channel) { return channel.async_try_write_move_result(std::move(data)); });
  }

  SendResult try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data, bool best_effort_send = false) {
    return nonblocking_send(
        data ? data->size() : 0, best_effort_send,
        [&](auto& serial) { return serial.try_write_shared(std::move(data)); },
        [&](auto& channel) { return channel.async_try_write_shared_result(std::move(data)); });
  }

  SendResult send(std::string_view data) {
    if (backpressure_strategy == base::constants::BackpressureStrategy::Reliable) return send_blocking(data);
    return try_send(data, true);
  }

  struct ConnectionPin {
    std::shared_ptr<interface::ConnectionChannel> custom;
    interface::ConnectionChannel::Connection custom_wait;
    std::shared_ptr<transport::Serial> serial;
    std::shared_ptr<transport::detail::SerialWriteWait> wait;
  };

  bool connection_matches(const ConnectionPin& pin) const {
    return !pin.serial || (pin.wait && pin.serial->write_connection() == pin.wait->sequence);
  }

  // channel->on_backpressure() calls bp_cv_.notify_all() from the transport's io_context
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
      if (auto hook = detail::g_serial_capacity_wait_hook.load()) hook();
      while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), [&] {
        outcome = connection.custom_wait->poll_capacity();
        return outcome.has_value();
      })) {
      }
      if (auto hook = detail::g_serial_capacity_wait_result_hook.load()) hook(*outcome);
      return *outcome;
    }
    if (!detail::payload_needs_capacity(payload_size)) return SendResult::accept();
    auto immediate = [this, payload_size, generation, &connection] {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      return callback_generation_.load() != generation || !started_.load() || !channel || !channel->is_connected() ||
             !connection_matches(connection) ||
             !detail::payload_needs_capacity(payload_size, channel->write_queue_limit()) ||
             !channel->is_backpressure_active();
    };
    // A bypass is not a completed capacity wait; final admission checks still apply.
    if (immediate()) return SendResult::accept();
    if (detail::in_data_callback()) return SendResult::reject(SendRejection::WouldBlock);
    if (auto hook = detail::g_serial_capacity_wait_hook.load()) hook();
    std::optional<SendResult> outcome;
    auto released = [&] {
      if (outcome) return true;
      std::shared_lock<std::shared_mutex> lock(mutex_);
      // The connection record retains the first terminal cause under the
      // same transport lock used by stop, loss and admission.
      outcome = connection.serial->poll_write_wait(connection.wait);
      return outcome.has_value();
    };
    while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), released)) {
    }
    if (auto hook = detail::g_serial_capacity_wait_result_hook.load()) hook(*outcome);
    return *outcome;
  }

  // #509: high-water pressure and hard-limit reservations are different
  // thresholds. Capacity can be refilled between a wait and admission, so
  // retry transient native WouldBlock at most five times. Validation and
  // terminal state failures return immediately.
  static constexpr int kMaxBlockingSendAttempts = 5;

  template <typename NativeWrite, typename CustomWrite>
  SendResult blocking_send(size_t size, NativeWrite native_write, CustomWrite custom_write) {
    uint64_t generation;
    ConnectionPin connection;
    const auto result = [&]() -> SendResult {
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        // Select the run and native connection together at entry. Validation
        // precedes state and waiting, including the line delimiter and hard cap.
        generation = callback_generation_.load();
        connection.serial = std::dynamic_pointer_cast<transport::Serial>(channel);
        connection.custom =
            connection.serial ? nullptr : std::dynamic_pointer_cast<interface::ConnectionChannel>(channel);
        auto validation = detail::validate_payload_size(size, channel ? channel->write_queue_limit() : std::nullopt);
        if (!validation.accepted()) return validation;
        auto state = send_state(connection.serial, connection.custom != nullptr);
        if (!state.accepted()) return state;
        if (connection.custom) {
          auto captured = connection.custom->capture_write_connection();
          if (auto reason = std::get_if<SendRejection>(&captured)) return SendResult::reject(*reason);
          connection.custom_wait = std::get<interface::ConnectionChannel::Connection>(std::move(captured));
          if (!connection.custom_wait) throw std::logic_error("ConnectionChannel returned a null connection");
        } else {
          state = connection.serial->write_state();
          if (!state.accepted()) return state;
          connection.wait = connection.serial->capture_write_wait();
          if (!connection.wait) return SendResult::reject(SendRejection::NotReady);
        }
      }
      for (int attempt = 0; attempt < kMaxBlockingSendAttempts; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        const auto released = wait_for_backpressure_clear(bp_lock, size, generation, connection);
        if (!released.accepted()) return released;  // Never overwrite the cause of release.
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto state = send_state(connection.serial, connection.custom != nullptr);
        if (!state.accepted()) return state;
        if (callback_generation_.load() != generation) return SendResult::reject(SendRejection::NotReady);
        const auto admitted = connection.custom_wait ? custom_write(*connection.custom_wait)
                                                     : native_write(*connection.serial, connection.wait->sequence);
        if (admitted.accepted() || admitted.reason() != SendRejection::WouldBlock) return admitted;
        // Only transient capacity refusal can be retried, never a terminal
        // result. Callback callers must return without entering another wait.
        if (detail::in_data_callback()) return admitted;
      }
      return SendResult::reject(SendRejection::WouldBlock);
    }();
    return finish_send(result);
  }

  SendResult send_move(std::vector<uint8_t>&& data) {
    if (backpressure_strategy != base::constants::BackpressureStrategy::Reliable)
      return try_send_move(std::move(data), true);
    return blocking_send(
        data.size(), [&](auto& serial, uint64_t sequence) { return serial.write_move(std::move(data), sequence); },
        [&](auto& connection) { return connection.write_move(std::move(data)); });
  }

  SendResult send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
    if (backpressure_strategy != base::constants::BackpressureStrategy::Reliable)
      return try_send_shared(std::move(data), true);
    return blocking_send(
        data ? data->size() : 0, [&](auto& serial, uint64_t sequence) { return serial.write_shared(data, sequence); },
        [&](auto& connection) { return connection.write_shared(data); });
  }

  SendResult send_line(std::string_view line) {
    if (backpressure_strategy == base::constants::BackpressureStrategy::Reliable) return send_line_blocking(line);
    return try_send_line(line, true);
  }

  SendResult try_send_line(std::string_view line, bool best_effort_send = false) {
    return try_send(std::string(line) + "\n", best_effort_send);
  }

  SendResult send_blocking(std::string_view data) {
    auto binary_view = base::safe_convert::string_to_bytes(data);
    memory::ConstByteSpan span(binary_view.first, binary_view.second);
    return blocking_send(
        data.size(), [&](auto& serial, uint64_t sequence) { return serial.write_copy(span, sequence); },
        [&](auto& connection) { return connection.write_copy(span); });
  }

  SendResult send_line_blocking(std::string_view line) { return send_blocking(std::string(line) + "\n"); }

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

    std::weak_ptr<bool> weak_alive = alive_marker_;
    std::weak_ptr<Impl> weak_impl = weak_from_this();
    const auto generation = callback_gate_.open_new_generation();
    callback_generation_.store(generation);

    channel->on_bytes([this, generation, weak_impl, weak_alive](memory::ConstByteSpan data) {
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
        handler = data_handler;
        framer_to_push = framer;
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
        detail::invoke_user_callback("serial", "on_data_batch", flush_handler, batch);
      } else {
        detail::invoke_user_callback("serial", "on_data", handler, MessageContext(0, data));
      }

      if (framer_to_push) framer_to_push->push_bytes(data);
    });

    channel->on_backpressure([this, generation, weak_impl, weak_alive](size_t queued) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
      bp_cv_.notify_all();
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        handler = bp_handler;
      }
      detail::invoke_user_callback("serial", "on_backpressure", handler, queued);
    });

    channel->on_state([this, generation, weak_impl, weak_alive](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;

      switch (state) {
        case base::LinkState::Connected: {
          ConnectionHandler handler;
          {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            fulfill_all_locked(true);
            handler = connect_handler;
          }
          detail::invoke_user_callback("serial", "on_connect", handler, ConnectionContext(0));
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
          detail::invoke_user_callback("serial", "on_disconnect", disconnect_handler_snapshot, ConnectionContext(0));
          detail::invoke_user_callback("serial", "on_error", error_handler_snapshot,
                                       channel ? detail::build_error_context(*channel, "Connection error")
                                               : ErrorContext(ErrorCode::IoError, "Connection error"));
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
      const auto generation = callback_generation_.load();
      // #441: snapshot under a shared_lock (pure read), build the copy
      // before taking the exclusive lock for queue mutation.
      bool batch_mode;
      interface::SharedCallback<MessageHandler> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        batch_mode = static_cast<bool>(message_batch_handler_);
        handler = message_handler;
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
        detail::invoke_user_callback("serial", "on_message_batch", flush_handler, batch);
        return;
      }

      detail::invoke_user_callback("serial", "on_message", handler, MessageContext(0, msg));
    });
  }

  void set_framer(std::unique_ptr<framer::IFramer> f) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    framer = std::shared_ptr<framer::IFramer>(std::move(f));
    if (framer && (message_handler || message_batch_handler_)) attach_framer_callback();
  }

  void on_message(MessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_handler = interface::share_callback(std::move(handler));
    if (framer) attach_framer_callback();
  }

  void on_message_batch(BatchMessageHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    message_batch_handler_ = interface::share_callback(std::move(handler));
    if (framer) attach_framer_callback();
  }

  config::SerialConfig build_config_locked() const {
    config::SerialConfig config;
    config.device = device;
    config.baud_rate = baud_rate;
    config.char_size = static_cast<unsigned int>(data_bits);
    config.stop_bits = static_cast<unsigned int>(stop_bits);
    std::string p = to_lower(parity);
    if (p == "even")
      config.parity = config::SerialConfig::Parity::Even;
    else if (p == "odd")
      config.parity = config::SerialConfig::Parity::Odd;
    else
      config.parity = config::SerialConfig::Parity::None;

    std::string f = to_lower(flow_control);
    if (f == "software")
      config.flow = config::SerialConfig::Flow::Software;
    else if (f == "hardware")
      config.flow = config::SerialConfig::Flow::Hardware;
    else
      config.flow = config::SerialConfig::Flow::None;

    config.retry_interval_ms = static_cast<unsigned int>(retry_interval.count());
    config.reopen_on_error = reopen_on_error;
    config.read_chunk = read_chunk;
    config.low_latency = low_latency;
    config.rs485 = rs485;
    config.dtr = dtr;
    config.rts = rts;
    config.rx_idle_timeout_ms = static_cast<unsigned>(rx_idle_timeout.count());
    config.backpressure_threshold = backpressure_threshold;
    config.backpressure_strategy = backpressure_strategy;
    config.use_shared_context = shared_context_.load();
    return config;
  }
};

Serial::Serial(const std::string& d, uint32_t b) : impl_(std::make_shared<Impl>(d, b)) {}
Serial::Serial(const std::string& d, uint32_t b, std::shared_ptr<boost::asio::io_context> i)
    : impl_(std::make_shared<Impl>(d, b, i)) {}
Serial::Serial(std::shared_ptr<interface::Channel> ch) : impl_(std::make_shared<Impl>(ch)) {
  impl_->setup_internal_handlers();
}
Serial::~Serial() = default;

Serial::Serial(Serial&&) noexcept = default;
Serial& Serial::operator=(Serial&&) noexcept = default;

std::future<bool> Serial::start() { return impl_->start(); }
void Serial::stop() { impl_->stop(); }
SendResult Serial::send(std::string_view data) { return impl_->send(data); }
SendResult Serial::try_send(std::string_view data) { return impl_->try_send(data); }
SendResult Serial::send_line(std::string_view line) { return impl_->send_line(line); }
SendResult Serial::try_send_line(std::string_view line) { return impl_->try_send_line(line); }
SendResult Serial::send_move(std::vector<uint8_t>&& data) { return impl_->send_move(std::move(data)); }
SendResult Serial::try_send_move(std::vector<uint8_t>&& data) { return impl_->try_send_move(std::move(data)); }
SendResult Serial::send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->send_shared(std::move(data));
}
SendResult Serial::try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  return impl_->try_send_shared(std::move(data));
}
SendResult Serial::send_blocking(std::string_view data) { return impl_->send_blocking(data); }
SendResult Serial::send_line_blocking(std::string_view line) { return impl_->send_line_blocking(line); }
bool Serial::connected() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->channel && impl_->channel->is_connected();
}
RuntimeStats Serial::stats() const { return impl_->stats(); }
void Serial::reset_stats() { impl_->reset_stats(); }

Serial& Serial::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_handler = interface::share_callback(std::move(h));
  return *this;
}
Serial& Serial::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = interface::share_callback(std::move(h));
  return *this;
}
Serial& Serial::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->connect_handler = std::move(h);
  return *this;
}
Serial& Serial::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->disconnect_handler = std::move(h);
  return *this;
}
Serial& Serial::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->error_handler = std::move(h);
  return *this;
}

Serial& Serial::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->bp_handler = std::move(h);
  return *this;
}

Serial& Serial::framer(std::unique_ptr<framer::IFramer> f) {
  impl_->set_framer(std::move(f));
  return *this;
}
Serial& Serial::on_message(MessageHandler h) {
  impl_->on_message(std::move(h));
  return *this;
}
Serial& Serial::on_message_batch(BatchMessageHandler h) {
  impl_->on_message_batch(std::move(h));
  return *this;
}

Serial& Serial::auto_start(bool m) {
  impl_->auto_start_.store(m);
  if (impl_->auto_start_.load() && !impl_->started_.load()) start();
  return *this;
}

Serial& Serial::shared_context(bool use_shared) {
  impl_->shared_context_.store(use_shared);
  return *this;
}

Serial& Serial::baud_rate(uint32_t b) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->baud_rate = b;
  return *this;
}
Serial& Serial::data_bits(int d) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_bits = d;
  return *this;
}
Serial& Serial::stop_bits(int s) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->stop_bits = s;
  return *this;
}
Serial& Serial::parity(const std::string& p) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->parity = p;
  return *this;
}
Serial& Serial::flow_control(const std::string& f) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->flow_control = f;
  return *this;
}
Serial& Serial::read_chunk(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->read_chunk = bytes;
  return *this;
}

Serial& Serial::low_latency(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->low_latency = enable;
  return *this;
}

Serial& Serial::rs485(bool rts_on_send, bool rx_during_tx, unsigned delay_before_ms, unsigned delay_after_ms) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->rs485.enabled = true;
  impl_->rs485.rts_on_send = rts_on_send;
  impl_->rs485.rx_during_tx = rx_during_tx;
  impl_->rs485.delay_rts_before_send_ms = delay_before_ms;
  impl_->rs485.delay_rts_after_send_ms = delay_after_ms;
  return *this;
}

Serial& Serial::dtr(bool assert_line) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->dtr = assert_line;
  return *this;
}

Serial& Serial::rts(bool assert_line) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->rts = assert_line;
  return *this;
}

Serial& Serial::rx_idle_timeout(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->rx_idle_timeout = timeout;
  return *this;
}

Serial& Serial::reopen_on_error(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->reopen_on_error = enable;
  return *this;
}
Serial& Serial::retry_interval(std::chrono::milliseconds i) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->retry_interval = i;
  if (impl_->channel) {
    auto ts = std::dynamic_pointer_cast<transport::Serial>(impl_->channel);
    if (ts) ts->set_retry_interval(static_cast<unsigned int>(i.count()));
  }
  return *this;
}

Serial& Serial::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_threshold = threshold;
  return *this;
}

Serial& Serial::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->backpressure_strategy = strategy;
  if (impl_->channel) {
    auto ts = std::dynamic_pointer_cast<transport::Serial>(impl_->channel);
    if (ts) ts->set_backpressure_strategy(strategy);
  }
  return *this;
}

size_t Serial::backpressure_threshold() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_threshold;
}

base::constants::BackpressureStrategy Serial::backpressure_strategy() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->backpressure_strategy;
}

config::SerialConfig Serial::build_config() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->build_config_locked();
}

Serial& Serial::manage_external_context(bool m) {
  impl_->manage_external_context.store(m);
  return *this;
}

Serial& Serial::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

Serial& Serial::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace wirestead
