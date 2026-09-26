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

#include "wirestead/transport/serial/serial.hpp"

#include <spdlog/fmt/fmt.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/diagnostics/send_accounting.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/base/bp_state_machine.hpp"
#include "wirestead/transport/base/bp_utils.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/serial/boost_serial_port.hpp"
#include "wirestead/transport/serial/detail/write_wait.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using base::LinkState;
using concurrency::AtomicLinkState;

using BufferVariant =
    std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;

struct Serial::Impl {
  std::atomic<bool> started_{false};
  std::atomic<bool> stopping_{false};
  std::unique_ptr<net::io_context> owned_ioc_;
  net::io_context& ioc_;
  bool owns_ioc_{false};
  bool uses_shared_context_{false};
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;

  std::unique_ptr<interface::SerialPortInterface> port_;
  config::SerialConfig cfg_;
  // #443: per-channel pool instead of the process-wide GlobalMemoryPool
  // singleton - avoids cross-channel contention on the singleton's bucket
  // mutexes. Capacity is much smaller than the old shared default since
  // it's no longer amortized across every channel in the process.
  // Prefill stays 0. This literal was written while MemoryPool discarded
  // initial_pool_size, so 50 allocated nothing; #575 made the parameter real
  // and turned it into ~1 MiB eagerly allocated per channel at construction.
  // The pool fills as buffers are released.
  memory::MemoryPool pool_{0, 200};
  net::steady_timer retry_timer_;
  // Watchdog for cfg_.rx_idle_timeout_ms: armed on connect, pushed back by
  // every read that carries bytes, so it only ever fires on silence.
  net::steady_timer rx_idle_timer_;

  std::shared_ptr<std::vector<uint8_t>> rx_;
  using Ledger = diagnostics::SendAccountingLedger;
  struct TrackedBuffer {
    BufferVariant buffer;
    Ledger::Request request;
  };
  struct BufferProjection {
    const BufferVariant& operator()(const TrackedBuffer& item) const { return item.buffer; }
  };
  Ledger send_accounting_;
  std::deque<TrackedBuffer> tx_;
  std::deque<TrackedBuffer> pending_;
  std::atomic<size_t> pending_bytes_{0};
  // Completions retain their own buffers across device reopen.
  struct WriteBatch {
    std::vector<TrackedBuffer> buffers;
    std::vector<net::const_buffer> views;
    size_t bytes = 0;
  };
  std::shared_ptr<WriteBatch> active_write_;
  bool writing_ = false;
  std::atomic<size_t> queued_bytes_{0};
  // Bytes accepted by a plain async_write_* call but not yet routed onto the
  // strand - reserved via try_reserve_limit_bytes() to close the
  // accept-then-drop race (jwsung91/wirestead#517). inflight_bytes_ mutations
  // and the queued_bytes_/pending_bytes_ increments that promote a
  // reservation both go through write_reserve_mtx_ - see bp_utils.hpp.
  std::atomic<size_t> inflight_bytes_{0};
  std::mutex write_reserve_mtx_;
  std::mutex stop_mtx_;
  std::condition_variable stop_cv_;
  bool cleanup_done_ = false;
  bool cleanup_started_ = false;
  bool cleanup_finished_ = false;
  size_t pending_io_ = 0;  // Strand-confined, including handler destruction.
  std::mutex join_mtx_;
  std::mutex submission_mtx_;
  std::atomic<uint64_t> generation_{0};
  std::atomic<uint64_t> connection_seq_{0};
  std::shared_ptr<detail::SerialWriteWait> write_wait_;

  // Caller holds submission_mtx_: state, connection and admission are one decision.
  std::optional<wrapper::SendRejection> admission_rejection(
      std::optional<uint64_t> expected_connection = std::nullopt) {
    if (stopping_.load()) {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      return cleanup_done_ ? wrapper::SendRejection::NotStarted : wrapper::SendRejection::Stopping;
    }
    if (generation_.load() == 0) return wrapper::SendRejection::NotStarted;
    if ((expected_connection && *expected_connection != connection_seq_.load()) || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error) || !opened_.load()) {
      return wrapper::SendRejection::NotReady;
    }
    return std::nullopt;
  }

  void mark_cleanup_done() {
    // A discarded fake completion can also release the last tracked operation.
    // Only now is it safe to release the gather-write storage.
    active_write_.reset();
    detail::stop_test_hook(this, true);
    work_guard_.reset();
    started_ = false;
    {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      cleanup_done_ = true;
    }
    stop_cv_.notify_all();
  }

  // The serial interface erases executor associations into std::function.
  // Explicit dispatch preserves serialization. The lifetime also accounts for
  // test ports that discard an operation instead of invoking its handler.
  template <typename Handler>
  auto track_io(std::shared_ptr<Serial> self, Handler handler, bool defer = false) {
    ++pending_io_;
    std::shared_ptr<void> lifetime(nullptr, [self](void*) {
      net::dispatch(self->impl_->strand_, [self] {
        auto* impl = self->impl_.get();
        if (impl->cleanup_finished_ && impl->pending_io_ == 1) {
          if (auto hook = detail::g_serial_io_completion_hook.load()) hook();
        }
        if (--impl->pending_io_ == 0 && impl->cleanup_finished_) impl->mark_cleanup_done();
      });
    });
    return [self, lifetime, handler = std::move(handler), defer](auto... args) {
      auto completion = [lifetime, handler, args...]() mutable { handler(args...); };
      // Write initiation holds submission_mtx_; injected interfaces may invoke
      // the callback inline. Queue it before acquiring that mutex again.
      if (defer)
        net::post(self->impl_->strand_, std::move(completion));
      else
        net::dispatch(self->impl_->strand_, std::move(completion));
    };
  }

  // Atomic rather than mutex-guarded: read both from the strand and from
  // arbitrary caller threads (async_try_write_* fast-fail prechecks) - a
  // strand-post/dispatch here would only protect the former (#436).
  std::atomic<base::constants::BackpressureStrategy> bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  // Mirrors cfg_.retry_interval_ms but is the one actually read by
  // schedule_retry(); set_retry_interval() writes only this atomic rather
  // than mutating cfg_ directly, so the read/write pair for this specific
  // field doesn't need a mutex (#436).
  std::atomic<unsigned> retry_interval_ms_;
  size_t bp_high_;
  size_t bp_limit_;
  size_t bp_low_;
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;

  // Guards on_bytes_/on_state_/on_bp_. Setters (called from any user thread)
  // and the strand-confined read sites both take this lock; readers copy
  // the callback under lock then invoke the copy outside the lock (#436).
  mutable std::mutex callback_mtx_;
  // Shared snapshots: the strand copies one out per received chunk, and a
  // std::function copy allocates whenever the target outgrows its small-object
  // buffer. See interface::SharedCallback.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;

  std::atomic<bool> opened_{false};
  AtomicLinkState state_{LinkState::Idle};

  ErrorInfoHolder error_info_holder_{"serial"};

  void observe_queue() {
    stats_.observe_queue(queued_bytes_.load(std::memory_order_relaxed) +
                         pending_bytes_.load(std::memory_order_relaxed));
  }

  queue_util::BackpressureFields bp_fields() {
    return queue_util::BackpressureFields{queued_bytes_,
                                          pending_bytes_,
                                          backpressure_active_,
                                          bp_high_,
                                          bp_low_,
                                          bp_limit_,
                                          bp_strategy_.load(std::memory_order_relaxed),
                                          &write_reserve_mtx_};
  }

  // Shared decide_enqueue()/route dispatch used by all 4 async_write_* call
  // sites (async_write_copy's pool+fallback paths, async_write_move,
  // async_write_shared) (#434). Unlike every other transport, a rejection
  // caused by the tx_/BestEffort-trim overflow check is treated as a FATAL
  // transport error here rather than reject-and-continue - this is a
  // pre-existing, deliberate Serial-specific behavior the design plan
  // flagged as needing a product decision, not something to silently unify
  // away. A rejection caused by the separate Reliable-pending_-overflow
  // check (queue+pending+added > bp_limit_ while backpressure is already
  // active) still just drops and continues, matching everyone else -
  // distinguished here since decide_enqueue()'s Rejected result doesn't
  // otherwise tell the two apart.
  //
  // Also normalizes an internal inconsistency found while migrating: only
  // async_write_copy's *fallback* path (and async_write_move/_shared)
  // triggered the fatal-error behavior on tx_ overflow before this change -
  // the *pooled*-buffer path in async_write_copy did not, for no documented
  // reason. All 4 call sites now behave identically.
  void route_enqueued_buffer(std::shared_ptr<Serial> self, TrackedBuffer&& buf, size_t added) {
    if (stopping_) {
      queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
      return;
    }
    const bool reliable_pending_active =
        bp_strategy_.load(std::memory_order_relaxed) == base::constants::BackpressureStrategy::Reliable &&
        backpressure_active_.load(std::memory_order_relaxed);

    auto f = bp_fields();
    queue_util::DropAccounting dropped;
    auto decision = queue_util::decide_enqueue(
        f, added, tx_, dropped, BufferProjection{},
        [this](const TrackedBuffer& item) { send_accounting_.discard(item.request, Ledger::Cause::QueuePressure); });
    if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);

    if (decision == queue_util::EnqueueDecision::Rejected) {
      send_accounting_.discard(buf.request, Ledger::Cause::QueuePressure);
      WIRESTEAD_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
      // #448: record as dropped so it's reflected in RuntimeStats instead of
      // silently vanishing after being counted as accepted.
      stats_.record_dropped(1, added);
      queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
      if (reliable_pending_active) {
        return;
      }
      report_backpressure(queued_bytes_ + added);
      mark_disconnected();
      discard_connection_writes();
      state_.set(LinkState::Error);
      notify_state();
      handle_error(self, "write_queue_overflow", make_error_code(boost::system::errc::no_buffer_space));
      return;
    }
    if (decision == queue_util::EnqueueDecision::Pending) {
      queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, pending_bytes_, inflight_bytes_, added);
      pending_.emplace_back(std::move(buf));
      observe_queue();
      return;
    }
    queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, queued_bytes_, inflight_bytes_, added);
    tx_.emplace_back(std::move(buf));
    observe_queue();
    report_backpressure(queued_bytes_);
    if (!writing_) do_write(self);
  }

  explicit Impl(const config::SerialConfig& cfg, bool use_shared_context)
      : owned_ioc_(use_shared_context ? nullptr : std::make_unique<net::io_context>()),
        ioc_(use_shared_context ? concurrency::IoContextManager::instance().get_context() : *owned_ioc_),
        owns_ioc_(!use_shared_context),
        uses_shared_context_(use_shared_context),
        strand_(ioc_.get_executor()),
        cfg_(cfg),
        retry_timer_(ioc_),
        rx_idle_timer_(ioc_),
        bp_strategy_(cfg.backpressure_strategy),
        retry_interval_ms_(cfg.retry_interval_ms),
        bp_high_(cfg.backpressure_threshold) {
    init();
    port_ = std::make_unique<BoostSerialPort>(ioc_);
  }

  Impl(const config::SerialConfig& cfg, std::unique_ptr<interface::SerialPortInterface> port, net::io_context& ioc)
      : ioc_(ioc),
        owns_ioc_(false),
        strand_(ioc.get_executor()),
        port_(std::move(port)),
        cfg_(cfg),
        retry_timer_(ioc),
        rx_idle_timer_(ioc),
        bp_strategy_(cfg.backpressure_strategy),
        retry_interval_ms_(cfg.retry_interval_ms),
        bp_high_(cfg.backpressure_threshold) {
    init();
  }

  void mark_disconnected() {
    std::lock_guard<std::mutex> lock(submission_mtx_);
    mark_disconnected_locked();
  }
  void mark_disconnected_locked() {
    if (opened_.exchange(false)) {
      send_accounting_.end(Ledger::Cause::ConnectionLoss);
      if (write_wait_) write_wait_->end(wrapper::SendRejection::NotReady);
      connection_seq_.fetch_add(1);
    }
  }

  void discard_connection_writes() {
    size_t messages = tx_.size() + pending_.size();
    size_t bytes = 0;
    for (const auto& buffer : tx_)
      bytes += std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, buffer.buffer);
    for (const auto& buffer : pending_)
      bytes += std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, buffer.buffer);
    if (active_write_) {
      messages += active_write_->buffers.size();
      bytes += active_write_->bytes;
      // Its completion handler retains the buffers; it must not mutate new state.
      active_write_.reset();
    }
    tx_.clear();
    pending_.clear();
    queued_bytes_ = 0;
    pending_bytes_ = 0;
    writing_ = false;
    backpressure_active_ = false;
    if (messages) stats_.record_dropped(messages, bytes);
  }

  void init() {
    cfg_.validate_and_clamp();
    bp_high_ = cfg_.backpressure_threshold;
    bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                         base::constants::MAX_BUFFER_SIZE);
    bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
    if (bp_low_ == 0) bp_low_ = 1;
    rx_ = std::make_shared<std::vector<uint8_t>>(cfg_.read_chunk);
  }

  ~Impl() {
    try {
      stopping_.store(true);
      if (ioc_thread_.joinable()) {
        if (std::this_thread::get_id() == ioc_thread_.get_id()) {
          ioc_thread_.detach();
        } else {
          ioc_thread_.request_stop();
          ioc_thread_.join();
        }
      }
      perform_cleanup();
    } catch (...) {
    }
  }

  void open_and_configure(std::shared_ptr<Serial> self) {
    if (stopping_) return;
    boost::system::error_code ec;
    port_->open(cfg_.device, ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "open", fmt::format("Failed to open device: {} - {}", cfg_.device, ec.message()));
      handle_error(self, "open", ec);
      return;
    }

    port_->set_option(net::serial_port_base::baud_rate(cfg_.baud_rate), ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "configure", fmt::format("Failed baud rate: {}", ec.message()));
      handle_error(self, "baud_rate", ec);
      return;
    }

    port_->set_option(net::serial_port_base::character_size(cfg_.char_size), ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "configure", fmt::format("Failed char size: {}", ec.message()));
      handle_error(self, "char_size", ec);
      return;
    }

    using sb = net::serial_port_base::stop_bits;
    port_->set_option(sb(cfg_.stop_bits == 2 ? sb::two : sb::one), ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "configure", fmt::format("Failed stop bits: {}", ec.message()));
      handle_error(self, "stop_bits", ec);
      return;
    }

    using pa = net::serial_port_base::parity;
    pa::type p = pa::none;
    if (cfg_.parity == config::SerialConfig::Parity::Even)
      p = pa::even;
    else if (cfg_.parity == config::SerialConfig::Parity::Odd)
      p = pa::odd;
    port_->set_option(pa(p), ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "configure", fmt::format("Failed parity: {}", ec.message()));
      handle_error(self, "parity", ec);
      return;
    }

    using fc = net::serial_port_base::flow_control;
    fc::type f = fc::none;
    if (cfg_.flow == config::SerialConfig::Flow::Software)
      f = fc::software;
    else if (cfg_.flow == config::SerialConfig::Flow::Hardware)
      f = fc::hardware;
    port_->set_option(fc(f), ec);
    if (ec) {
      WIRESTEAD_LOG_ERROR("serial", "configure", fmt::format("Failed flow control: {}", ec.message()));
      handle_error(self, "flow_control", ec);
      return;
    }

    // RS-485 before the first read and before anything is written: the very
    // first frame out has to be sent with the direction pin already under the
    // driver's control, or it goes out with the transceiver still in receive.
    //
    // Unlike low_latency, a refusal here is worth a warning rather than a
    // debug line. Asking for RS-485 and silently running in plain UART mode
    // means every write collides on a shared bus, which presents as garbage
    // from the device rather than as a configuration problem.
    if (cfg_.rs485.enabled &&
        !port_->set_rs485(cfg_.rs485.rts_on_send, cfg_.rs485.rx_during_tx, cfg_.rs485.delay_rts_before_send_ms,
                          cfg_.rs485.delay_rts_after_send_ms)) {
      WIRESTEAD_LOG_WARNING("serial", "configure",
                            fmt::format("RS-485 mode was requested but {} does not support it; the adapter must switch "
                                        "direction in hardware or the bus will collide",
                                        cfg_.device));
    }

    if ((cfg_.dtr || cfg_.rts) && !port_->set_modem_lines(cfg_.dtr, cfg_.rts)) {
      WIRESTEAD_LOG_WARNING("serial", "configure", fmt::format("Could not set DTR/RTS on {}", cfg_.device));
    }

    // Best effort, after the line settings and before the first read: a driver
    // without a latency timer just says no, and the port is fine either way.
    if (cfg_.low_latency && !port_->set_low_latency()) {
      WIRESTEAD_LOG_DEBUG("serial", "configure",
                          fmt::format("Low-latency mode unavailable on {}, using the driver default", cfg_.device));
    }

    WIRESTEAD_LOG_INFO("serial", "connect", fmt::format("Device opened: {}", cfg_.device));
    {
      std::lock_guard<std::mutex> lock(submission_mtx_);
      if (stopping_) return;
      const auto connection = connection_seq_.fetch_add(1) + 1;
      write_wait_ = std::make_shared<detail::SerialWriteWait>(connection);
      opened_.store(true);
      state_.set(LinkState::Connected);
    }
    rx_ = std::make_shared<std::vector<uint8_t>>(cfg_.read_chunk);
    start_read(self);
    reset_rx_idle_timer(self);
    notify_state();
    do_write(self);
  }

  void start_read(std::shared_ptr<Serial> self) {
    if (stopping_ || !opened_) return;
    const auto connection = connection_seq_.load();
    auto buffer = rx_;
    port_->async_read_some(
        net::buffer(*buffer), track_io(self, [self, connection, buffer](auto ec, std::size_t n) {
          auto impl = self->get_impl();
          if (impl->stopping_ || connection != impl->connection_seq_) return;
          if (ec) {
            impl->handle_error(self, "read", ec);
            return;
          }
          if (n > 0) {
            impl->stats_.record_received(n);
            impl->reset_rx_idle_timer(self);
          }
          interface::SharedCallback<OnBytes> on_bytes;
          {
            std::lock_guard<std::mutex> lock(impl->callback_mtx_);
            on_bytes = impl->on_bytes_;
          }
          if (on_bytes) {
            try {
              (*on_bytes)(memory::ConstByteSpan(buffer->data(), n));
            } catch (const std::exception& e) {
              std::string msg = fmt::format("Exception in callback: {}", e.what());
              WIRESTEAD_LOG_ERROR("serial", "on_bytes", msg);
              if (impl->cfg_.stop_on_callback_exception) {
                impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR,
                                                      diagnostics::ErrorCategory::COMMUNICATION, "on_bytes", {}, msg,
                                                      false, 0);
                impl->mark_disconnected();
                impl->discard_connection_writes();
                impl->close_port();
                impl->state_.set(LinkState::Error);
                impl->notify_state();
                return;
              }
              impl->handle_error(self, "on_bytes_callback", make_error_code(boost::system::errc::io_error));
              return;
            } catch (...) {
              if (impl->cfg_.stop_on_callback_exception) {
                impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR,
                                                      diagnostics::ErrorCategory::COMMUNICATION, "on_bytes", {},
                                                      "Unknown exception in callback", false, 0);
                impl->mark_disconnected();
                impl->discard_connection_writes();
                impl->close_port();
                impl->state_.set(LinkState::Error);
                impl->notify_state();
                return;
              }
              impl->handle_error(self, "on_bytes_callback", make_error_code(boost::system::errc::io_error));
              return;
            }
          }
          impl->start_read(self);
        }));
  }

  void do_write(std::shared_ptr<Serial> self) {
    if (stopping_.load() || !opened_.load() || tx_.empty() || writing_) return;
    std::unique_lock<std::mutex> submission_lock(submission_mtx_);
    if (stopping_.load() || !opened_.load()) return;
    writing_ = true;
    auto batch = std::make_shared<WriteBatch>();
    batch->bytes = queue_util::take_gather_batch(tx_, batch->buffers, batch->views, BufferProjection{});
    for (const auto& item : batch->buffers) send_accounting_.begin(item.request);
    active_write_ = batch;
    const auto connection = connection_seq_.load();
    auto completion = track_io(
        self,
        [self, connection, batch](boost::system::error_code ec, size_t written) {
          auto* impl = self->get_impl();
          bool current_connection;
          {
            std::lock_guard<std::mutex> lock(impl->submission_mtx_);
            current_connection = connection == impl->connection_seq_.load();
            size_t remaining = written;
            for (const auto& item : batch->buffers) {
              const auto bytes =
                  std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, item.buffer);
              const auto confirmed = std::min(remaining, bytes);
              impl->send_accounting_.complete(item.request, confirmed);
              remaining -= confirmed;
            }
            // A short composed write without an error is still a terminal failure.
            if (!ec && written < batch->bytes) ec = net::error::connection_reset;
            if (current_connection && ec) impl->mark_disconnected_locked();
          }
          if (!current_connection) {
            batch->buffers.clear();
            return;
          }
          impl->active_write_.reset();
          impl->writing_ = false;
          if (impl->stopping_.load()) {
            batch->buffers.clear();
            return;
          }
          if (ec) {
            queue_util::return_gather_batch(impl->tx_, batch->buffers);
            impl->handle_error(self, "write", ec);
            return;
          }
          batch->buffers.clear();
          queue_util::release_reserved_write_bytes(impl->queued_bytes_, batch->bytes);
          impl->stats_.record_sent(written);
          impl->report_backpressure(impl->queued_bytes_);
          impl->do_write(self);
        },
        true);
    try {
      port_->async_write(batch->views, std::move(completion));
    } catch (...) {
      mark_disconnected_locked();
      submission_lock.unlock();
      const auto ec = make_error_code(boost::system::errc::no_buffer_space);
      handle_error(self, "write", ec);
    }
  }

  void perform_cleanup() {
    try {
      retry_timer_.cancel();
      mark_disconnected();
      discard_connection_writes();
      close_port();
      tx_.clear();
      queued_bytes_ = 0;
      pending_.clear();
      pending_bytes_ = 0;
      writing_ = false;
      report_backpressure(queued_bytes_);
      opened_.store(false);
      state_.set(LinkState::Closed);
      notify_state();
    } catch (...) {
    }
  }

  void perform_stop_cleanup() {
    if (cleanup_started_) return;
    cleanup_started_ = true;
    perform_cleanup();
    backpressure_active_ = false;
    cleanup_finished_ = true;
    if (pending_io_ == 0) mark_cleanup_done();
  }

  void handle_error(std::shared_ptr<Serial> self, const char* where, const boost::system::error_code& ec) {
    if (stopping_) return;
    if (ec == boost::asio::error::eof && std::string_view(where) == "read") {
      if (self) start_read(self);
      return;
    }

    if (ec == boost::asio::error::operation_aborted) {
      if (state_.is_state(LinkState::Error)) return;
      // Connecting means a reopen is already scheduled and the port was closed
      // on purpose - the read this aborts is the one that closure cancelled.
      // Cleaning up here would cancel that retry and report Closed instead.
      if (state_.is_state(LinkState::Connecting)) return;
      perform_cleanup();
      return;
    }

    bool retryable = cfg_.reopen_on_error;
    diagnostics::error_reporting::report_connection_error("serial", where, ec, retryable);

    WIRESTEAD_LOG_ERROR("serial", where, fmt::format("Error: {}", ec.message()));
    error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, where, ec,
                                    ec.message(), retryable, 0);

    if (cfg_.reopen_on_error) {
      mark_disconnected();
      discard_connection_writes();
      close_port();
      state_.set(LinkState::Connecting);
      notify_state();
      if (self) schedule_retry(self, where, ec);
    } else {
      mark_disconnected();
      discard_connection_writes();
      close_port();
      state_.set(LinkState::Error);
      notify_state();
    }
  }

  void schedule_retry(std::shared_ptr<Serial> self, const char* where, const boost::system::error_code& ec) {
    (void)ec;
    WIRESTEAD_LOG_INFO("serial", "retry", fmt::format("Scheduling retry at {}", where));
    if (stopping_.load()) return;
    retry_timer_.expires_after(std::chrono::milliseconds(retry_interval_ms_.load()));
    retry_timer_.async_wait(track_io(self, [self](auto e) {
      if (!e && !self->get_impl()->stopping_.load()) self->get_impl()->open_and_configure(self);
    }));
  }

  // Rearmed by every read that carried bytes, so the deadline always measures
  // silence rather than time since connect. Writes deliberately do not rearm
  // it: a driver polling a mute device would otherwise keep it alive forever.
  void reset_rx_idle_timer(std::shared_ptr<Serial> self) {
    const unsigned timeout_ms = cfg_.rx_idle_timeout_ms;
    if (timeout_ms == 0 || stopping_.load()) return;

    const auto connection = connection_seq_.load();
    rx_idle_timer_.expires_after(std::chrono::milliseconds(timeout_ms));
    rx_idle_timer_.async_wait(track_io(self, [self, timeout_ms, connection](const boost::system::error_code& e) {
      if (e) return;  // rearmed or cancelled
      auto impl = self->get_impl();
      if (impl->stopping_.load() || !impl->opened_.load() || connection != impl->connection_seq_) return;

      WIRESTEAD_LOG_WARNING("serial", "rx_idle_timeout",
                            fmt::format("No data received for {}ms on {}", timeout_ms, impl->cfg_.device));
      // Same path a read error takes, so reopen_on_error decides what happens
      // next and the reconnect/backoff plumbing is not duplicated here.
      impl->handle_error(self, "rx_idle_timeout", make_error_code(boost::asio::error::timed_out));
    }));
  }

  void close_port() {
    rx_idle_timer_.cancel();
    boost::system::error_code ec;
    if (port_ && port_->is_open()) {
      port_->close(ec);
    }
  }

  void notify_state() {
    if (stopping_.load()) return;
    interface::SharedCallback<OnState> on_state;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_state = on_state_;
    }
    if (!on_state) return;
    try {
      (*on_state)(state_.get());
    } catch (...) {
    }
  }

  // Unlike every other migrated transport, this deliberately keeps taking no
  // `self` parameter and passes an empty kick_write hook to the shared
  // state machine below: perform_cleanup() (reachable from ~Impl(), where
  // shared_from_this() cannot be used at all) calls this with no self
  // available, and the corresponding do_write() kick after a backpressure
  // OFF transition is still issued manually at every async_write_* call
  // site exactly as before (#434 - normalizing this into the shared hook
  // was judged not worth the self-availability hazard in the cleanup path).
  void report_backpressure(size_t qb) {
    if (stopping_.load()) return;
    observe_queue();

    interface::SharedCallback<OnBackpressure> on_bp;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_bp = on_bp_;
    }
    static const OnBackpressure kNoCallback;

    auto f = bp_fields();
    queue_util::report_backpressure(
        f, qb, on_bp ? *on_bp : kNoCallback, stats_,
        [&]() -> size_t {
          const size_t moved = pending_bytes_.exchange(0);
          while (!pending_.empty()) {
            tx_.emplace_back(std::move(pending_.front()));
            pending_.pop_front();
          }
          return moved;
        },
        [&]() { observe_queue(); });
  }
};

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg, bool use_shared_context) {
  return std::shared_ptr<Serial>(new Serial(cfg, use_shared_context));
}

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg, net::io_context& ioc) {
  return std::shared_ptr<Serial>(new Serial(cfg, std::make_unique<BoostSerialPort>(ioc), ioc));
}

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg,
                                       std::unique_ptr<interface::SerialPortInterface> port, net::io_context& ioc) {
  return std::shared_ptr<Serial>(new Serial(cfg, std::move(port), ioc));
}

Serial::Serial(const config::SerialConfig& cfg, bool use_shared_context)
    : impl_(std::make_unique<Impl>(cfg, use_shared_context)) {}

Serial::Serial(const config::SerialConfig& cfg, std::unique_ptr<interface::SerialPortInterface> port,
               net::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, std::move(port), ioc)) {}

Serial::~Serial() {
  if (impl_) stop();
}

Serial::Serial(Serial&&) noexcept = default;
Serial& Serial::operator=(Serial&&) noexcept = default;

void Serial::start() {
  auto impl = get_impl();
  if (impl->started_) return;
  if (impl->ioc_thread_.joinable()) impl->ioc_thread_.join();
  if (impl->owns_ioc_ && impl->ioc_.stopped()) impl->ioc_.restart();
  if (impl->uses_shared_context_) {
    auto& manager = concurrency::IoContextManager::instance();
    if (!manager.is_running()) manager.start();
  }
  const auto generation = impl->generation_.fetch_add(1) + 1;
  {
    std::lock_guard<std::mutex> lock(impl->stop_mtx_);
    impl->cleanup_done_ = false;
  }
  impl->cleanup_started_ = false;
  impl->cleanup_finished_ = false;
  impl->stopping_ = false;
  impl->opened_ = false;
  impl->backpressure_active_ = false;
  impl->inflight_bytes_ = 0;
  impl->state_.set(LinkState::Idle);
  impl->started_ = true;
  impl->work_guard_ =
      std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl->ioc_.get_executor());
  if (impl->owns_ioc_) {
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_.stop(); });
        impl->ioc_.run();
      } catch (...) {
      }
    });
  }
  net::post(impl->strand_, [self = shared_from_this(), generation] {
    auto impl = self->get_impl();
    if (generation != impl->generation_ || impl->stopping_) return;
    impl->state_.set(LinkState::Connecting);
    impl->notify_state();
    impl->open_and_configure(self);
  });
}

void Serial::stop() {
  auto impl = get_impl();
  const bool on_executor = impl->ioc_.get_executor().running_in_this_thread();
  bool first;
  {
    std::lock_guard<std::mutex> lock(impl->submission_mtx_);
    first = !impl->stopping_.exchange(true);
    if (first) impl->send_accounting_.end(Impl::Ledger::Cause::ExplicitStop);
    if (first && impl->write_wait_) impl->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
  }
  if (first) {
    if (impl->generation_.load() == 0)
      impl->perform_stop_cleanup();
    else {
      auto self = weak_from_this().lock();
      net::post(impl->strand_, [self, impl] { impl->perform_stop_cleanup(); });
    }
  }
  if (on_executor) return;
  detail::stop_test_hook(impl, false);
  {
    std::unique_lock<std::mutex> lock(impl->stop_mtx_);
    impl->stop_cv_.wait(lock, [impl] { return impl->cleanup_done_; });
  }
  std::lock_guard<std::mutex> join_lock(impl->join_mtx_);
  if (impl->ioc_thread_.joinable()) impl->ioc_thread_.join();
}

bool Serial::is_connected() const { return get_impl()->opened_.load(); }
bool Serial::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
std::optional<size_t> Serial::write_queue_limit() const { return get_impl()->bp_limit_; }
wrapper::RuntimeStats Serial::stats() const {
  auto impl = get_impl();
  auto result = impl->stats_.snapshot(impl->queued_bytes_.load(std::memory_order_relaxed),
                                      impl->pending_bytes_.load(std::memory_order_relaxed),
                                      impl->backpressure_active_.load(std::memory_order_relaxed));
  result.send_accounting = impl->send_accounting_.snapshot();
  return result;
}
void Serial::reset_stats() {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->submission_mtx_);
  impl->send_accounting_.reset();
  impl->stats_.reset(impl->queued_bytes_.load(std::memory_order_relaxed) +
                     impl->pending_bytes_.load(std::memory_order_relaxed));
}

std::optional<diagnostics::ErrorInfo> Serial::last_error_info() const {
  return get_impl()->error_info_holder_.last_error_info();
}

void Serial::fail_receive() {
  const auto connection = write_connection();
  if (!connection) return;
  const auto run = impl_->generation_.load();
  net::dispatch(impl_->strand_, [self = shared_from_this(), connection, run] {
    if (run != self->impl_->generation_.load() || self->write_connection() != connection) return;
    self->impl_->handle_error(self, "receive_limit", make_error_code(boost::system::errc::no_buffer_space));
  });
}

boost::asio::any_io_executor Serial::get_executor() { return impl_->strand_; }

std::shared_ptr<detail::SerialWriteWait> Serial::capture_write_wait() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stopping_.load() || !impl_->opened_.load()) return {};
  return impl_->write_wait_;
}

std::optional<wrapper::SendResult> Serial::poll_write_wait(const std::shared_ptr<detail::SerialWriteWait>& wait) const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (!wait) return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (wait->ended_by) return wrapper::SendResult::reject(*wait->ended_by);
  if (wait->sequence != impl_->connection_seq_.load() || !impl_->opened_.load())
    return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (!impl_->backpressure_active_.load()) return wrapper::SendResult::accept();
  return std::nullopt;
}

void Serial::cancel_write_waits() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->write_wait_) impl_->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
}

std::optional<uint64_t> Serial::write_connection() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stopping_.load() || !impl_->opened_.load()) return std::nullopt;
  return impl_->connection_seq_.load();
}

wrapper::SendResult Serial::write_state() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (auto reason = impl_->admission_rejection()) return wrapper::SendResult::reject(*reason);
  return wrapper::SendResult::accept();
}

wrapper::SendResult Serial::async_write_copy_result(memory::ConstByteSpan data) {
  const auto result = write_copy(data, std::nullopt);
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_serial_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->generation_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_serial_write_admission_hook.load()) hook();

  size_t size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("serial", "async_write_copy", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }

  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("serial", "async_write_copy",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  if (size <= 65536 && impl_->cfg_.enable_memory_pool) {
    try {
      memory::PooledBuffer pooled_buffer(size, impl_->pool_);
      if (pooled_buffer.valid()) {
        base::safe_memory::safe_memcpy(pooled_buffer.data(), data.data(), size);
        const auto added = pooled_buffer.size();
        if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queued_bytes_, impl_->pending_bytes_,
                                                 impl_->inflight_bytes_, added, impl_->bp_limit_)) {
          impl_->stats_.record_failed_send();
          return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
        }
        impl_->stats_.record_accepted(added);
        Impl::Ledger::Admission admission(impl_->send_accounting_, added);
        const auto request = admission.request();
        net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(pooled_buffer), added, seq, connection,
                                   request]() mutable {
          if (seq != self->impl_->generation_.load()) return;
          if (connection != self->impl_->connection_seq_.load()) {
            self->impl_->stats_.record_dropped(1, added);
            queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_,
                                                     added);
            return;
          }
          self->impl_->route_enqueued_buffer(self, Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request}, added);
        });
        admission.commit();
        return wrapper::SendResult::accept();
      }
    } catch (const std::exception& e) {
      WIRESTEAD_LOG_ERROR("serial", "async_write_copy", fmt::format("Failed to acquire pooled buffer: {}", e.what()));
    }
  }

  std::vector<uint8_t> fallback(data.begin(), data.end());
  const auto added = fallback.size();
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queued_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(fallback), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->generation_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request}, added);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult Serial::async_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = write_move(std::move(data), std::nullopt);
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_serial_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->generation_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_serial_write_admission_hook.load()) hook();
  const auto size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("serial", "async_write_move", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("serial", "async_write_move",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queued_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->generation_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request}, added);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult Serial::async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = write_shared(std::move(data), std::nullopt);
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                         std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_serial_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->generation_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_serial_write_admission_hook.load()) hook();
  if (!data || data->empty()) {
    WIRESTEAD_LOG_WARNING("serial", "async_write_shared", "Ignoring empty shared buffer");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  const auto size = data->size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("serial", "async_write_shared",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queued_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->generation_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request}, added);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult Serial::async_try_write_copy_result(memory::ConstByteSpan data) {
  const auto result = try_write_copy(data);
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::try_write_copy(memory::ConstByteSpan data) {
  if (data.empty()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  if (data.size() > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }
  return try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

wrapper::SendResult Serial::async_try_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = try_write_move(std::move(data));
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::try_write_move(std::vector<uint8_t>&& data) {
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->generation_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_serial_write_admission_hook.load()) hook();
  const auto added = data.size();
  if (added == 0 || added > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(added == 0 ? wrapper::SendRejection::InvalidArgument
                                                  : wrapper::SendRejection::TooLarge);
  }
  const auto reject_for_pressure = [this, added]() {
    if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl_->stats_.record_dropped(1, added);
    } else {
      impl_->stats_.record_failed_send();
    }
  };
  if (impl_->backpressure_active_.load() || impl_->queued_bytes_ + added > impl_->bp_high_ ||
      impl_->queued_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl_->write_reserve_mtx_, impl_->inflight_bytes_, impl_->queued_bytes_,
                                           impl_->pending_bytes_, impl_->backpressure_active_, added, impl_->bp_high_,
                                           impl_->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->generation_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      // The old connection drain already removed this queue reservation.
      return;
    }
    auto impl = self->impl_.get();
    if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queued_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back(Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request});
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult Serial::async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = try_write_shared(std::move(data));
  if (auto hook = detail::g_serial_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult Serial::try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->generation_.load();
  const auto connection = impl_->connection_seq_.load();
  if (!data || data->empty()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  if (data->size() > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }
  const auto added = data->size();
  const auto reject_for_pressure = [this, added]() {
    if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl_->stats_.record_dropped(1, added);
    } else {
      impl_->stats_.record_failed_send();
    }
  };
  if (auto reason = impl_->admission_rejection()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_serial_write_admission_hook.load()) hook();
  if (impl_->backpressure_active_.load() || impl_->queued_bytes_ + added > impl_->bp_high_ ||
      impl_->queued_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl_->write_reserve_mtx_, impl_->inflight_bytes_, impl_->queued_bytes_,
                                           impl_->pending_bytes_, impl_->backpressure_active_, added, impl_->bp_high_,
                                           impl_->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->generation_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      // The old connection drain already removed this queue reservation.
      return;
    }
    auto impl = self->impl_.get();
    if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queued_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back(Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request});
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

void Serial::on_bytes(OnBytes cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(shared);
}
void Serial::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(shared);
}
void Serial::on_backpressure(OnBackpressure cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(shared);
}

void Serial::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  get_impl()->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void Serial::set_retry_interval(unsigned interval_ms) {
  if (interval_ms < base::constants::MIN_RETRY_INTERVAL_MS) {
    interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
  } else if (interval_ms > base::constants::MAX_RETRY_INTERVAL_MS) {
    interval_ms = base::constants::MAX_RETRY_INTERVAL_MS;
  }
  get_impl()->retry_interval_ms_.store(interval_ms, std::memory_order_relaxed);
}

}  // namespace transport
}  // namespace wirestead
