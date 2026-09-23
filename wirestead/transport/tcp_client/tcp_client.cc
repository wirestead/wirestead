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

#include "wirestead/transport/tcp_client/tcp_client.hpp"

#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#ifdef WIRESTEAD_TLS_ENABLED
#include <boost/asio/ssl.hpp>
#endif
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/error_mapping.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/base/bp_state_machine.hpp"
#include "wirestead/transport/base/bp_utils.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/tcp_client/detail/reconnect_decider.hpp"
#include "wirestead/transport/tcp_client/detail/write_wait.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

using base::LinkState;
using concurrency::AtomicLinkState;
using config::TcpClientConfig;
using interface::Channel;

struct TcpClient::Impl {
  // Members moved from TcpClient
  std::shared_ptr<net::io_context> owned_ioc_;
  net::io_context* ioc_ = nullptr;
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;
  std::atomic<uint64_t> lifecycle_seq_{0};
  std::atomic<uint64_t> stop_seq_{0};
  std::atomic<uint64_t> current_seq_{0};
  // Changes when a usable connection ends, independently of start/stop runs.
  std::atomic<uint64_t> connection_seq_{0};
  std::shared_ptr<detail::TcpWriteWait> write_wait_;
  tcp::resolver resolver_;
  tcp::socket socket_;

#ifdef WIRESTEAD_TLS_ENABLED
  // The stream borrows socket_ rather than owning it, so connect, socket
  // options, cancel and close all keep operating on socket_ exactly as they do
  // without TLS. Only reads, writes and shutdown route through here, and only
  // while a connection is up - it is rebuilt per connection because a TLS
  // session cannot outlive the socket it negotiated on.
  std::shared_ptr<boost::asio::ssl::context> ssl_context_;
  std::optional<boost::asio::ssl::stream<tcp::socket&>> tls_;
  // Whether IO should currently be routed through tls_, tracked separately
  // from tls_.has_value() because the stream now outlives the connection - see
  // close_socket(). Strand-confined, like the stream itself.
  bool tls_engaged_ = false;
#endif

  // True when this connection is encrypted. Reads to it happen on the strand.
  bool tls_active() const {
#ifdef WIRESTEAD_TLS_ENABLED
    return tls_engaged_;
#else
    return false;
#endif
  }
  // Guards the mutable subset of cfg_ (retry_interval_ms, max_retries,
  // connection_timeout_ms, idle_timeout_ms, idle_timeout_action) and
  // reconnect_policy_ below - the fields that have runtime setters
  // (set_retry_interval() etc.) reachable from any user thread while the
  // strand concurrently reads them for reconnect/idle-timeout decisions.
  // Fields with no runtime setter (tcp_no_delay, keep_alive, buffer sizes)
  // are set once at construction and read only at connect time, so they
  // don't need this lock (#436).
  mutable std::mutex cfg_mtx_;
  TcpClientConfig cfg_;
  // #443: per-channel pool instead of the process-wide GlobalMemoryPool
  // singleton - avoids cross-channel contention on the singleton's bucket
  // mutexes. Capacity is much smaller than the old shared default (400/2000)
  // since it's no longer amortized across every channel in the process.
  // Prefill stays 0. This literal was written while MemoryPool discarded
  // initial_pool_size, so 50 allocated nothing; #575 made the parameter real
  // and turned it into ~1 MiB eagerly allocated per channel at construction.
  // The pool fills as buffers are released.
  memory::MemoryPool pool_{0, 200};
  net::steady_timer retry_timer_;
  net::steady_timer connect_timer_;
  net::steady_timer idle_timer_;
  bool owns_ioc_ = true;
  // Orders caller-side write admission and strand submission before stop.
  // Never run strand handlers/user callbacks while holding this mutex.
  std::mutex submission_mtx_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> terminal_state_notified_{false};
  std::atomic<bool> reconnect_pending_{false};

  // Per-connection storage, sized from cfg_.read_buffer_size. A cancelled
  // read retains its buffer even if a replacement connection is ready.
  std::shared_ptr<std::vector<uint8_t>> rx_;
  std::deque<BufferVariant> tx_;
  std::deque<BufferVariant> pending_;
  std::atomic<size_t> pending_bytes_{0};
  // Each operation owns its buffers until its completion handler returns,
  // even if a replacement connection has already started writing.
  struct WriteBatch {
    std::vector<BufferVariant> buffers;
    std::vector<net::const_buffer> views;
    size_t bytes = 0;
  };
  std::shared_ptr<WriteBatch> active_write_;
  bool writing_ = false;
  std::atomic<size_t> queue_bytes_{0};
  // Bytes accepted by a plain async_write_* call but not yet routed onto the
  // strand - reserved via try_reserve_limit_bytes() to close the
  // accept-then-drop race (jwsung91/wirestead#517). inflight_bytes_ mutations
  // and the queue_bytes_/pending_bytes_ increments that promote a
  // reservation both go through write_reserve_mtx_ - see bp_utils.hpp.
  std::atomic<size_t> inflight_bytes_{0};
  std::mutex write_reserve_mtx_;
  // D-1: shutdown *requested* and shutdown *completed* are separate states.
  // The cleanup handler that runs on the strand is what completes it, and
  // waiting callers wait for that signal rather than for a mutex - a mutex
  // only says another caller was here, not that the teardown ran.
  std::mutex stop_mtx_;
  std::condition_variable stop_cv_;
  bool cleanup_done_ = false;
  std::atomic<bool> cleanup_started_{false};
  // Strand-confined. Cancellation does not mean its completion handler has
  // run; keep the run closed until those handlers have left the old buffers.
  size_t pending_io_ = 0;
  bool cleanup_finished_ = false;
  struct IoCompletion {
    Impl* impl;
    ~IoCompletion() {
      if (impl->cleanup_finished_ && impl->pending_io_ == 1) {
        if (auto hook = detail::g_tcp_io_completion_hook.load()) hook();
      }
      if (--impl->pending_io_ == 0 && impl->cleanup_finished_) impl->mark_cleanup_done();
    }
  };

  // Serializes the join itself: joining a thread from two callers is not
  // allowed, and the second caller must not return before the first has.
  std::mutex join_mtx_;

  void mark_cleanup_done() {
    detail::stop_test_hook(this, true);
    {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      cleanup_done_ = true;
    }
    stop_cv_.notify_all();
  }

  // Waits for the teardown that was posted to the strand. The contract's
  // precondition for an externally run io_context is that the caller keeps it
  // running, so this waits and nothing more: running the caller's executor
  // here would execute other channels' handlers on the stopping thread.
  void wait_for_cleanup() {
    detail::stop_test_hook(this, false);
    std::unique_lock<std::mutex> lock(stop_mtx_);
    stop_cv_.wait(lock, [this] { return cleanup_done_; });
  }

  // Called with submission_mtx_ held. Explicit stop and readiness publication
  // use that mutex; cleanup completion has its own condition-variable mutex.
  std::optional<wrapper::SendRejection> admission_rejection(
      std::optional<uint64_t> expected_connection = std::nullopt) {
    if (stop_requested_.load()) {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      return cleanup_done_ ? wrapper::SendRejection::NotStarted : wrapper::SendRejection::Stopping;
    }
    if (current_seq_.load() == 0) return wrapper::SendRejection::NotStarted;
    if ((expected_connection && *expected_connection != connection_seq_.load()) || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error) || !ioc_ || !connected_.load()) {
      return wrapper::SendRejection::NotReady;
    }
    return std::nullopt;
  }

  // Atomic rather than mutex-guarded: read both from the strand and from
  // arbitrary caller threads (async_try_write_* fast-fail prechecks) (#436).
  std::atomic<base::constants::BackpressureStrategy> bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_high_;
  size_t bp_low_;
  size_t bp_limit_;
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;
  unsigned first_retry_interval_ms_ = 100;

  // Shared snapshots rather than plain std::functions: the io thread copies
  // one out per received chunk, and a std::function copy allocates whenever
  // the target outgrows its small-object buffer. See interface::SharedCallback.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;
  mutable std::mutex callback_mtx_;
  std::atomic<bool> connected_{false};
  AtomicLinkState state_{LinkState::Idle};
  int retry_attempts_ = 0;
  uint32_t reconnect_attempt_count_{0};
  std::optional<ReconnectPolicy> reconnect_policy_;

  ErrorInfoHolder error_info_holder_{"tcp_client"};

  Impl(const TcpClientConfig& cfg, net::io_context* ioc_ptr)
      : owned_ioc_(ioc_ptr ? nullptr : std::make_shared<net::io_context>()),
        ioc_(ioc_ptr ? ioc_ptr : owned_ioc_.get()),
        strand_(net::make_strand(*ioc_)),
        resolver_(strand_),
        socket_(strand_),
        cfg_(cfg),
        retry_timer_(strand_),
        connect_timer_(strand_),
        idle_timer_(strand_),
        owns_ioc_(!ioc_ptr),
        bp_strategy_(cfg.backpressure_strategy),
        bp_high_(cfg.backpressure_threshold) {
    init();
  }

  void mark_disconnected() {
    std::lock_guard<std::mutex> lock(submission_mtx_);
    if (connected_.exchange(false)) {
      if (write_wait_) write_wait_->end(wrapper::SendRejection::NotReady);
      connection_seq_.fetch_add(1);
    }
  }

  void discard_connection_writes();

  void init() {
    connected_ = false;
    writing_ = false;
    queue_bytes_ = 0;
    pending_bytes_ = 0;
    cfg_.validate_and_clamp();
    rx_ = std::make_shared<std::vector<uint8_t>>(cfg_.read_buffer_size);
    recalculate_backpressure_bounds();
    first_retry_interval_ms_ = std::min(first_retry_interval_ms_, cfg_.retry_interval_ms);
  }

  void do_resolve_connect(std::shared_ptr<TcpClient> self, uint64_t seq);
  void schedule_retry(std::shared_ptr<TcpClient> self, uint64_t seq);
  void start_read(std::shared_ptr<TcpClient> self, uint64_t seq);
  void do_write(std::shared_ptr<TcpClient> self, uint64_t seq);
  void handle_close(std::shared_ptr<TcpClient> self, uint64_t seq, const boost::system::error_code& ec = {});
  void handle_idle_timeout(std::shared_ptr<TcpClient> self, uint64_t seq);
  void transition_to(LinkState next, const boost::system::error_code& ec = {});
  void perform_stop_cleanup();
  void reset_start_state();
  void join_ioc_thread(bool allow_detach);
  void close_socket();
  void recalculate_backpressure_bounds();
  void report_backpressure(std::shared_ptr<TcpClient> self, size_t queued_bytes);
  void observe_queue();
  // Shared decide_enqueue()/route dispatch used by all 3 async_write_* variants (#434).
  // `reserved` tells this whether the caller reserved `added` bytes into
  // inflight_bytes_ via try_reserve_limit_bytes() - only Reliable-strategy
  // sends do (jwsung91/wirestead#517); BestEffort's plain path has no
  // precheck and relies entirely on decide_enqueue()'s own keep-latest trim.
  void route_enqueued_buffer(std::shared_ptr<TcpClient> self, BufferVariant&& buf, size_t added, bool reserved);
  queue_util::BackpressureFields bp_fields();
  void notify_state();
  void reset_io_objects();
  void apply_socket_options();
  void handshake_then(std::shared_ptr<TcpClient> self, uint64_t seq, std::function<void()> next);
  void finish_connect(std::shared_ptr<TcpClient> self, uint64_t seq);
  void reset_idle_timer(std::shared_ptr<TcpClient> self, uint64_t seq);
  void cancel_idle_timer();
  void record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat, std::string_view operation,
                    const boost::system::error_code& ec, std::string_view msg, bool retryable, uint32_t retry_count);
};

std::shared_ptr<TcpClient> TcpClient::create(const TcpClientConfig& cfg) {
  return std::shared_ptr<TcpClient>(new TcpClient(cfg));
}

std::shared_ptr<TcpClient> TcpClient::create(const TcpClientConfig& cfg, boost::asio::io_context& ioc) {
  return std::shared_ptr<TcpClient>(new TcpClient(cfg, ioc));
}

TcpClient::TcpClient(const TcpClientConfig& cfg) : impl_(std::make_unique<Impl>(cfg, nullptr)) {}
TcpClient::TcpClient(const TcpClientConfig& cfg, boost::asio::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, &ioc)) {}

TcpClient::~TcpClient() {
  // #446: null after being moved-from - the move ctor/assignment are
  // defaulted, and destroying a moved-from instance must not dereference
  // a null impl_ (matches TcpServer/Serial/UdpChannel/UdsServer's
  // destructors, which already guard this way).
  if (!impl_) return;
  stop();
  impl_->join_ioc_thread(true);

  impl_->on_bytes_ = nullptr;
  impl_->on_state_ = nullptr;
  impl_->on_bp_ = nullptr;
}

TcpClient::TcpClient(TcpClient&&) noexcept = default;
TcpClient& TcpClient::operator=(TcpClient&&) noexcept = default;

std::optional<diagnostics::ErrorInfo> TcpClient::last_error_info() const {
  return impl_->error_info_holder_.last_error_info();
}

void TcpClient::start() {
  auto current_state = impl_->state_.get();
  if (current_state == LinkState::Connecting || current_state == LinkState::Connected) {
    WIRESTEAD_LOG_DEBUG("tcp_client", "start", "Start called while already active, ignoring");
    return;
  }

  if (!impl_->ioc_) {
    WIRESTEAD_LOG_ERROR("tcp_client", "start", "io_context is null");
  }

  impl_->recalculate_backpressure_bounds();

  if (impl_->ioc_ && impl_->ioc_->stopped()) {
    WIRESTEAD_LOG_DEBUG("tcp_client", "start", "io_context stopped; restarting before start");
    impl_->ioc_->restart();
  }

  if (impl_->ioc_thread_.joinable()) {
    impl_->join_ioc_thread(false);
  }

  const auto seq = impl_->lifecycle_seq_.fetch_add(1) + 1;
  impl_->current_seq_.store(seq);
  impl_->reset_start_state();

  if (impl_->owns_ioc_ && impl_->ioc_) {
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl_->ioc_->get_executor());
    impl_->ioc_thread_ = std::jthread([ioc = impl_->owned_ioc_](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [ioc] { ioc->stop(); });
        ioc->run();
      } catch (const std::exception& e) {
        WIRESTEAD_LOG_ERROR("tcp_client", "io_context", fmt::format("IO context error: {}", e.what()));
        diagnostics::error_reporting::report_system_error("tcp_client", "io_context",
                                                          fmt::format("Exception in IO context: {}", e.what()));
      }
    });
  }

  auto weak_self = weak_from_this();
  if (impl_->ioc_) {
    net::dispatch(impl_->strand_, [weak_self, seq] {
      if (auto self = weak_self.lock()) {
        if (seq <= self->impl_->stop_seq_.load()) {
          return;
        }
        self->impl_->mark_disconnected();
        self->impl_->reset_io_objects();
        self->impl_->transition_to(LinkState::Connecting);
        self->impl_->do_resolve_connect(self, seq);
      }
    });
  } else {
    WIRESTEAD_LOG_ERROR("tcp_client", "start", "io_context is null");
  }
}

void TcpClient::stop() {
  const bool on_executor = impl_->ioc_->get_executor().running_in_this_thread();
  bool first;
  {
    std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
    first = !impl_->stop_requested_.exchange(true);
    if (first) {
      if (impl_->write_wait_) impl_->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
      impl_->stopping_.store(true);
      impl_->stop_seq_.store(impl_->current_seq_.load());
    }
  }
  if (first) {
    // Only a never-started transport has no executor work to serialize with.
    if (impl_->current_seq_.load() == 0) {
      impl_->perform_stop_cleanup();
    } else {
      // A waiting destructor keeps Impl alive until this handler completes.
      // Ordinary requests retain the transport across request-only returns.
      auto self = weak_from_this().lock();
      auto* impl = impl_.get();
      net::post(impl_->strand_, [self, impl] { impl->perform_stop_cleanup(); });
    }
  }
  if (on_executor) return;

  // External executors must keep progressing, as required by D-1. Never
  // poll them here or infer completion from stopped()/elapsed time.
  impl_->wait_for_cleanup();
  std::lock_guard<std::mutex> join_lock(impl_->join_mtx_);
  impl_->join_ioc_thread(false);
}

bool TcpClient::is_connected() const { return get_impl()->connected_.load(); }
bool TcpClient::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
std::optional<size_t> TcpClient::write_queue_limit() const { return impl_->bp_limit_; }
wrapper::RuntimeStats TcpClient::stats() const {
  return impl_->stats_.snapshot(impl_->queue_bytes_.load(std::memory_order_relaxed),
                                impl_->pending_bytes_.load(std::memory_order_relaxed),
                                impl_->backpressure_active_.load(std::memory_order_relaxed));
}
void TcpClient::reset_stats() {
  impl_->stats_.reset(impl_->queue_bytes_.load(std::memory_order_relaxed) +
                      impl_->pending_bytes_.load(std::memory_order_relaxed));
}

boost::asio::any_io_executor TcpClient::get_executor() { return impl_->socket_.get_executor(); }

std::shared_ptr<detail::TcpWriteWait> TcpClient::capture_write_wait() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stop_requested_.load() || !impl_->connected_.load()) return {};
  return impl_->write_wait_;
}

std::optional<wrapper::SendResult> TcpClient::poll_write_wait(const std::shared_ptr<detail::TcpWriteWait>& wait) const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (!wait) return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (wait->ended_by) return wrapper::SendResult::reject(*wait->ended_by);
  if (wait->sequence != impl_->connection_seq_.load() || !impl_->connected_.load())
    return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (!impl_->backpressure_active_.load()) return wrapper::SendResult::accept();
  return std::nullopt;
}

void TcpClient::cancel_write_waits() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->write_wait_) impl_->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
}

std::optional<uint64_t> TcpClient::write_connection() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stop_requested_.load() || !impl_->connected_.load()) return std::nullopt;
  return impl_->connection_seq_.load();
}

bool TcpClient::async_write_copy(memory::ConstByteSpan data) {
  const auto result = write_copy(data, std::nullopt);
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_tcp_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_tcp_write_admission_hook.load()) hook();

  size_t size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("tcp_client", "async_write_copy", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }

  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("tcp_client", "async_write_copy",
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
        const bool reliable = impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable;
        if (reliable &&
            !queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                                 impl_->inflight_bytes_, added, impl_->bp_limit_)) {
          impl_->stats_.record_failed_send();
          return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
        }
        impl_->stats_.record_accepted(added);
        net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(pooled_buffer), added, reliable, seq,
                                   connection]() mutable {
          if (seq != self->impl_->current_seq_.load()) return;
          if (connection != self->impl_->connection_seq_.load()) {
            self->impl_->stats_.record_dropped(1, added);
            if (reliable)
              queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_,
                                                       added);
            return;
          }
          self->impl_->route_enqueued_buffer(self, BufferVariant{std::move(buf)}, added, reliable);
        });
        return wrapper::SendResult::accept();
      }
    } catch (const std::exception& e) {
      WIRESTEAD_LOG_ERROR("tcp_client", "async_write_copy",
                          fmt::format("Failed to acquire pooled buffer: {}", e.what()));
    }
  }

  std::vector<uint8_t> fallback(data.begin(), data.end());
  const auto added = fallback.size();
  const bool reliable = impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable;
  if (reliable &&
      !queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(fallback), added, reliable, seq,
                             connection]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      if (reliable)
        queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, BufferVariant{std::move(buf)}, added, reliable);
  });
  return wrapper::SendResult::accept();
}

bool TcpClient::async_write_move(std::vector<uint8_t>&& data) {
  const auto result = write_move(std::move(data), std::nullopt);
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_tcp_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_tcp_write_admission_hook.load()) hook();
  const auto size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("tcp_client", "async_write_move", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("tcp_client", "async_write_move",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  const bool reliable = impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable;
  if (reliable &&
      !queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, reliable, seq,
                             connection]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      if (reliable)
        queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, BufferVariant{std::move(buf)}, added, reliable);
  });
  return wrapper::SendResult::accept();
}

bool TcpClient::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = write_shared(std::move(data), std::nullopt);
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                            std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_tcp_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_tcp_write_admission_hook.load()) hook();
  if (!data || data->empty()) {
    WIRESTEAD_LOG_WARNING("tcp_client", "async_write_shared", "Ignoring empty shared buffer");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  const auto size = data->size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("tcp_client", "async_write_shared",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  const bool reliable = impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable;
  if (reliable &&
      !queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, reliable, seq,
                             connection]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      if (reliable)
        queue_util::release_reserved_limit_bytes(self->impl_->write_reserve_mtx_, self->impl_->inflight_bytes_, added);
      return;
    }
    self->impl_->route_enqueued_buffer(self, BufferVariant{std::move(buf)}, added, reliable);
  });
  return wrapper::SendResult::accept();
}

bool TcpClient::async_try_write_copy(memory::ConstByteSpan data) {
  const auto result = try_write_copy(data);
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::try_write_copy(memory::ConstByteSpan data) {
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

bool TcpClient::async_try_write_move(std::vector<uint8_t>&& data) {
  const auto result = try_write_move(std::move(data));
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::try_write_move(std::vector<uint8_t>&& data) {
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_tcp_write_admission_hook.load()) hook();
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
  if (impl_->backpressure_active_.load() || impl_->queue_bytes_ + added > impl_->bp_high_ ||
      impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl_->queue_bytes_, impl_->pending_bytes_, impl_->backpressure_active_,
                                           added, impl_->bp_high_, impl_->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      // The old connection drain already removed this queue reservation.
      return;
    }
    auto impl = self->impl_.get();
    if (impl->stop_requested_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    if (!impl->writing_) impl->do_write(self, impl->current_seq_.load());
  });
  return wrapper::SendResult::accept();
}

bool TcpClient::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = try_write_shared(std::move(data));
  if (auto hook = detail::g_tcp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpClient::try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
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
  if (auto hook = detail::g_tcp_write_admission_hook.load()) hook();
  if (impl_->backpressure_active_.load() || impl_->queue_bytes_ + added > impl_->bp_high_ ||
      impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl_->queue_bytes_, impl_->pending_bytes_, impl_->backpressure_active_,
                                           added, impl_->bp_high_, impl_->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
    if (connection != self->impl_->connection_seq_.load()) {
      self->impl_->stats_.record_dropped(1, added);
      // The old connection drain already removed this queue reservation.
      return;
    }
    auto impl = self->impl_.get();
    if (impl->stop_requested_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    if (!impl->writing_) impl->do_write(self, impl->current_seq_.load());
  });
  return wrapper::SendResult::accept();
}

// Each setter builds the shared snapshot before taking the lock, so the
// allocation stays outside the critical section the io thread contends on.
void TcpClient::on_bytes(OnBytes cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(shared);
}
void TcpClient::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(shared);
}
void TcpClient::on_backpressure(OnBackpressure cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(shared);
}
void TcpClient::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void TcpClient::set_retry_interval(unsigned interval_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.retry_interval_ms = interval_ms;
  impl_->cfg_.validate_and_clamp();
}
void TcpClient::set_max_retries(int max_retries) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.max_retries = max_retries;
  impl_->cfg_.validate_and_clamp();
}
void TcpClient::set_connection_timeout(unsigned timeout_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.connection_timeout_ms = timeout_ms;
  impl_->cfg_.validate_and_clamp();
}
void TcpClient::set_idle_timeout(unsigned timeout_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.idle_timeout_ms = timeout_ms;
  impl_->cfg_.validate_and_clamp();
}
void TcpClient::set_idle_timeout_action(IdleTimeoutAction action) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.idle_timeout_action = action;
}
void TcpClient::set_reconnect_policy(ReconnectPolicy policy) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  if (policy) {
    impl_->reconnect_policy_ = std::move(policy);
  } else {
    impl_->reconnect_policy_ = std::nullopt;
  }
}

// Impl methods implementation

void TcpClient::Impl::apply_socket_options() {
  boost::system::error_code ec;

  if (cfg_.tcp_no_delay) {
    socket_.set_option(tcp::no_delay(true), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("tcp_client", "socket_options", fmt::format("Failed to set TCP_NODELAY: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.keep_alive) {
    socket_.set_option(net::socket_base::keep_alive(true), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("tcp_client", "socket_options", fmt::format("Failed to set keep_alive: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.send_buffer_size > 0) {
    socket_.set_option(net::socket_base::send_buffer_size(static_cast<int>(cfg_.send_buffer_size)), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("tcp_client", "socket_options",
                            fmt::format("Failed to set send buffer size: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.receive_buffer_size > 0) {
    socket_.set_option(net::socket_base::receive_buffer_size(static_cast<int>(cfg_.receive_buffer_size)), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("tcp_client", "socket_options",
                            fmt::format("Failed to set receive buffer size: {}", ec.message()));
      ec.clear();
    }
  }
}

void TcpClient::Impl::do_resolve_connect(std::shared_ptr<TcpClient> self, uint64_t seq) {
  ++pending_io_;
  resolver_.async_resolve(
      cfg_.host, fmt::format("{}", cfg_.port), [self, seq](auto ec, tcp::resolver::results_type results) {
        IoCompletion completion{self->impl_.get()};
        if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
          return;
        }
        if (self->impl_->stop_requested_.load() || self->impl_->stopping_.load()) {
          return;
        }
        if (ec) {
          bool has_policy;
          {
            std::lock_guard<std::mutex> lock(self->impl_->cfg_mtx_);
            has_policy = self->impl_->reconnect_policy_.has_value();
          }
          uint32_t current_attempts =
              has_policy ? self->impl_->reconnect_attempt_count_ : static_cast<uint32_t>(self->impl_->retry_attempts_);
          self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "resolve",
                                    ec, fmt::format("Resolution failed: {}", ec.message()),
                                    diagnostics::is_retryable_tcp_connect_error(ec), current_attempts);
          self->impl_->schedule_retry(self, seq);
          return;
        }
        unsigned connection_timeout_ms;
        {
          std::lock_guard<std::mutex> lock(self->impl_->cfg_mtx_);
          connection_timeout_ms = self->impl_->cfg_.connection_timeout_ms;
        }
        self->impl_->connect_timer_.expires_after(std::chrono::milliseconds(connection_timeout_ms));
        ++self->impl_->pending_io_;
        self->impl_->connect_timer_.async_wait([self, seq](const boost::system::error_code& timer_ec) {
          IoCompletion completion{self->impl_.get()};
          if (timer_ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
            return;
          }
          if (!timer_ec && !self->impl_->stop_requested_.load() && !self->impl_->stopping_.load()) {
            bool has_policy;
            unsigned timeout_ms;
            {
              std::lock_guard<std::mutex> lock(self->impl_->cfg_mtx_);
              has_policy = self->impl_->reconnect_policy_.has_value();
              timeout_ms = self->impl_->cfg_.connection_timeout_ms;
            }
            WIRESTEAD_LOG_ERROR("tcp_client", "connect_timeout",
                                fmt::format("Connection timed out after {}ms", timeout_ms));
            uint32_t current_attempts = has_policy ? self->impl_->reconnect_attempt_count_
                                                   : static_cast<uint32_t>(self->impl_->retry_attempts_);
            self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "connect",
                                      boost::asio::error::timed_out, "Connection timed out",
                                      diagnostics::is_retryable_tcp_connect_error(boost::asio::error::timed_out),
                                      current_attempts);
            self->impl_->handle_close(self, seq, boost::asio::error::timed_out);
          }
        });

        ++self->impl_->pending_io_;
        net::async_connect(self->impl_->socket_, results, [self, seq](auto ec2, const auto&) {
          IoCompletion completion{self->impl_.get()};
          if (ec2 == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
            return;
          }
          if (self->impl_->stop_requested_.load() || self->impl_->stopping_.load()) {
            self->impl_->close_socket();
            self->impl_->connect_timer_.cancel();
            return;
          }
          if (ec2) {
            self->impl_->connect_timer_.cancel();
            bool has_policy;
            {
              std::lock_guard<std::mutex> lock(self->impl_->cfg_mtx_);
              has_policy = self->impl_->reconnect_policy_.has_value();
            }
            uint32_t current_attempts = has_policy ? self->impl_->reconnect_attempt_count_
                                                   : static_cast<uint32_t>(self->impl_->retry_attempts_);
            self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "connect",
                                      ec2, fmt::format("Connection failed: {}", ec2.message()),
                                      diagnostics::is_retryable_tcp_connect_error(ec2), current_attempts);
            self->impl_->schedule_retry(self, seq);
            return;
          }
          self->impl_->connect_timer_.cancel();
          self->impl_->retry_attempts_ = 0;
          self->impl_->reconnect_attempt_count_ = 0;

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
          int yes = 1;
          (void)::setsockopt(static_cast<int>(self->impl_->socket_.native_handle()), SOL_SOCKET, SO_NOSIGPIPE, &yes,
                             static_cast<socklen_t>(sizeof(yes)));
#endif

          self->impl_->apply_socket_options();

          // TCP is up; TLS still has to prove who answered. Reading before the
          // handshake completes would hand the session ciphertext, and a failed
          // handshake must not be reported as a working connection - so the
          // rest of the connect path waits behind it. Plaintext runs the
          // continuation immediately, which is what it did before TLS existed.
          self->impl_->handshake_then(self, seq, [self, seq] { self->impl_->finish_connect(self, seq); });
        });
      });
}

// Sets up the TLS stream if configured, runs the handshake, then hands control
// back. Without TLS - or in a build without it - `next` runs straight away and
// the connect path is byte for byte what it was.
void TcpClient::Impl::handshake_then(std::shared_ptr<TcpClient> self, uint64_t seq, std::function<void()> next) {
#ifdef WIRESTEAD_TLS_ENABLED
  bool want_tls = false;
  std::string ca_file;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    want_tls = cfg_.tls_enabled;
    ca_file = cfg_.tls_ca_file;
  }

  if (want_tls) {
    namespace ssl = boost::asio::ssl;
    try {
      if (!ssl_context_) {
        auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
        ctx->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                         ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1);
        if (ca_file.empty()) {
          ctx->set_default_verify_paths();
        } else {
          ctx->load_verify_file(ca_file);
        }
        // Not optional. An unverified TLS connection encrypts traffic to
        // whoever answered, which is what an attacker in the middle wants.
        ctx->set_verify_mode(ssl::verify_peer);
        ssl_context_ = std::move(ctx);
      }
      tls_.emplace(socket_, *ssl_context_);
      tls_engaged_ = true;
      tls_->set_verify_callback(ssl::host_name_verification(cfg_.host));
      // SNI, and the name the certificate is checked against.
      ::SSL_set_tlsext_host_name(tls_->native_handle(), cfg_.host.c_str());
    } catch (const std::exception& e) {
      const std::string msg = std::string("TLS setup failed: ") + e.what();
      WIRESTEAD_LOG_ERROR("tcp_client", "handshake", msg);
      record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "handshake",
                   boost::asio::error::invalid_argument, msg, false, 0);
      tls_.reset();
      tls_engaged_ = false;
      handle_close(self, seq, boost::asio::error::invalid_argument);
      return;
    }

    ++pending_io_;
    tls_->async_handshake(ssl::stream_base::client,
                          net::bind_executor(strand_, [this, self, seq, next](const boost::system::error_code& ec) {
                            IoCompletion completion{this};
                            if (seq != current_seq_.load() || stop_requested_.load() || stopping_.load()) return;
                            if (ec) {
                              WIRESTEAD_LOG_ERROR("tcp_client", "handshake", "TLS handshake failed: " + ec.message());
                              record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                           "handshake", ec, "TLS handshake failed: " + ec.message(), false, 0);
                              // Not tls_.reset(): this runs from inside the
                              // stream's own completion handler, and
                              // close_socket() below explains why the stream
                              // has to outlive its operations.
                              tls_engaged_ = false;
                              handle_close(self, seq, ec);
                              return;
                            }
                            next();
                          }));
    return;
  }
#else
  (void)self;
  (void)seq;
#endif
  next();
}

void TcpClient::Impl::finish_connect(std::shared_ptr<TcpClient> self, uint64_t seq) {
  // Set here rather than at TCP connect: with TLS, a socket whose handshake
  // has not finished is not a usable connection, and connected() is what
  // callers poll before sending. Reporting true for a peer that failed
  // verification would be worse than useless.
  {
    std::lock_guard<std::mutex> lock(submission_mtx_);
    if (seq != current_seq_.load() || stop_requested_.load() || stopping_.load()) return;
    write_wait_ = std::make_shared<detail::TcpWriteWait>(connection_seq_.load());
    connected_.store(true);
  }
  rx_ = std::make_shared<std::vector<uint8_t>>(cfg_.read_buffer_size);
  const auto connection = connection_seq_.load();
  transition_to(LinkState::Connected);
  boost::system::error_code ep_ec;
  auto rep = socket_.remote_endpoint(ep_ec);
  if (!ep_ec) {
    WIRESTEAD_LOG_INFO("tcp_client", "connect",
                       fmt::format("Connected to {}:{}", rep.address().to_string(), rep.port()));
  }
  start_read(self, seq);
  reset_idle_timer(self, seq);
  net::post(strand_, [self, seq, connection]() {
    if (seq != self->impl_->current_seq_.load() || connection != self->impl_->connection_seq_.load()) return;
    if (!self->impl_->writing_) self->impl_->do_write(self, seq);
  });
}

void TcpClient::Impl::schedule_retry(std::shared_ptr<TcpClient> self, uint64_t seq) {
  mark_disconnected();
  if (stop_requested_.load() || stopping_.load()) {
    return;
  }

  // Prevent double scheduling of reconnect
  if (reconnect_pending_.exchange(true)) {
    return;
  }

  std::optional<diagnostics::ErrorInfo> last_err = error_info_holder_.last_error_info();

  if (!last_err) {
    last_err = diagnostics::ErrorInfo(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                      "tcp_client", "schedule_retry", "Unknown error",
                                      make_error_code(boost::asio::error::not_connected), true);
  }

  // Snapshot once rather than locking repeatedly for each read below -
  // cfg_/reconnect_policy_ can change concurrently via set_retry_interval()
  // etc. from any user thread while this runs on the strand (#436).
  TcpClientConfig cfg_snapshot;
  std::optional<ReconnectPolicy> reconnect_policy_snapshot;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    cfg_snapshot = cfg_;
    reconnect_policy_snapshot = reconnect_policy_;
  }

  // Determine current attempt count based on active mode
  uint32_t current_attempts =
      reconnect_policy_snapshot ? reconnect_attempt_count_ : static_cast<uint32_t>(retry_attempts_);

  auto decision = detail::decide_reconnect(cfg_snapshot, *last_err, current_attempts, reconnect_policy_snapshot);

  if (!decision.should_retry) {
    WIRESTEAD_LOG_INFO("tcp_client", "retry", "Reconnect stopped by policy/config");
    transition_to(LinkState::Error);
    reconnect_pending_.store(false);
    return;
  }

  // The decider returns a base delay for both policy and fallback paths.
  std::chrono::milliseconds delay = decision.delay.value_or(std::chrono::milliseconds(cfg_snapshot.retry_interval_ms));
  if (reconnect_policy_snapshot) {
    reconnect_attempt_count_++;
  } else {
    // Preserve existing "fast first retry" behavior for non-policy mode.
    ++retry_attempts_;
    if (retry_attempts_ == 1) {
      delay = std::chrono::milliseconds(first_retry_interval_ms_);
    }
  }

  transition_to(LinkState::Connecting);

  WIRESTEAD_LOG_INFO("tcp_client", "retry",
                     fmt::format("Scheduling retry in {:.3f}s", static_cast<double>(delay.count()) / 1000.0));

  retry_timer_.expires_after(delay);
  ++pending_io_;
  retry_timer_.async_wait([self, seq](const boost::system::error_code& ec) {
    IoCompletion completion{self->impl_.get()};
    // Clear pending flag regardless of result (fired or aborted)
    self->impl_->reconnect_pending_.store(false);

    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
      return;
    }
    if (!ec && !self->impl_->stop_requested_.load() && !self->impl_->stopping_.load())
      self->impl_->do_resolve_connect(self, seq);
  });
}

void TcpClient::Impl::start_read(std::shared_ptr<TcpClient> self, uint64_t seq) {
  const auto connection = connection_seq_.load();
  auto buffer = rx_;
  ++pending_io_;
  auto on_read = [self, seq, connection, buffer](auto ec, std::size_t n) {
    IoCompletion completion{self->impl_.get()};
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load() ||
        connection != self->impl_->connection_seq_.load()) {
      return;
    }
    if (self->impl_->stop_requested_.load()) {
      return;
    }
    if (ec) {
      self->impl_->handle_close(self, seq, ec);
      return;
    }
    if (n > 0) {
      self->impl_->reset_idle_timer(self, seq);
    }
    interface::SharedCallback<OnBytes> on_bytes;
    {
      std::lock_guard<std::mutex> lock(self->impl_->callback_mtx_);
      on_bytes = self->impl_->on_bytes_;
    }

    self->impl_->stats_.record_received(n);

    if (on_bytes) {
      try {
        (*on_bytes)(memory::ConstByteSpan(buffer->data(), n));
      } catch (const std::exception& e) {
        WIRESTEAD_LOG_ERROR("tcp_client", "on_bytes", fmt::format("Exception in on_bytes callback: {}", e.what()));
        self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "on_bytes",
                                  boost::asio::error::connection_aborted,
                                  fmt::format("Exception in on_bytes: {}", e.what()), false, 0);
        self->impl_->handle_close(self, seq, make_error_code(boost::asio::error::connection_aborted));
        return;
      } catch (...) {
        WIRESTEAD_LOG_ERROR("tcp_client", "on_bytes", "Unknown exception in on_bytes callback");
        self->impl_->handle_close(self, seq, make_error_code(boost::asio::error::connection_aborted));
        return;
      }
    }
    self->impl_->start_read(self, seq);
  };
#ifdef WIRESTEAD_TLS_ENABLED
  if (tls_active()) {
    tls_->async_read_some(net::buffer(buffer->data(), buffer->size()), std::move(on_read));
    return;
  }
#endif
  socket_.async_read_some(net::buffer(buffer->data(), buffer->size()), std::move(on_read));
}

void TcpClient::Impl::do_write(std::shared_ptr<TcpClient> self, uint64_t seq) {
  if (stop_requested_.load()) {
    tx_.clear();
    queue_bytes_ = 0;
    pending_.clear();
    pending_bytes_ = 0;
    writing_ = false;
    report_backpressure(self, queue_bytes_);
    return;
  }

  if (!connected_.load()) {
    writing_ = false;
    return;
  }

  if (tx_.empty() || state_.is_state(LinkState::Closed) || state_.is_state(LinkState::Error)) {
    writing_ = false;
    return;
  }
  writing_ = true;

  auto batch = std::make_shared<WriteBatch>();
  const auto queued_bytes = queue_util::take_gather_batch(tx_, batch->buffers, batch->views);
  batch->bytes = queued_bytes;
  active_write_ = batch;
  const auto connection = connection_seq_.load();

  ++pending_io_;
  auto on_write = [self, queued_bytes, seq, connection, batch](auto ec, std::size_t bytes_written) {
    IoCompletion completion{self->impl_.get()};
    if (connection != self->impl_->connection_seq_.load()) {
      // Release pooled buffers while the captured client still owns its pool.
      batch->buffers.clear();
      return;
    }
    self->impl_->active_write_.reset();
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
      batch->buffers.clear();
      self->impl_->queue_bytes_ =
          (self->impl_->queue_bytes_ > queued_bytes) ? (self->impl_->queue_bytes_ - queued_bytes) : 0;
      self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
      self->impl_->writing_ = false;
      return;
    }

    if (ec) {
      queue_util::return_gather_batch(self->impl_->tx_, batch->buffers);

      WIRESTEAD_LOG_ERROR("tcp_client", "do_write", fmt::format("Write failed: {}", ec.message()));
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write", ec,
                                fmt::format("Write failed: {}", ec.message()), false, 0);
      self->impl_->writing_ = false;
      self->impl_->handle_close(self, seq, ec);
      return;
    }

    batch->buffers.clear();
    self->impl_->stats_.record_sent(bytes_written);
    if (bytes_written > 0) {
      self->impl_->reset_idle_timer(self, seq);
    }
    self->impl_->queue_bytes_ =
        (self->impl_->queue_bytes_ > queued_bytes) ? (self->impl_->queue_bytes_ - queued_bytes) : 0;
    self->impl_->report_backpressure(self, self->impl_->queue_bytes_);

    if (self->impl_->stop_requested_.load() || self->impl_->state_.is_state(LinkState::Closed) ||
        self->impl_->state_.is_state(LinkState::Error)) {
      self->impl_->writing_ = false;
      return;
    }

    self->impl_->do_write(self, seq);
  };

#ifdef WIRESTEAD_TLS_ENABLED
  if (tls_active()) {
    net::async_write(*tls_, batch->views, on_write);
    return;
  }
#endif
  net::async_write(socket_, batch->views, on_write);
}

void TcpClient::Impl::handle_close(std::shared_ptr<TcpClient> self, uint64_t seq, const boost::system::error_code& ec) {
  if (ec == net::error::operation_aborted || seq != current_seq_.load()) {
    return;
  }
  mark_disconnected();
  discard_connection_writes();
  WIRESTEAD_LOG_INFO("tcp_client", "handle_close", fmt::format("Closing connection. Error: {}", ec.message()));
  if (ec) {
    bool has_policy;
    {
      std::lock_guard<std::mutex> lock(cfg_mtx_);
      has_policy = reconnect_policy_.has_value();
    }
    const bool retryable = diagnostics::is_retryable_tcp_connect_error(ec);
    const uint32_t current_attempts = has_policy ? reconnect_attempt_count_ : static_cast<uint32_t>(retry_attempts_);

    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "handle_close", ec,
                 fmt::format("Connection closed with error: {}", ec.message()), retryable, current_attempts);
  }
  writing_ = false;
  cancel_idle_timer();
  connect_timer_.cancel();
  close_socket();
  if (stop_requested_.load() || stopping_.load() || state_.is_state(LinkState::Closed)) {
    transition_to(LinkState::Closed, ec);
    return;
  }
  transition_to(LinkState::Connecting, ec);
  schedule_retry(self, seq);
}

void TcpClient::Impl::handle_idle_timeout(std::shared_ptr<TcpClient> self, uint64_t seq) {
  if (seq != current_seq_.load() || stop_requested_.load() || stopping_.load() || !connected_.load()) {
    return;
  }

  mark_disconnected();
  discard_connection_writes();
  IdleTimeoutAction idle_timeout_action;
  unsigned idle_timeout_ms;
  bool has_policy;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    idle_timeout_action = cfg_.idle_timeout_action;
    idle_timeout_ms = cfg_.idle_timeout_ms;
    has_policy = reconnect_policy_.has_value();
  }

  const auto ec = make_error_code(boost::asio::error::timed_out);
  const bool should_reconnect = idle_timeout_action == IdleTimeoutAction::Reconnect;
  const uint32_t current_attempts = has_policy ? reconnect_attempt_count_ : static_cast<uint32_t>(retry_attempts_);

  WIRESTEAD_LOG_WARNING("tcp_client", "idle_timeout",
                        fmt::format("Idle timeout expired after {}ms; {}", idle_timeout_ms,
                                    should_reconnect ? "scheduling reconnect" : "closing connection"));
  record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "idle_timeout", ec,
               "Idle timeout expired", should_reconnect, current_attempts);

  writing_ = false;
  cancel_idle_timer();
  connect_timer_.cancel();
  close_socket();

  if (!should_reconnect) {
    transition_to(LinkState::Closed, ec);
    return;
  }

  transition_to(LinkState::Connecting, ec);
  schedule_retry(self, seq);
}

void TcpClient::Impl::discard_connection_writes() {
  size_t messages = tx_.size() + pending_.size();
  size_t bytes = 0;
  for (const auto& buffer : tx_)
    bytes += std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, buffer);
  for (const auto& buffer : pending_)
    bytes += std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, buffer);
  if (active_write_) {
    messages += active_write_->buffers.size();
    bytes += active_write_->bytes;
    // Its completion handler retains the buffers; it must not mutate new state.
    active_write_.reset();
  }
  tx_.clear();
  pending_.clear();
  queue_bytes_ = 0;
  pending_bytes_ = 0;
  writing_ = false;
  backpressure_active_ = false;
  if (messages) stats_.record_dropped(messages, bytes);
}

void TcpClient::Impl::close_socket() {
  boost::system::error_code ec;
#ifdef WIRESTEAD_TLS_ENABLED
  // One SSL_shutdown writes close_notify and returns; a second would wait for
  // the peer's, which is the block measured at 8 s on the server side.
  if (tls_) ::SSL_shutdown(tls_->native_handle());
  // Deliberately no tls_.reset() here. socket_.cancel()/close() do not retract
  // a boost::asio::ssl::detail::io_op continuation that is already queued on
  // the strand: the strand runs it afterwards, and it calls back into the
  // engine's BIO. Destroying the stream at this point therefore left that
  // continuation dereferencing freed memory - a SIGSEGV inside BIO_ctrl,
  // reproducible under parallel load and seen intermittently in CI.
  //
  // The stream is instead destroyed by the next tls_.emplace(), which runs on
  // the strand after any pending continuation, or by ~Impl once the io thread
  // has been joined. It cannot be moved out to a holder either - a pending
  // operation holds the stream's original address.
  tls_engaged_ = false;
#endif
  socket_.shutdown(tcp::socket::shutdown_both, ec);
  socket_.close(ec);
}

void TcpClient::Impl::recalculate_backpressure_bounds() {
  bp_high_ = cfg_.backpressure_threshold;
  bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
  if (bp_low_ == 0) {
    bp_low_ = 1;
  }
  bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                       base::constants::MAX_BUFFER_SIZE);
  if (bp_limit_ < bp_high_) {
    bp_limit_ = bp_high_;
  }
  backpressure_active_ = false;
}

queue_util::BackpressureFields TcpClient::Impl::bp_fields() {
  return queue_util::BackpressureFields{queue_bytes_,
                                        pending_bytes_,
                                        backpressure_active_,
                                        bp_high_,
                                        bp_low_,
                                        bp_limit_,
                                        bp_strategy_.load(std::memory_order_relaxed)};
}

void TcpClient::Impl::route_enqueued_buffer(std::shared_ptr<TcpClient> self, BufferVariant&& buf, size_t added,
                                            bool reserved) {
  if (stop_requested_.load() || state_.is_state(LinkState::Closed) || state_.is_state(LinkState::Error)) {
    if (reserved) queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
    stats_.record_failed_send();
    return;
  }

  auto f = bp_fields();
  queue_util::DropAccounting dropped;
  auto decision = queue_util::decide_enqueue(f, added, tx_, dropped);
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);

  if (decision == queue_util::EnqueueDecision::Rejected) {
    WIRESTEAD_LOG_ERROR("tcp_client", "write", fmt::format("Queue limit exceeded ({} bytes)", queue_bytes_ + added));
    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write",
                 boost::asio::error::no_buffer_space, "Queue limit exceeded", false, 0);
    // #448: this path used to leave RuntimeStats showing the message as
    // accepted (from the caller-thread pre-check) with no corresponding
    // sent/dropped/queued accounting - record it as dropped so it's at
    // least observable.
    stats_.record_dropped(1, added);
    if (reserved) queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
    report_backpressure(self, queue_bytes_ + added);
    return;
  }
  if (decision == queue_util::EnqueueDecision::Pending) {
    if (reserved) {
      queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, pending_bytes_, inflight_bytes_, added);
    } else {
      queue_util::commit_unreserved_limit_bytes(write_reserve_mtx_, pending_bytes_, added);
    }
    pending_.emplace_back(std::move(buf));
    observe_queue();
    return;
  }
  if (reserved) {
    queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, queue_bytes_, inflight_bytes_, added);
  } else {
    queue_util::commit_unreserved_limit_bytes(write_reserve_mtx_, queue_bytes_, added);
  }
  tx_.emplace_back(std::move(buf));
  observe_queue();
  report_backpressure(self, queue_bytes_);
  if (!writing_) do_write(self, current_seq_.load());
}

void TcpClient::Impl::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void TcpClient::Impl::report_backpressure(std::shared_ptr<TcpClient> self, size_t queued_bytes) {
  if (stop_requested_.load() || stopping_.load()) return;
  observe_queue();

  interface::SharedCallback<OnBackpressure> on_bp;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    on_bp = on_bp_;
  }
  static const OnBackpressure kNoCallback;

  auto f = bp_fields();
  queue_util::report_backpressure(
      f, queued_bytes, on_bp ? *on_bp : kNoCallback, stats_,
      [&]() -> size_t {
        const size_t moved = pending_bytes_.exchange(0);
        while (!pending_.empty()) {
          tx_.emplace_back(std::move(pending_.front()));
          pending_.pop_front();
        }
        return moved;
      },
      [&]() {
        observe_queue();
        if (!writing_) do_write(self, current_seq_.load());
      });
}

void TcpClient::Impl::transition_to(LinkState next, const boost::system::error_code& ec) {
  if (ec == net::error::operation_aborted) {
    return;
  }

  const auto current = state_.get();
  const bool retrying_same_state = (next == LinkState::Connecting && current == LinkState::Connecting);
  if ((current == LinkState::Closed || current == LinkState::Error) &&
      (next == LinkState::Closed || next == LinkState::Error)) {
    return;
  }

  if (next == LinkState::Closed || next == LinkState::Error) {
    if (terminal_state_notified_.exchange(true)) {
      return;
    }
  } else if (current == next && !retrying_same_state) {
    return;
  }

  state_.set(next);
  notify_state();
}

void TcpClient::Impl::perform_stop_cleanup() {
  if (cleanup_started_.exchange(true)) return;
  // Signals completion on every exit, including the error paths below.
  struct CompletionSignal {
    Impl* impl;
    ~CompletionSignal() {
      impl->cleanup_finished_ = true;
      if (impl->pending_io_ == 0) impl->mark_cleanup_done();
    }
  } completion_signal{this};
  try {
    retry_timer_.cancel();
    connect_timer_.cancel();
    cancel_idle_timer();
    resolver_.cancel();
    boost::system::error_code ec_cancel;
    socket_.cancel(ec_cancel);
    close_socket();
    tx_.clear();
    queue_bytes_ = 0;
    pending_.clear();
    pending_bytes_ = 0;
    writing_ = false;
    active_write_.reset();
    mark_disconnected();
    // Deliberately does NOT fire on_bp_ here, unlike UDP/server sessions'
    // terminal drain (#434): this is an explicit, tested contract
    // (ContractComplianceTest.TcpClient_Backpressure_Contract) - a
    // Reliable-mode caller blocked in send_blocking() is woken instead via
    // the wrapper's own bp_cv_.notify_all()/is_connected() check, not a
    // relief callback. Don't "fix" this to match the other transports
    // without updating that contract test first.
    backpressure_active_ = false;

    if (owns_ioc_ && work_guard_) {
      work_guard_->reset();
    }
    transition_to(LinkState::Closed);
  } catch (const std::exception& e) {
    WIRESTEAD_LOG_ERROR("tcp_client", "stop_cleanup", fmt::format("Cleanup error: {}", e.what()));
    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "stop_cleanup", {},
                 fmt::format("Cleanup error: {}", e.what()), false, 0);
    diagnostics::error_reporting::report_system_error("tcp_client", "stop_cleanup",
                                                      fmt::format("Exception in stop cleanup: {}", e.what()));
  } catch (...) {
    WIRESTEAD_LOG_ERROR("tcp_client", "stop_cleanup", "Unknown error in stop cleanup");
    diagnostics::error_reporting::report_system_error("tcp_client", "stop_cleanup", "Unknown error in stop cleanup");
  }
}

void TcpClient::Impl::reset_start_state() {
  cleanup_started_.store(false);
  cleanup_finished_ = false;
  inflight_bytes_.store(0);
  {
    std::lock_guard<std::mutex> lock(stop_mtx_);
    cleanup_done_ = false;
  }
  stop_requested_.store(false);
  stopping_.store(false);
  terminal_state_notified_.store(false);
  reconnect_pending_.store(false);
  retry_attempts_ = 0;
  reconnect_attempt_count_ = 0;
  mark_disconnected();
  writing_ = false;
  queue_bytes_ = 0;
  pending_.clear();
  pending_bytes_ = 0;
  backpressure_active_ = false;
  state_.set(LinkState::Idle);
}

void TcpClient::Impl::join_ioc_thread(bool allow_detach) {
  if (!owns_ioc_ || !ioc_thread_.joinable()) {
    return;
  }

  if (std::this_thread::get_id() == ioc_thread_.get_id()) {
    if (allow_detach) {
      ioc_thread_.detach();
    }
    return;
  }

  try {
    ioc_thread_.join();
  } catch (const std::exception& e) {
    WIRESTEAD_LOG_ERROR("tcp_client", "join", "Join failed: " + std::string(e.what()));
  } catch (...) {
    WIRESTEAD_LOG_ERROR("tcp_client", "join", "Join failed with unknown error");
  }
}

void TcpClient::Impl::notify_state() {
  if (stop_requested_.load() || stopping_.load()) return;

  interface::SharedCallback<OnState> on_state;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    on_state = on_state_;
  }
  if (!on_state) return;

  try {
    (*on_state)(state_.get());
  } catch (const std::exception& e) {
    WIRESTEAD_LOG_ERROR("tcp_client", "on_state", "Exception in state callback: " + std::string(e.what()));
  } catch (...) {
    WIRESTEAD_LOG_ERROR("tcp_client", "on_state", "Unknown exception in state callback");
  }
}

void TcpClient::Impl::record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat,
                                   std::string_view operation, const boost::system::error_code& ec,
                                   std::string_view msg, bool retryable, uint32_t retry_count) {
  error_info_holder_.record_error(lvl, cat, operation, ec, msg, retryable, retry_count);
}

void TcpClient::Impl::reset_io_objects() {
  try {
    boost::system::error_code ec_cancel;
    socket_.cancel(ec_cancel);
    close_socket();
    socket_ = tcp::socket(strand_);
    resolver_.cancel();
    resolver_ = tcp::resolver(strand_);
    retry_timer_ = net::steady_timer(strand_);
    connect_timer_ = net::steady_timer(strand_);
    idle_timer_ = net::steady_timer(strand_);
    tx_.clear();
    queue_bytes_ = 0;
    pending_.clear();
    pending_bytes_ = 0;
    writing_ = false;
    backpressure_active_ = false;
  } catch (const std::exception& e) {
    WIRESTEAD_LOG_ERROR("tcp_client", "reset_io_objects", fmt::format("Reset error: {}", e.what()));
    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "reset_io_objects", {},
                 fmt::format("Reset error: {}", e.what()), false, 0);
    diagnostics::error_reporting::report_system_error(
        "tcp_client", "reset_io_objects", fmt::format("Exception while resetting io objects: {}", e.what()));
  } catch (...) {
    WIRESTEAD_LOG_ERROR("tcp_client", "reset_io_objects", "Unknown reset error");
    diagnostics::error_reporting::report_system_error("tcp_client", "reset_io_objects",
                                                      "Unknown error while resetting io objects");
  }
}

void TcpClient::Impl::reset_idle_timer(std::shared_ptr<TcpClient> self, uint64_t seq) {
  unsigned idle_timeout_ms;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    idle_timeout_ms = cfg_.idle_timeout_ms;
  }
  if (idle_timeout_ms == 0 || !connected_.load() || stop_requested_.load() || stopping_.load()) {
    return;
  }

  idle_timer_.cancel();
  idle_timer_.expires_after(std::chrono::milliseconds(idle_timeout_ms));
  const auto connection = connection_seq_.load();
  ++pending_io_;
  idle_timer_.async_wait([self, seq, connection](const boost::system::error_code& ec) {
    IoCompletion completion{self->impl_.get()};
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load() ||
        connection != self->impl_->connection_seq_.load()) {
      return;
    }
    if (!ec) {
      self->impl_->handle_idle_timeout(self, seq);
    }
  });
}

void TcpClient::Impl::cancel_idle_timer() { idle_timer_.cancel(); }

}  // namespace transport
}  // namespace wirestead
