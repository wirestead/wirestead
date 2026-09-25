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

#include "wirestead/transport/uds/uds_client.hpp"

#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/diagnostics/send_accounting.hpp"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>

#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/error_mapping.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/base/bp_state_machine.hpp"
#include "wirestead/transport/base/bp_utils.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/uds/boost_uds_socket.hpp"
#include "wirestead/transport/uds/detail/reconnect_decider.hpp"
#include "wirestead/transport/uds/detail/write_wait.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

using base::LinkState;
using concurrency::AtomicLinkState;
using config::UdsClientConfig;
using interface::Channel;

struct UdsClient::Impl {
  std::shared_ptr<net::io_context> owned_ioc_;
  net::io_context* ioc_ = nullptr;
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;
  std::atomic<uint64_t> current_seq_{0};
  std::atomic<uint64_t> connection_seq_{0};
  std::shared_ptr<detail::UdsWriteWait> write_wait_;
  std::unique_ptr<interface::UdsSocketInterface> socket_;
  // Guards the mutable subset of cfg_ (retry_interval_ms, etc.) and
  // reconnect_policy_ below - see the identical rationale in
  // transport/tcp_client/tcp_client.cc (#436).
  mutable std::mutex cfg_mtx_;
  UdsClientConfig cfg_;
  // #443: UDS never actually pooled - async_write_copy always heap-allocated
  // a fresh std::vector, despite a dead PooledBuffer std::visit branch
  // suggesting otherwise. Give it a real per-channel pool like every other
  // transport, instead of the process-wide GlobalMemoryPool singleton.
  // Prefill stays 0. This literal was written while MemoryPool discarded
  // initial_pool_size, so 50 allocated nothing; #575 made the parameter real
  // and turned it into ~1 MiB eagerly allocated per channel at construction.
  // The pool fills as buffers are released.
  memory::MemoryPool pool_{0, 200};
  net::steady_timer retry_timer_;
  net::steady_timer connect_timer_;
  bool owns_ioc_ = true;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};

  std::mutex stop_mtx_;
  std::condition_variable stop_cv_;
  bool cleanup_done_ = false;
  bool cleanup_finished_ = false;
  size_t pending_io_ = 0;  // Strand-confined, including completion destruction.
  std::mutex join_mtx_;
  // Order accepted write submissions before the cleanup post. A caller
  // that passed a fast precheck must not post old work after completion.
  std::mutex submission_mtx_;

  // Caller holds submission_mtx_: state, connection and admission are one decision.
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

  void mark_cleanup_done() {
    detail::stop_test_hook(this, true);
    if (owns_ioc_) work_guard_.reset();
    {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      cleanup_done_ = true;
    }
    stop_cv_.notify_all();
  }

  // The socket interface erases executor associations into std::function.
  // Explicit dispatch preserves serialization. The lifetime also accounts for
  // test sockets that discard an operation instead of invoking its handler.
  template <typename Handler>
  auto track_io(std::shared_ptr<UdsClient> self, Handler handler, bool defer = false) {
    ++pending_io_;
    std::shared_ptr<void> lifetime(nullptr, [self](void*) {
      net::dispatch(self->impl_->strand_, [self] {
        auto* impl = self->impl_.get();
        if (impl->cleanup_finished_ && impl->pending_io_ == 1) {
          if (auto hook = detail::g_uds_io_completion_hook.load()) hook();
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

  // Sized from cfg_.read_buffer_size in init() rather than being a fixed
  // std::array, so a bulk-transfer workload can trade memory for fewer read
  // completions and callback dispatches.
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
  // Each completion retains its gather buffers across loss/reconnect. A stale
  // completion may release its batch but must not mutate the new connection.
  struct WriteBatch {
    std::vector<TrackedBuffer> buffers;
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
  // Atomic rather than mutex-guarded: read both from the strand and from
  // arbitrary caller threads (async_try_write_* fast-fail prechecks) (#436).
  std::atomic<base::constants::BackpressureStrategy> bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_high_;
  size_t bp_low_;
  size_t bp_limit_;
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;

  // Shared snapshots: the io thread copies one out per received chunk, and a
  // std::function copy allocates whenever the target outgrows its small-object
  // buffer. See interface::SharedCallback.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;
  mutable std::mutex callback_mtx_;
  std::atomic<bool> connected_{false};
  AtomicLinkState state_{LinkState::Idle};
  int retry_attempts_ = 0;
  uint32_t reconnect_attempt_count_{0};
  std::optional<ReconnectPolicy> reconnect_policy_;

  ErrorInfoHolder error_info_holder_{"uds_client"};

  Impl(const UdsClientConfig& cfg, net::io_context* ioc_ptr,
       std::unique_ptr<interface::UdsSocketInterface> socket = nullptr)
      : owned_ioc_(ioc_ptr ? nullptr : std::make_shared<net::io_context>()),
        ioc_(ioc_ptr ? ioc_ptr : owned_ioc_.get()),
        strand_(net::make_strand(*ioc_)),
        socket_(std::move(socket)),
        cfg_(cfg),
        retry_timer_(strand_),
        connect_timer_(strand_),
        owns_ioc_(!ioc_ptr),
        bp_strategy_(cfg.backpressure_strategy),
        bp_high_(cfg.backpressure_threshold) {
    if (!socket_) {
      socket_ = std::make_unique<BoostUdsSocket>(uds::socket(strand_));
    }
    init();
  }

  void mark_disconnected() {
    std::lock_guard<std::mutex> lock(submission_mtx_);
    mark_disconnected_locked();
  }
  void mark_disconnected_locked() {
    if (connected_.exchange(false)) {
      send_accounting_.end(Ledger::Cause::ConnectionLoss);
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
  }

  void do_connect(std::shared_ptr<UdsClient> self, uint64_t seq);
  void schedule_retry(std::shared_ptr<UdsClient> self, uint64_t seq);
  void start_read(std::shared_ptr<UdsClient> self, uint64_t seq);
  void do_write(std::shared_ptr<UdsClient> self, uint64_t seq);
  void handle_close(std::shared_ptr<UdsClient> self, uint64_t seq, const boost::system::error_code& ec = {});
  void transition_to(LinkState next, const boost::system::error_code& ec = {});
  void perform_stop_cleanup();
  void close_socket();
  void recalculate_backpressure_bounds();
  void report_backpressure(std::shared_ptr<UdsClient> self, size_t queued_bytes);
  void observe_queue();
  // Shared decide_enqueue()/route dispatch used by both async_write_* variants (#434).
  void route_enqueued_buffer(std::shared_ptr<UdsClient> self, TrackedBuffer&& buf, size_t added);
  queue_util::BackpressureFields bp_fields();
  void record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat, std::string_view operation,
                    const boost::system::error_code& ec, std::string_view msg, bool retryable, uint32_t retry_count);

  ~Impl() {
    stop_requested_ = true;
    stopping_ = true;

    retry_timer_.cancel();
    connect_timer_.cancel();
    close_socket();

    if (work_guard_) {
      work_guard_.reset();
    }

    if (ioc_ && owns_ioc_ && ioc_thread_.joinable()) {
      if (std::this_thread::get_id() == ioc_thread_.get_id()) {
        ioc_thread_.detach();
      } else {
        ioc_thread_.request_stop();
        ioc_thread_.join();
      }
    }
  }
};

std::shared_ptr<UdsClient> UdsClient::create(const UdsClientConfig& cfg) {
  return std::shared_ptr<UdsClient>(new UdsClient(cfg));
}

std::shared_ptr<UdsClient> UdsClient::create(const UdsClientConfig& cfg, boost::asio::io_context& ioc) {
  return std::shared_ptr<UdsClient>(new UdsClient(cfg, ioc));
}

std::shared_ptr<UdsClient> UdsClient::create(const UdsClientConfig& cfg,
                                             std::unique_ptr<interface::UdsSocketInterface> socket,
                                             boost::asio::io_context& ioc) {
  return std::shared_ptr<UdsClient>(new UdsClient(cfg, std::move(socket), ioc));
}

UdsClient::UdsClient(const UdsClientConfig& cfg) : impl_(std::make_unique<Impl>(cfg, nullptr)) {}
UdsClient::UdsClient(const UdsClientConfig& cfg, boost::asio::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, &ioc)) {}

UdsClient::UdsClient(const UdsClientConfig& cfg, std::unique_ptr<interface::UdsSocketInterface> socket,
                     boost::asio::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, &ioc, std::move(socket))) {}

UdsClient::~UdsClient() {
  // #446: null after being moved-from - the move ctor/assignment are
  // defaulted, and destroying a moved-from instance must not dereference
  // a null impl_ (matches TcpServer/Serial/UdpChannel/UdsServer's
  // destructors, which already guard this way).
  if (!impl_) return;
  stop();

  if (impl_->owns_ioc_ && impl_->ioc_thread_.joinable()) {
    if (std::this_thread::get_id() != impl_->ioc_thread_.get_id()) {
      impl_->ioc_thread_.join();
    } else {
      impl_->ioc_thread_.detach();
    }
  }
}

UdsClient::UdsClient(UdsClient&&) noexcept = default;
UdsClient& UdsClient::operator=(UdsClient&&) noexcept = default;

void UdsClient::start() {
  auto current_state = impl_->state_.get();
  if (current_state == LinkState::Connecting || current_state == LinkState::Connected) {
    return;
  }

  impl_->recalculate_backpressure_bounds();
  impl_->stop_requested_ = false;
  impl_->stopping_ = false;
  {
    std::lock_guard<std::mutex> lock(impl_->stop_mtx_);
    impl_->cleanup_done_ = false;
  }
  impl_->cleanup_finished_ = false;
  impl_->inflight_bytes_ = 0;
  impl_->active_write_.reset();
  impl_->current_seq_++;
  uint64_t seq = impl_->current_seq_.load();

  if (impl_->owns_ioc_ && !impl_->ioc_thread_.joinable()) {
    if (impl_->ioc_->stopped()) {
      impl_->ioc_->restart();
    }
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(net::make_work_guard(*impl_->ioc_));
    impl_->ioc_thread_ = std::jthread([ioc = impl_->owned_ioc_](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [ioc] { ioc->stop(); });
        ioc->run();
      } catch (...) {
      }
    });
  }

  net::post(impl_->strand_, [self = shared_from_this(), seq]() {
    if (self->impl_->stop_requested_ || seq != self->impl_->current_seq_) return;
    self->impl_->transition_to(LinkState::Connecting);
    self->impl_->do_connect(self, seq);
  });
}

void UdsClient::stop() {
  const bool on_executor = impl_->ioc_->get_executor().running_in_this_thread();
  {
    std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
    if (!impl_->stop_requested_.exchange(true)) {
      impl_->send_accounting_.end(Impl::Ledger::Cause::ExplicitStop);
      if (impl_->write_wait_) impl_->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
      impl_->stopping_ = true;
      impl_->connected_ = false;
      if (impl_->current_seq_.load() == 0) {
        impl_->perform_stop_cleanup();
      } else {
        auto self = weak_from_this().lock();
        auto* impl = impl_.get();
        net::post(impl_->strand_, [self, impl] { impl->perform_stop_cleanup(); });
      }
    }
  }
  if (on_executor) return;

  detail::stop_test_hook(impl_.get(), false);
  {
    std::unique_lock<std::mutex> lock(impl_->stop_mtx_);
    impl_->stop_cv_.wait(lock, [this] { return impl_->cleanup_done_; });
  }
  std::lock_guard<std::mutex> join_lock(impl_->join_mtx_);
  if (impl_->owns_ioc_ && impl_->ioc_thread_.joinable()) impl_->ioc_thread_.join();
}

bool UdsClient::is_connected() const { return impl_->connected_.load(); }
bool UdsClient::is_backpressure_active() const { return impl_->backpressure_active_.load(); }
std::optional<size_t> UdsClient::write_queue_limit() const { return impl_->bp_limit_; }
wrapper::RuntimeStats UdsClient::stats() const {
  auto result = impl_->stats_.snapshot(impl_->queue_bytes_.load(std::memory_order_relaxed),
                                       impl_->pending_bytes_.load(std::memory_order_relaxed),
                                       impl_->backpressure_active_.load(std::memory_order_relaxed));
  result.send_accounting = impl_->send_accounting_.snapshot();
  return result;
}
void UdsClient::reset_stats() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  impl_->send_accounting_.reset();
  impl_->stats_.reset(impl_->queue_bytes_.load(std::memory_order_relaxed) +
                      impl_->pending_bytes_.load(std::memory_order_relaxed));
}

boost::asio::any_io_executor UdsClient::get_executor() { return impl_->strand_; }

std::shared_ptr<detail::UdsWriteWait> UdsClient::capture_write_wait() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stop_requested_.load() || !impl_->connected_.load()) return {};
  return impl_->write_wait_;
}

std::optional<wrapper::SendResult> UdsClient::poll_write_wait(const std::shared_ptr<detail::UdsWriteWait>& wait) const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (!wait) return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (wait->ended_by) return wrapper::SendResult::reject(*wait->ended_by);
  if (wait->sequence != impl_->connection_seq_.load() || !impl_->connected_.load())
    return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (!impl_->backpressure_active_.load()) return wrapper::SendResult::accept();
  return std::nullopt;
}

void UdsClient::cancel_write_waits() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->write_wait_) impl_->write_wait_->end(wrapper::SendRejection::CancelledWhileWaiting);
}

std::optional<uint64_t> UdsClient::write_connection() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->stop_requested_.load() || !impl_->connected_.load()) return std::nullopt;
  return impl_->connection_seq_.load();
}

wrapper::SendResult UdsClient::write_state() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (auto reason = impl_->admission_rejection()) return wrapper::SendResult::reject(*reason);
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdsClient::async_write_copy_result(memory::ConstByteSpan data) {
  const auto result = write_copy(data, std::nullopt);
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_uds_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_uds_write_admission_hook.load()) hook();

  size_t size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("uds_client", "async_write_copy", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }

  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("uds_client", "async_write_copy",
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
        if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                                 impl_->inflight_bytes_, added, impl_->bp_limit_)) {
          impl_->stats_.record_failed_send();
          return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
        }
        impl_->stats_.record_accepted(added);
        Impl::Ledger::Admission admission(impl_->send_accounting_, added);
        const auto request = admission.request();
        net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(pooled_buffer), added, seq, connection,
                                   request]() mutable {
          if (seq != self->impl_->current_seq_.load()) return;
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
      WIRESTEAD_LOG_ERROR("uds_client", "async_write_copy",
                          fmt::format("Failed to acquire pooled buffer: {}", e.what()));
    }
  }

  std::vector<uint8_t> fallback(data.begin(), data.end());
  const auto added = fallback.size();
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(fallback), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
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

wrapper::SendResult UdsClient::async_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = write_move(std::move(data), std::nullopt);
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_uds_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_uds_write_admission_hook.load()) hook();
  const auto size = data.size();
  if (size == 0) {
    WIRESTEAD_LOG_WARNING("uds_client", "async_write_move", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("uds_client", "async_write_move",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
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

wrapper::SendResult UdsClient::async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = write_shared(std::move(data), std::nullopt);
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                            std::optional<uint64_t> expected_connection) {
  if (expected_connection) {
    if (auto hook = detail::g_uds_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection(expected_connection)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_uds_write_admission_hook.load()) hook();
  if (!data || data->empty()) {
    WIRESTEAD_LOG_WARNING("uds_client", "async_write_shared", "Ignoring empty shared buffer");
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::InvalidArgument);
  }
  const auto size = data->size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    WIRESTEAD_LOG_ERROR("uds_client", "async_write_shared",
                        fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }

  const auto added = size;
  if (!queue_util::try_reserve_limit_bytes(impl_->write_reserve_mtx_, impl_->queue_bytes_, impl_->pending_bytes_,
                                           impl_->inflight_bytes_, added, impl_->bp_limit_)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl_->stats_.record_accepted(added);
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();
  net::post(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added, seq, connection,
                             request]() mutable {
    if (seq != self->impl_->current_seq_.load()) return;
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

wrapper::SendResult UdsClient::async_try_write_copy_result(memory::ConstByteSpan data) {
  const auto result = try_write_copy(data);
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::try_write_copy(memory::ConstByteSpan data) {
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

wrapper::SendResult UdsClient::async_try_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = try_write_move(std::move(data));
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::try_write_move(std::vector<uint8_t>&& data) {
  std::lock_guard<std::mutex> submission_lock(impl_->submission_mtx_);
  const auto seq = impl_->current_seq_.load();
  const auto connection = impl_->connection_seq_.load();
  if (auto reason = impl_->admission_rejection()) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (auto hook = detail::g_uds_write_admission_hook.load()) hook();
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
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_,
            [self = shared_from_this(), buf = std::move(data), added, seq, connection, request]() mutable {
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

              impl->tx_.push_back(Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request});
              impl->observe_queue();
              impl->report_backpressure(self, impl->queue_bytes_);
              if (!impl->writing_) impl->do_write(self, impl->current_seq_.load());
            });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdsClient::async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = try_write_shared(std::move(data));
  if (auto hook = detail::g_uds_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdsClient::try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
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
  if (auto hook = detail::g_uds_write_admission_hook.load()) hook();
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
  Impl::Ledger::Admission admission(impl_->send_accounting_, added);
  const auto request = admission.request();

  net::post(impl_->strand_,
            [self = shared_from_this(), buf = std::move(data), added, seq, connection, request]() mutable {
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

              impl->tx_.push_back(Impl::TrackedBuffer{BufferVariant{std::move(buf)}, request});
              impl->observe_queue();
              impl->report_backpressure(self, impl->queue_bytes_);
              if (!impl->writing_) impl->do_write(self, impl->current_seq_.load());
            });
  admission.commit();
  return wrapper::SendResult::accept();
}

void UdsClient::on_bytes(OnBytes cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(shared);
}

void UdsClient::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(shared);
}

void UdsClient::on_backpressure(OnBackpressure cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(shared);
}

void UdsClient::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void UdsClient::set_retry_interval(unsigned interval_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.retry_interval_ms = interval_ms;
  impl_->cfg_.validate_and_clamp();
}

void UdsClient::set_reconnect_policy(ReconnectPolicy policy) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  if (policy) {
    impl_->reconnect_policy_ = std::move(policy);
  } else {
    impl_->reconnect_policy_ = std::nullopt;
  }
}

std::optional<diagnostics::ErrorInfo> UdsClient::last_error_info() const {
  return impl_->error_info_holder_.last_error_info();
}

void UdsClient::Impl::do_connect(std::shared_ptr<UdsClient> self, uint64_t seq) {
  if (stop_requested_.load() || stopping_.load() || seq != current_seq_.load()) {
    return;
  }

  if (!cfg_.is_valid()) {
    self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION, "connect",
                              make_error_code(boost::system::errc::invalid_argument), "Invalid UDS socket path", false,
                              self->impl_->reconnect_attempt_count_);
    self->impl_->transition_to(LinkState::Error);
    return;
  }

  std::shared_ptr<uds::endpoint> endpoint;
  try {
    endpoint = std::make_shared<uds::endpoint>(cfg_.socket_path);
  } catch (const std::exception& e) {
    self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION, "connect",
                              make_error_code(boost::system::errc::filename_too_long),
                              "Invalid UDS endpoint: " + std::string(e.what()), false,
                              self->impl_->reconnect_attempt_count_);
    self->impl_->transition_to(LinkState::Error);
    return;
  }

  unsigned connection_timeout_ms;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    connection_timeout_ms = cfg_.connection_timeout_ms;
  }
  connect_timer_.expires_after(std::chrono::milliseconds(connection_timeout_ms));
  connect_timer_.async_wait(track_io(self, [self, seq](const boost::system::error_code& ec) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
    if (!ec) {
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "connect",
                                boost::asio::error::timed_out, "Connection timed out", true,
                                self->impl_->reconnect_attempt_count_);
      self->impl_->handle_close(self, seq, boost::asio::error::timed_out);
    }
  }));

  socket_->async_connect(*endpoint, track_io(self, [self, seq, endpoint](const boost::system::error_code& ec) {
    self->impl_->connect_timer_.cancel();
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
    if (self->impl_->stop_requested_.load() || self->impl_->stopping_.load()) {
      self->impl_->close_socket();
      return;
    }
    if (ec) {
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "connect", ec,
                                "Connect failed: " + ec.message(), diagnostics::is_retryable_uds_connect_error(ec),
                                self->impl_->reconnect_attempt_count_);
      self->impl_->schedule_retry(self, seq);
      return;
    }

    {
      std::lock_guard<std::mutex> lock(self->impl_->submission_mtx_);
      if (self->impl_->stop_requested_.load()) return;
      const auto connection = self->impl_->connection_seq_.fetch_add(1) + 1;
      self->impl_->write_wait_ = std::make_shared<detail::UdsWriteWait>(connection);
      self->impl_->connected_ = true;
    }
    self->impl_->rx_ = std::make_shared<std::vector<uint8_t>>(self->impl_->cfg_.read_buffer_size);
    self->impl_->reconnect_attempt_count_ = 0;
    self->impl_->retry_attempts_ = 0;
    self->impl_->transition_to(LinkState::Connected);
    self->impl_->start_read(self, seq);
    self->impl_->writing_ = false;  // Force reset
    self->impl_->do_write(self, seq);
  }));
}

void UdsClient::Impl::schedule_retry(std::shared_ptr<UdsClient> self, uint64_t seq) {
  transition_to(LinkState::Error);

  // Snapshot once rather than locking repeatedly - cfg_/reconnect_policy_
  // can change concurrently via set_retry_interval() etc. from any user
  // thread while this runs on the strand (#436).
  UdsClientConfig cfg_snapshot;
  std::optional<ReconnectPolicy> reconnect_policy_snapshot;
  {
    std::lock_guard<std::mutex> lock(cfg_mtx_);
    cfg_snapshot = cfg_;
    reconnect_policy_snapshot = reconnect_policy_;
  }

  diagnostics::ErrorInfo dummy_err(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "uds_client",
                                   "connect", "Retry pending", boost::system::error_code(), true);
  auto decision =
      detail::decide_reconnect_uds(cfg_snapshot, dummy_err, reconnect_attempt_count_, reconnect_policy_snapshot);

  if (!decision.should_retry || stop_requested_.load() || stopping_.load()) {
    transition_to(LinkState::Idle);
    return;
  }

  reconnect_attempt_count_++;
  retry_timer_.expires_after(decision.delay.value_or(std::chrono::milliseconds(cfg_snapshot.retry_interval_ms)));
  retry_timer_.async_wait(track_io(self, [self, seq](const boost::system::error_code& ec) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
    self->impl_->do_connect(self, seq);
  }));
}

void UdsClient::Impl::start_read(std::shared_ptr<UdsClient> self, uint64_t seq) {
  if (stop_requested_.load() || seq != current_seq_.load() || !connected_.load()) return;
  const auto connection = connection_seq_.load();
  auto buffer = rx_;
  socket_->async_read_some(net::buffer(*buffer), track_io(self, [self, seq, connection, buffer](
                                                                    const boost::system::error_code& ec, size_t bytes) {
                             auto* impl = self->impl_.get();
                             if (ec == net::error::operation_aborted || seq != impl->current_seq_.load() ||
                                 connection != impl->connection_seq_.load() || impl->stop_requested_.load())
                               return;
                             if (ec) {
                               impl->handle_close(self, seq, ec);
                               return;
                             }
                             interface::SharedCallback<OnBytes> cb;
                             {
                               std::lock_guard<std::mutex> lock(impl->callback_mtx_);
                               cb = impl->on_bytes_;
                             }
                             if (bytes > 0) impl->stats_.record_received(bytes);
                             if (cb) (*cb)(memory::ConstByteSpan(buffer->data(), bytes));
                             impl->start_read(self, seq);
                           }));
}

void UdsClient::Impl::do_write(std::shared_ptr<UdsClient> self, uint64_t seq) {
  if (stop_requested_.load() || seq != current_seq_.load() || !connected_.load() || tx_.empty() || writing_) return;
  std::unique_lock<std::mutex> submission_lock(submission_mtx_);
  if (stop_requested_.load() || seq != current_seq_.load() || !connected_.load()) return;
  writing_ = true;
  auto batch = std::make_shared<WriteBatch>();
  batch->bytes = queue_util::take_gather_batch(tx_, batch->buffers, batch->views, BufferProjection{});
  for (const auto& item : batch->buffers) send_accounting_.begin(item.request);
  active_write_ = batch;
  const auto connection = connection_seq_.load();
  auto completion = track_io(
      self,
      [self, seq, connection, batch](boost::system::error_code ec, size_t written) {
        auto* impl = self->impl_.get();
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
        if (impl->stop_requested_.load() || seq != impl->current_seq_.load()) {
          batch->buffers.clear();
          return;
        }
        if (ec) {
          queue_util::return_gather_batch(impl->tx_, batch->buffers);
          impl->handle_close(self, seq, ec);
          return;
        }
        batch->buffers.clear();
        queue_util::release_reserved_write_bytes(impl->queue_bytes_, batch->bytes);
        impl->stats_.record_sent(written);
        impl->report_backpressure(self, impl->queue_bytes_);
        impl->do_write(self, seq);
      },
      true);
  try {
    socket_->async_write(batch->views, std::move(completion));
  } catch (...) {
    mark_disconnected_locked();
    submission_lock.unlock();
    const auto ec = make_error_code(boost::system::errc::no_buffer_space);
    handle_close(self, seq, ec);
  }
}

void UdsClient::Impl::discard_connection_writes() {
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
  queue_bytes_ = 0;
  pending_bytes_ = 0;
  writing_ = false;
  backpressure_active_ = false;
  if (messages) stats_.record_dropped(messages, bytes);
}

void UdsClient::Impl::handle_close(std::shared_ptr<UdsClient> self, uint64_t seq, const boost::system::error_code&) {
  mark_disconnected();
  discard_connection_writes();
  close_socket();
  retry_timer_.cancel();
  connect_timer_.cancel();

  if (stop_requested_) {
    transition_to(LinkState::Idle);
  } else {
    schedule_retry(self, seq);
  }
}

void UdsClient::Impl::transition_to(LinkState next, const boost::system::error_code&) {
  state_.set(next);
  interface::SharedCallback<OnState> cb;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    cb = on_state_;
  }
  if (cb) (*cb)(next);
}

void UdsClient::Impl::perform_stop_cleanup() {
  retry_timer_.cancel();
  connect_timer_.cancel();
  close_socket();
  tx_.clear();
  queue_bytes_ = 0;
  pending_.clear();
  pending_bytes_ = 0;
  writing_ = false;
  connected_.store(false);
  backpressure_active_.store(false);

  state_.set(LinkState::Idle);
  cleanup_finished_ = true;
  if (pending_io_ == 0) mark_cleanup_done();
}

void UdsClient::Impl::close_socket() {
  boost::system::error_code ec;
  socket_->close(ec);
}

void UdsClient::Impl::recalculate_backpressure_bounds() {
  bp_high_ = cfg_.backpressure_threshold;
  bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
  if (bp_low_ == 0) bp_low_ = 1;
  bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                       base::constants::MAX_BUFFER_SIZE);
  if (bp_limit_ < bp_high_) bp_limit_ = bp_high_;
  backpressure_active_ = false;
}

queue_util::BackpressureFields UdsClient::Impl::bp_fields() {
  return queue_util::BackpressureFields{queue_bytes_,
                                        pending_bytes_,
                                        backpressure_active_,
                                        bp_high_,
                                        bp_low_,
                                        bp_limit_,
                                        bp_strategy_.load(std::memory_order_relaxed)};
}

void UdsClient::Impl::route_enqueued_buffer(std::shared_ptr<UdsClient> self, TrackedBuffer&& buf, size_t added) {
  if (stop_requested_ || stopping_) {
    queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
    return;
  }
  auto f = bp_fields();
  queue_util::DropAccounting dropped;
  auto decision = queue_util::decide_enqueue(
      f, added, tx_, dropped, BufferProjection{},
      [this](const TrackedBuffer& item) { send_accounting_.discard(item.request, Ledger::Cause::QueuePressure); });
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);

  if (decision == queue_util::EnqueueDecision::Rejected) {
    send_accounting_.discard(buf.request, Ledger::Cause::QueuePressure);
    WIRESTEAD_LOG_ERROR("uds_client", "write", fmt::format("Queue limit exceeded ({} bytes)", queue_bytes_ + added));
    // #448: record as dropped so it's reflected in RuntimeStats instead of
    // silently vanishing after being counted as accepted.
    stats_.record_dropped(1, added);
    queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, added);
    report_backpressure(self, queue_bytes_ + added);
    return;
  }
  if (decision == queue_util::EnqueueDecision::Pending) {
    queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, pending_bytes_, inflight_bytes_, added);
    pending_.emplace_back(std::move(buf));
    observe_queue();
    return;
  }
  queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, queue_bytes_, inflight_bytes_, added);
  tx_.emplace_back(std::move(buf));
  observe_queue();
  report_backpressure(self, queue_bytes_);
  if (!writing_) do_write(self, current_seq_.load());
}

void UdsClient::Impl::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void UdsClient::Impl::report_backpressure(std::shared_ptr<UdsClient> self, size_t queued_bytes) {
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

void UdsClient::Impl::record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat,
                                   std::string_view operation, const boost::system::error_code& ec,
                                   std::string_view msg, bool retryable, uint32_t retry_count) {
  error_info_holder_.record_error(lvl, cat, operation, ec, msg, retryable, retry_count);
}

}  // namespace transport
}  // namespace wirestead
