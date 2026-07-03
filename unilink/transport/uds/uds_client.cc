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

#include "unilink/transport/uds/uds_client.hpp"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <deque>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>

#include "unilink/base/constants.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/error_handler.hpp"
#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/uds/boost_uds_socket.hpp"
#include "unilink/transport/uds/detail/reconnect_decider.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

using base::LinkState;
using concurrency::ThreadSafeLinkState;
using config::UdsClientConfig;
using interface::Channel;

struct UdsClient::Impl {
  std::shared_ptr<net::io_context> owned_ioc_;
  net::io_context* ioc_ = nullptr;
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;
  std::atomic<uint64_t> current_seq_{0};
  std::unique_ptr<interface::UdsSocketInterface> socket_;
  // Guards the mutable subset of cfg_ (retry_interval_ms, etc.) and
  // reconnect_policy_ below - see the identical rationale in
  // transport/tcp_client/tcp_client.cc (#436).
  mutable std::mutex cfg_mtx_;
  UdsClientConfig cfg_;
  net::steady_timer retry_timer_;
  net::steady_timer connect_timer_;
  bool owns_ioc_ = true;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};

  std::array<uint8_t, base::constants::DEFAULT_READ_BUFFER_SIZE> rx_{};
  std::deque<BufferVariant> tx_;
  std::deque<BufferVariant> pending_;
  std::atomic<size_t> pending_bytes_{0};
  std::optional<BufferVariant> current_write_buffer_;
  bool writing_ = false;
  std::atomic<size_t> queue_bytes_{0};
  // Atomic rather than mutex-guarded: read both from the strand and from
  // arbitrary caller threads (async_try_write_* fast-fail prechecks) (#436).
  std::atomic<base::constants::BackpressureStrategy> bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_high_;
  size_t bp_low_;
  size_t bp_limit_;
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;

  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_bp_;
  mutable std::mutex callback_mtx_;
  std::atomic<bool> connected_{false};
  ThreadSafeLinkState state_{LinkState::Idle};
  int retry_attempts_ = 0;
  uint32_t reconnect_attempt_count_{0};
  std::optional<ReconnectPolicy> reconnect_policy_;

  mutable std::mutex last_err_mtx_;
  std::optional<diagnostics::ErrorInfo> last_error_info_;

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

  void init() {
    connected_ = false;
    writing_ = false;
    queue_bytes_ = 0;
    pending_bytes_ = 0;
    cfg_.validate_and_clamp();
    recalculate_backpressure_bounds();
  }

  void do_connect(std::shared_ptr<UdsClient> self, uint64_t seq);
  void schedule_retry(std::shared_ptr<UdsClient> self, uint64_t seq);
  void start_read(std::shared_ptr<UdsClient> self, uint64_t seq);
  void do_write(std::shared_ptr<UdsClient> self, uint64_t seq);
  void handle_close(std::shared_ptr<UdsClient> self, uint64_t seq, const boost::system::error_code& ec = {});
  void transition_to(LinkState next, const boost::system::error_code& ec = {});
  void perform_stop_cleanup(uint64_t seq);
  void close_socket();
  void recalculate_backpressure_bounds();
  void maybe_flush_for_keep_latest(size_t added);
  void report_backpressure(std::shared_ptr<UdsClient> self, size_t queued_bytes);
  void observe_queue();
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
  auto current_state = impl_->state_.state();
  if (current_state == LinkState::Connecting || current_state == LinkState::Connected) {
    return;
  }

  impl_->recalculate_backpressure_bounds();
  impl_->stop_requested_ = false;
  impl_->stopping_ = false;
  impl_->current_seq_++;
  uint64_t seq = impl_->current_seq_.load();

  if (impl_->owns_ioc_ && !impl_->ioc_thread_.joinable()) {
    if (impl_->ioc_->stopped()) {
      impl_->ioc_->restart();
    }
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(net::make_work_guard(*impl_->ioc_));
    impl_->ioc_thread_ = std::jthread([ioc = impl_->owned_ioc_](std::stop_token st) {
      try {
        std::stop_callback cb(st, [ioc] { ioc->stop(); });
        ioc->run();
      } catch (...) {
      }
    });
  }

  net::post(impl_->strand_, [self = shared_from_this(), seq]() {
    self->impl_->transition_to(LinkState::Connecting);
    self->impl_->do_connect(self, seq);
  });
}

void UdsClient::stop() {
  bool already_stopping = impl_->stopping_.exchange(true);
  if (already_stopping) return;

  impl_->stop_requested_ = true;
  impl_->connected_ = false;
  const auto seq = impl_->current_seq_.fetch_add(1) + 1;

  // Release work guard and allow io_context to run out of work
  if (impl_->ioc_) {
    if (auto self = weak_from_this().lock()) {
      net::post(impl_->strand_, [self, seq]() { self->impl_->perform_stop_cleanup(seq); });
    } else {
      impl_->perform_stop_cleanup(seq);
    }
  } else {
    impl_->perform_stop_cleanup(seq);
  }

  if (impl_->owns_ioc_ && impl_->ioc_thread_.joinable()) {
    if (std::this_thread::get_id() == impl_->ioc_thread_.get_id()) {
      impl_->ioc_thread_.detach();
    } else {
      impl_->ioc_thread_.join();
    }
  }

  // Transition to Idle state (lock-free or safe call)
  impl_->state_.set_state(LinkState::Idle);
}

bool UdsClient::is_connected() const { return impl_->connected_.load(); }
bool UdsClient::is_backpressure_active() const { return impl_->backpressure_active_.load(); }
wrapper::RuntimeStats UdsClient::stats() const {
  return impl_->stats_.snapshot(impl_->queue_bytes_.load(std::memory_order_relaxed),
                                impl_->pending_bytes_.load(std::memory_order_relaxed),
                                impl_->backpressure_active_.load(std::memory_order_relaxed));
}
void UdsClient::reset_stats() {
  impl_->stats_.reset(impl_->queue_bytes_.load(std::memory_order_relaxed) +
                      impl_->pending_bytes_.load(std::memory_order_relaxed));
}

boost::asio::any_io_executor UdsClient::get_executor() { return impl_->strand_; }

bool UdsClient::async_write_copy(memory::ConstByteSpan data) {
  std::vector<uint8_t> vec(data.begin(), data.end());
  return async_write_move(std::move(vec));
}

bool UdsClient::async_write_move(std::vector<uint8_t>&& data) {
  if (!impl_->connected_.load() || impl_->stop_requested_.load()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (data.empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (impl_->queue_bytes_ + impl_->pending_bytes_ + data.size() > impl_->bp_limit_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  impl_->stats_.record_accepted(data.size());
  net::post(impl_->strand_, [this, self = shared_from_this(), data = std::move(data)]() mutable {
    size_t added = data.size();

    // Reliable: route to pending_ when backpressure is active
    if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable && impl_->backpressure_active_.load()) {
      if (impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
        UNILINK_LOG_ERROR("uds_client", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", impl_->queue_bytes_ + added));
        return;
      }
      impl_->pending_bytes_ += added;
      impl_->pending_.emplace_back(std::move(data));
      impl_->observe_queue();
      return;
    }

    impl_->maybe_flush_for_keep_latest(added);
    if (impl_->queue_bytes_ + added > impl_->bp_limit_) {
      UNILINK_LOG_ERROR("uds_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", impl_->queue_bytes_ + added));
      impl_->report_backpressure(self, impl_->queue_bytes_ + added);
      return;
    }
    impl_->queue_bytes_ += added;
    impl_->tx_.emplace_back(std::move(data));
    impl_->observe_queue();
    impl_->report_backpressure(self, impl_->queue_bytes_);
    if (!impl_->writing_) {
      impl_->do_write(self, impl_->current_seq_.load());
    }
  });
  return true;
}

bool UdsClient::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!impl_->connected_.load() || impl_->stop_requested_.load() || !data || data->empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (impl_->queue_bytes_ + impl_->pending_bytes_ + data->size() > impl_->bp_limit_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  impl_->stats_.record_accepted(data->size());
  net::post(impl_->strand_, [this, self = shared_from_this(), data = std::move(data)]() mutable {
    size_t added = data->size();

    // Reliable: route to pending_ when backpressure is active
    if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable && impl_->backpressure_active_.load()) {
      if (impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
        UNILINK_LOG_ERROR("uds_client", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", impl_->queue_bytes_ + added));
        return;
      }
      impl_->pending_bytes_ += added;
      impl_->pending_.emplace_back(std::move(data));
      impl_->observe_queue();
      return;
    }

    impl_->maybe_flush_for_keep_latest(added);
    if (impl_->queue_bytes_ + added > impl_->bp_limit_) {
      UNILINK_LOG_ERROR("uds_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", impl_->queue_bytes_ + added));
      impl_->report_backpressure(self, impl_->queue_bytes_ + added);
      return;
    }
    impl_->queue_bytes_ += added;
    impl_->tx_.emplace_back(std::move(data));
    impl_->observe_queue();
    impl_->report_backpressure(self, impl_->queue_bytes_);
    if (!impl_->writing_) impl_->do_write(self, impl_->current_seq_.load());
  });
  return true;
}

bool UdsClient::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty() || data.size() > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool UdsClient::async_try_write_move(std::vector<uint8_t>&& data) {
  if (!impl_->connected_.load() || impl_->stop_requested_.load()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  const auto added = data.size();
  if (added == 0 || added > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return false;
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
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl_->queue_bytes_, impl_->pending_bytes_, impl_->backpressure_active_,
                                           added, impl_->bp_high_, impl_->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl_->stats_.record_accepted(added);

  net::post(impl_->strand_, [this, self = shared_from_this(), data = std::move(data), added]() mutable {
    if (!impl_->connected_.load() || impl_->stop_requested_.load()) {
      queue_util::release_reserved_write_bytes(impl_->queue_bytes_, added);
      impl_->stats_.record_failed_send();
      return;
    }

    impl_->tx_.emplace_back(std::move(data));
    impl_->observe_queue();
    impl_->report_backpressure(self, impl_->queue_bytes_);
    if (!impl_->writing_) impl_->do_write(self, impl_->current_seq_.load());
  });
  return true;
}

bool UdsClient::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!impl_->connected_.load() || impl_->stop_requested_.load() || !data || data->empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  if (added > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return false;
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
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl_->queue_bytes_, impl_->pending_bytes_, impl_->backpressure_active_,
                                           added, impl_->bp_high_, impl_->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl_->stats_.record_accepted(added);

  net::post(impl_->strand_, [this, self = shared_from_this(), data = std::move(data), added]() mutable {
    if (!impl_->connected_.load() || impl_->stop_requested_.load()) {
      queue_util::release_reserved_write_bytes(impl_->queue_bytes_, added);
      impl_->stats_.record_failed_send();
      return;
    }

    impl_->tx_.emplace_back(std::move(data));
    impl_->observe_queue();
    impl_->report_backpressure(self, impl_->queue_bytes_);
    if (!impl_->writing_) impl_->do_write(self, impl_->current_seq_.load());
  });
  return true;
}

void UdsClient::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(cb);
}

void UdsClient::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(cb);
}

void UdsClient::on_backpressure(OnBackpressure cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(cb);
}

void UdsClient::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void UdsClient::set_retry_interval(unsigned interval_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.retry_interval_ms = interval_ms;
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
  std::lock_guard<std::mutex> lock(impl_->last_err_mtx_);
  return impl_->last_error_info_;
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
  connect_timer_.async_wait(net::bind_executor(strand_, [self, seq](const boost::system::error_code& ec) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
    if (!ec) {
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "connect",
                                boost::asio::error::timed_out, "Connection timed out", true,
                                self->impl_->reconnect_attempt_count_);
      self->impl_->handle_close(self, seq, boost::asio::error::timed_out);
    }
  }));

  socket_->async_connect(*endpoint, net::bind_executor(strand_, [self, seq,
                                                                 endpoint](const boost::system::error_code& ec) {
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

    self->impl_->connected_ = true;
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
  retry_timer_.async_wait(net::bind_executor(strand_, [self, seq](const boost::system::error_code& ec) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
    self->impl_->do_connect(self, seq);
  }));
}

void UdsClient::Impl::start_read(std::shared_ptr<UdsClient> self, uint64_t seq) {
  if (stop_requested_.load() || stopping_.load() || seq != current_seq_.load()) {
    return;
  }

  socket_->async_read_some(net::buffer(rx_),
                           net::bind_executor(strand_, [self, seq](const boost::system::error_code& ec, size_t bytes) {
                             if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
                             if (self->impl_->stop_requested_.load() || self->impl_->stopping_.load()) return;
                             if (ec) {
                               self->impl_->handle_close(self, seq, ec);
                               return;
                             }

                             OnBytes cb;
                             {
                               std::lock_guard<std::mutex> lock(self->impl_->callback_mtx_);
                               cb = self->impl_->on_bytes_;
                             }
                             if (bytes > 0) self->impl_->stats_.record_received(bytes);
                             if (cb) cb(memory::ConstByteSpan(self->impl_->rx_.data(), bytes));
                             self->impl_->start_read(self, seq);
                           }));
}

void UdsClient::Impl::do_write(std::shared_ptr<UdsClient> self, uint64_t seq) {
  if (stop_requested_.load() || stopping_.load() || seq != current_seq_.load()) {
    tx_.clear();
    pending_.clear();
    queue_bytes_ = 0;
    pending_bytes_ = 0;
    current_write_buffer_ = std::nullopt;
    writing_ = false;
    return;
  }

  if (tx_.empty() || writing_) return;
  writing_ = true;
  current_write_buffer_ = std::move(tx_.front());
  tx_.pop_front();

  net::const_buffer buffer;
  std::visit(
      [&buffer](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
          buffer = net::buffer(arg);
        else if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>)
          buffer = net::buffer(*arg);
        else if constexpr (std::is_same_v<T, memory::PooledBuffer>)
          buffer = net::buffer(arg.data(), arg.size());
      },
      *current_write_buffer_);

  size_t bytes_to_write = buffer.size();
  socket_->async_write(
      buffer,
      net::bind_executor(strand_, [self, seq, bytes_to_write](const boost::system::error_code& ec, size_t written) {
        if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) return;
        if (self->impl_->stop_requested_.load() || self->impl_->stopping_.load()) {
          self->impl_->current_write_buffer_ = std::nullopt;
          self->impl_->writing_ = false;
          return;
        }
        self->impl_->writing_ = false;
        self->impl_->current_write_buffer_ = std::nullopt;
        self->impl_->queue_bytes_ =
            (self->impl_->queue_bytes_ >= bytes_to_write) ? (self->impl_->queue_bytes_ - bytes_to_write) : 0;
        self->impl_->report_backpressure(self, self->impl_->queue_bytes_);

        if (ec) {
          self->impl_->handle_close(self, seq, ec);
          return;
        }
        self->impl_->stats_.record_sent(written);
        if (!self->impl_->tx_.empty()) self->impl_->do_write(self, seq);
      }));
}

void UdsClient::Impl::handle_close(std::shared_ptr<UdsClient> self, uint64_t seq, const boost::system::error_code&) {
  connected_ = false;
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
  state_.set_state(next);
  OnState cb;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    cb = on_state_;
  }
  if (cb) cb(next);
}

void UdsClient::Impl::perform_stop_cleanup(uint64_t seq) {
  if (seq != current_seq_.load()) {
    return;
  }

  retry_timer_.cancel();
  connect_timer_.cancel();
  close_socket();
  tx_.clear();
  queue_bytes_ = 0;
  pending_.clear();
  pending_bytes_ = 0;
  current_write_buffer_ = std::nullopt;
  writing_ = false;
  connected_.store(false);
  backpressure_active_.store(false);

  if (owns_ioc_ && work_guard_) {
    work_guard_.reset();
  }

  state_.set_state(LinkState::Idle);
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

void UdsClient::Impl::maybe_flush_for_keep_latest(size_t added) {
  const auto dropped =
      queue_util::maybe_flush_for_keep_latest(bp_strategy_, added, bp_high_, tx_, queue_bytes_, backpressure_active_);
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);
}

void UdsClient::Impl::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void UdsClient::Impl::report_backpressure(std::shared_ptr<UdsClient> self, size_t queued_bytes) {
  if (stop_requested_.load() || stopping_.load()) return;
  observe_queue();

  OnBackpressure on_bp;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    on_bp = on_bp_;
  }

  if (!backpressure_active_ && queued_bytes >= bp_high_) {
    backpressure_active_ = true;
    stats_.record_backpressure_event();
    if (on_bp) {
      try {
        on_bp(queued_bytes);
      } catch (const std::exception& e) {
        UNILINK_LOG_ERROR("uds_client", "on_backpressure",
                          fmt::format("Exception in backpressure callback: {}", e.what()));
      } catch (...) {
        UNILINK_LOG_ERROR("uds_client", "on_backpressure", "Unknown exception in backpressure callback");
      }
    }
  } else if (backpressure_active_ && queued_bytes <= bp_low_) {
    // Flush pending_ → tx_
    const size_t moved = pending_bytes_.exchange(0);
    queue_bytes_ += moved;
    while (!pending_.empty()) {
      tx_.emplace_back(std::move(pending_.front()));
      pending_.pop_front();
    }
    observe_queue();
    backpressure_active_ = false;
    stats_.record_backpressure_event();
    if (on_bp) {
      try {
        on_bp(queued_bytes);
      } catch (...) {
      }  // fire OFF with pre-flush queue size
    }
    // If post-flush queue is still high, fire ON again
    if (queue_bytes_ >= bp_high_) {
      backpressure_active_ = true;
      stats_.record_backpressure_event();
      if (on_bp) {
        try {
          on_bp(queue_bytes_);
        } catch (...) {
        }
      }
    }
    if (!writing_) do_write(self, current_seq_.load());
  }
}

void UdsClient::Impl::record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat,
                                   std::string_view operation, const boost::system::error_code& ec,
                                   std::string_view msg, bool retryable, uint32_t) {
  std::lock_guard<std::mutex> lock(last_err_mtx_);
  last_error_info_ = diagnostics::ErrorInfo(lvl, cat, "uds_client", operation, msg, ec, retryable);
}

}  // namespace transport
}  // namespace unilink
