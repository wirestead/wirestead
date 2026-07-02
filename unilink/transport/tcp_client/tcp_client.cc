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

#include "unilink/transport/tcp_client/tcp_client.hpp"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
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

#include "unilink/base/constants.hpp"
#include "unilink/concurrency/io_context_manager.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/error_handler.hpp"
#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/tcp_client/detail/reconnect_decider.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

using base::LinkState;
using concurrency::ThreadSafeLinkState;
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
  tcp::resolver resolver_;
  tcp::socket socket_;
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
  net::steady_timer retry_timer_;
  net::steady_timer connect_timer_;
  net::steady_timer idle_timer_;
  bool owns_ioc_ = true;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> terminal_state_notified_{false};
  std::atomic<bool> reconnect_pending_{false};

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
  unsigned first_retry_interval_ms_ = 100;

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

  void init() {
    connected_ = false;
    writing_ = false;
    queue_bytes_ = 0;
    pending_bytes_ = 0;
    cfg_.validate_and_clamp();
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
  void maybe_flush_for_keep_latest(size_t added);
  void report_backpressure(std::shared_ptr<TcpClient> self, size_t queued_bytes);
  void observe_queue();
  void notify_state();
  void reset_io_objects();
  void apply_socket_options();
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
  stop();
  impl_->join_ioc_thread(true);

  impl_->on_bytes_ = nullptr;
  impl_->on_state_ = nullptr;
  impl_->on_bp_ = nullptr;
}

TcpClient::TcpClient(TcpClient&&) noexcept = default;
TcpClient& TcpClient::operator=(TcpClient&&) noexcept = default;

std::optional<diagnostics::ErrorInfo> TcpClient::last_error_info() const {
  std::lock_guard<std::mutex> lock(impl_->last_err_mtx_);
  return impl_->last_error_info_;
}

void TcpClient::start() {
  auto current_state = impl_->state_.state();
  if (current_state == LinkState::Connecting || current_state == LinkState::Connected) {
    UNILINK_LOG_DEBUG("tcp_client", "start", "Start called while already active, ignoring");
    return;
  }

  if (!impl_->ioc_) {
    UNILINK_LOG_ERROR("tcp_client", "start", "io_context is null");
  }

  impl_->recalculate_backpressure_bounds();

  if (impl_->ioc_ && impl_->ioc_->stopped()) {
    UNILINK_LOG_DEBUG("tcp_client", "start", "io_context stopped; restarting before start");
    impl_->ioc_->restart();
  }

  if (impl_->ioc_thread_.joinable()) {
    impl_->join_ioc_thread(false);
  }

  const auto seq = impl_->lifecycle_seq_.fetch_add(1) + 1;
  impl_->current_seq_.store(seq);

  if (impl_->owns_ioc_ && impl_->ioc_) {
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl_->ioc_->get_executor());
    impl_->ioc_thread_ = std::jthread([ioc = impl_->owned_ioc_](std::stop_token st) {
      try {
        std::stop_callback cb(st, [ioc] { ioc->stop(); });
        ioc->run();
      } catch (const std::exception& e) {
        UNILINK_LOG_ERROR("tcp_client", "io_context", fmt::format("IO context error: {}", e.what()));
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
        self->impl_->reset_start_state();
        self->impl_->connected_.store(false);
        self->impl_->reset_io_objects();
        self->impl_->transition_to(LinkState::Connecting);
        self->impl_->do_resolve_connect(self, seq);
      }
    });
  } else {
    UNILINK_LOG_ERROR("tcp_client", "start", "io_context is null");
  }
}

void TcpClient::stop() {
  if (impl_->stop_requested_.exchange(true)) {
    return;
  }

  impl_->stopping_.store(true);
  impl_->stop_seq_.store(impl_->current_seq_.load());
  auto weak_self = weak_from_this();
  if (!impl_->ioc_) {
    return;
  }

  if (auto self = weak_self.lock()) {
    net::post(impl_->strand_, [self]() { self->impl_->perform_stop_cleanup(); });
  }

  impl_->join_ioc_thread(false);
}

bool TcpClient::is_connected() const { return get_impl()->connected_.load(); }
bool TcpClient::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
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

bool TcpClient::async_write_copy(memory::ConstByteSpan data) {
  if (impl_->stop_requested_.load() || impl_->state_.is_state(LinkState::Closed) ||
      impl_->state_.is_state(LinkState::Error) || !impl_->ioc_) {
    impl_->stats_.record_failed_send();
    return false;
  }

  size_t size = data.size();
  if (size == 0) {
    UNILINK_LOG_WARNING("tcp_client", "async_write_copy", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return false;
  }

  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_client", "async_write_copy",
                      fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return false;
  }

  if (size <= 65536) {
    try {
      memory::PooledBuffer pooled_buffer(size);
      if (pooled_buffer.valid()) {
        base::safe_memory::safe_memcpy(pooled_buffer.data(), data.data(), size);
        const auto added = pooled_buffer.size();
        if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
            impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
          impl_->stats_.record_failed_send();
          return false;
        }
        impl_->stats_.record_accepted(added);
        net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(pooled_buffer), added]() mutable {
          if (self->impl_->stop_requested_.load() || self->impl_->state_.is_state(LinkState::Closed) ||
              self->impl_->state_.is_state(LinkState::Error)) {
            return;
          }

          // Reliable: route to pending_ when backpressure is active
          if (self->impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
              self->impl_->backpressure_active_.load()) {
            if (self->impl_->queue_bytes_ + self->impl_->pending_bytes_ + added > self->impl_->bp_limit_) {
              UNILINK_LOG_ERROR("tcp_client", "write",
                                fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
              return;
            }
            self->impl_->pending_bytes_ += added;
            self->impl_->pending_.emplace_back(std::move(buf));
            self->impl_->observe_queue();
            return;
          }

          self->impl_->maybe_flush_for_keep_latest(added);

          if (self->impl_->queue_bytes_ + added > self->impl_->bp_limit_) {
            UNILINK_LOG_ERROR("tcp_client", "write",
                              fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
            self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION,
                                      "write", boost::asio::error::no_buffer_space, "Queue limit exceeded", false, 0);
            self->impl_->report_backpressure(self, self->impl_->queue_bytes_ + added);
            return;
          }

          self->impl_->queue_bytes_ += added;
          self->impl_->tx_.emplace_back(std::move(buf));
          self->impl_->observe_queue();
          self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
          if (!self->impl_->writing_) self->impl_->do_write(self, self->impl_->current_seq_.load());
        });
        return true;
      }
    } catch (const std::exception& e) {
      UNILINK_LOG_ERROR("tcp_client", "async_write_copy", fmt::format("Failed to acquire pooled buffer: {}", e.what()));
    }
  }

  std::vector<uint8_t> fallback(data.begin(), data.end());
  const auto added = fallback.size();
  impl_->stats_.record_accepted(added);

  net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(fallback), added]() mutable {
    if (self->impl_->stop_requested_.load() || self->impl_->state_.is_state(LinkState::Closed) ||
        self->impl_->state_.is_state(LinkState::Error)) {
      return;
    }

    // Reliable: route to pending_ when backpressure is active
    if (self->impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
        self->impl_->backpressure_active_.load()) {
      if (self->impl_->queue_bytes_ + self->impl_->pending_bytes_ + added > self->impl_->bp_limit_) {
        UNILINK_LOG_ERROR("tcp_client", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
        return;
      }
      self->impl_->pending_bytes_ += added;
      self->impl_->pending_.emplace_back(std::move(buf));
      self->impl_->observe_queue();
      return;
    }

    self->impl_->maybe_flush_for_keep_latest(added);

    if (self->impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
        self->impl_->queue_bytes_ + self->impl_->pending_bytes_ + added > self->impl_->bp_limit_) {
      UNILINK_LOG_ERROR("tcp_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
      return;
    }

    if (self->impl_->queue_bytes_ + added > self->impl_->bp_limit_) {
      UNILINK_LOG_ERROR("tcp_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write",
                                boost::asio::error::no_buffer_space, "Queue limit exceeded", false, 0);
      self->impl_->report_backpressure(self, self->impl_->queue_bytes_ + added);
      return;
    }

    self->impl_->queue_bytes_ += added;
    self->impl_->tx_.emplace_back(std::move(buf));
    self->impl_->observe_queue();
    self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
    if (!self->impl_->writing_) self->impl_->do_write(self, self->impl_->current_seq_.load());
  });
  return true;
}

bool TcpClient::async_write_move(std::vector<uint8_t>&& data) {
  if (impl_->stop_requested_.load() || impl_->state_.is_state(LinkState::Closed) ||
      impl_->state_.is_state(LinkState::Error) || !impl_->ioc_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  const auto size = data.size();
  if (size == 0) {
    UNILINK_LOG_WARNING("tcp_client", "async_write_move", "Ignoring zero-length write");
    impl_->stats_.record_failed_send();
    return false;
  }
  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_client", "async_write_move",
                      fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return false;
  }

  const auto added = size;
  if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
      impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  impl_->stats_.record_accepted(added);
  net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (self->impl_->stop_requested_.load() || self->impl_->state_.is_state(LinkState::Closed) ||
        self->impl_->state_.is_state(LinkState::Error)) {
      return;
    }

    // Reliable: route to pending_ when backpressure is active
    if (self->impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
        self->impl_->backpressure_active_.load()) {
      if (self->impl_->queue_bytes_ + self->impl_->pending_bytes_ + added > self->impl_->bp_limit_) {
        UNILINK_LOG_ERROR("tcp_client", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
        return;
      }
      self->impl_->pending_bytes_ += added;
      self->impl_->pending_.emplace_back(std::move(buf));
      self->impl_->observe_queue();
      return;
    }

    self->impl_->maybe_flush_for_keep_latest(added);

    if (self->impl_->queue_bytes_ + added > self->impl_->bp_limit_) {
      UNILINK_LOG_ERROR("tcp_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write",
                                boost::asio::error::no_buffer_space, "Queue limit exceeded", false, 0);
      self->impl_->report_backpressure(self, self->impl_->queue_bytes_ + added);
      return;
    }

    self->impl_->queue_bytes_ += added;
    self->impl_->tx_.emplace_back(std::move(buf));
    self->impl_->observe_queue();
    self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
    if (!self->impl_->writing_) self->impl_->do_write(self, self->impl_->current_seq_.load());
  });
  return true;
}

bool TcpClient::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (impl_->stop_requested_.load() || impl_->state_.is_state(LinkState::Closed) ||
      impl_->state_.is_state(LinkState::Error) || !impl_->ioc_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (!data || data->empty()) {
    UNILINK_LOG_WARNING("tcp_client", "async_write_shared", "Ignoring empty shared buffer");
    impl_->stats_.record_failed_send();
    return false;
  }
  const auto size = data->size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_client", "async_write_shared",
                      fmt::format("Write size exceeds maximum allowed ({} bytes)", size));
    impl_->stats_.record_failed_send();
    return false;
  }

  const auto added = size;
  if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
      impl_->queue_bytes_ + impl_->pending_bytes_ + added > impl_->bp_limit_) {
    impl_->stats_.record_failed_send();
    return false;
  }
  impl_->stats_.record_accepted(added);
  net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (self->impl_->stop_requested_.load() || self->impl_->state_.is_state(LinkState::Closed) ||
        self->impl_->state_.is_state(LinkState::Error)) {
      return;
    }

    // Reliable: route to pending_ when backpressure is active
    if (self->impl_->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
        self->impl_->backpressure_active_.load()) {
      if (self->impl_->queue_bytes_ + self->impl_->pending_bytes_ + added > self->impl_->bp_limit_) {
        UNILINK_LOG_ERROR("tcp_client", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
        return;
      }
      self->impl_->pending_bytes_ += added;
      self->impl_->pending_.emplace_back(std::move(buf));
      self->impl_->observe_queue();
      return;
    }

    self->impl_->maybe_flush_for_keep_latest(added);

    if (self->impl_->queue_bytes_ + added > self->impl_->bp_limit_) {
      UNILINK_LOG_ERROR("tcp_client", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", self->impl_->queue_bytes_ + added));
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write",
                                boost::asio::error::no_buffer_space, "Queue limit exceeded", false, 0);
      self->impl_->report_backpressure(self, self->impl_->queue_bytes_ + added);
      return;
    }

    self->impl_->queue_bytes_ += added;
    self->impl_->tx_.emplace_back(std::move(buf));
    self->impl_->observe_queue();
    self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
    if (!self->impl_->writing_) self->impl_->do_write(self, self->impl_->current_seq_.load());
  });
  return true;
}

bool TcpClient::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (data.size() > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool TcpClient::async_try_write_move(std::vector<uint8_t>&& data) {
  if (impl_->stop_requested_.load() || impl_->state_.is_state(LinkState::Closed) ||
      impl_->state_.is_state(LinkState::Error) || !impl_->ioc_) {
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

  net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
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
  return true;
}

bool TcpClient::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!data || data->empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  if (data->size() > base::constants::MAX_BUFFER_SIZE) {
    impl_->stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  const auto reject_for_pressure = [this, added]() {
    if (impl_->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl_->stats_.record_dropped(1, added);
    } else {
      impl_->stats_.record_failed_send();
    }
  };
  if (impl_->stop_requested_.load() || impl_->state_.is_state(LinkState::Closed) ||
      impl_->state_.is_state(LinkState::Error) || !impl_->ioc_) {
    impl_->stats_.record_failed_send();
    return false;
  }
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

  net::dispatch(impl_->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
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
  return true;
}

void TcpClient::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(cb);
}
void TcpClient::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(cb);
}
void TcpClient::on_backpressure(OnBackpressure cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(cb);
}
void TcpClient::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void TcpClient::set_retry_interval(unsigned interval_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.retry_interval_ms = interval_ms;
}
void TcpClient::set_max_retries(int max_retries) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.max_retries = max_retries;
}
void TcpClient::set_connection_timeout(unsigned timeout_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.connection_timeout_ms = timeout_ms;
}
void TcpClient::set_idle_timeout(unsigned timeout_ms) {
  std::lock_guard<std::mutex> lock(impl_->cfg_mtx_);
  impl_->cfg_.idle_timeout_ms = timeout_ms;
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
      UNILINK_LOG_WARNING("tcp_client", "socket_options", fmt::format("Failed to set TCP_NODELAY: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.keep_alive) {
    socket_.set_option(net::socket_base::keep_alive(true), ec);
    if (ec) {
      UNILINK_LOG_WARNING("tcp_client", "socket_options", fmt::format("Failed to set keep_alive: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.send_buffer_size > 0) {
    socket_.set_option(net::socket_base::send_buffer_size(static_cast<int>(cfg_.send_buffer_size)), ec);
    if (ec) {
      UNILINK_LOG_WARNING("tcp_client", "socket_options",
                          fmt::format("Failed to set send buffer size: {}", ec.message()));
      ec.clear();
    }
  }

  if (cfg_.receive_buffer_size > 0) {
    socket_.set_option(net::socket_base::receive_buffer_size(static_cast<int>(cfg_.receive_buffer_size)), ec);
    if (ec) {
      UNILINK_LOG_WARNING("tcp_client", "socket_options",
                          fmt::format("Failed to set receive buffer size: {}", ec.message()));
      ec.clear();
    }
  }
}

void TcpClient::Impl::do_resolve_connect(std::shared_ptr<TcpClient> self, uint64_t seq) {
  resolver_.async_resolve(
      cfg_.host, fmt::format("{}", cfg_.port), [self, seq](auto ec, tcp::resolver::results_type results) {
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
        self->impl_->connect_timer_.async_wait([self, seq](const boost::system::error_code& timer_ec) {
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
            UNILINK_LOG_ERROR("tcp_client", "connect_timeout",
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

        net::async_connect(self->impl_->socket_, results, [self, seq](auto ec2, const auto&) {
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
          self->impl_->connected_.store(true);

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
          int yes = 1;
          (void)::setsockopt(static_cast<int>(self->impl_->socket_.native_handle()), SOL_SOCKET, SO_NOSIGPIPE, &yes,
                             static_cast<socklen_t>(sizeof(yes)));
#endif

          self->impl_->apply_socket_options();
          self->impl_->transition_to(LinkState::Connected);
          boost::system::error_code ep_ec;
          auto rep = self->impl_->socket_.remote_endpoint(ep_ec);
          if (!ep_ec) {
            UNILINK_LOG_INFO("tcp_client", "connect",
                             fmt::format("Connected to {}:{}", rep.address().to_string(), rep.port()));
          }
          self->impl_->start_read(self, seq);
          self->impl_->reset_idle_timer(self, seq);
          net::post(self->impl_->strand_, [self, seq]() {
            self->impl_->writing_ = false;
            self->impl_->do_write(self, seq);
          });
        });
      });
}

void TcpClient::Impl::schedule_retry(std::shared_ptr<TcpClient> self, uint64_t seq) {
  connected_.store(false);
  if (stop_requested_.load() || stopping_.load()) {
    return;
  }

  // Prevent double scheduling of reconnect
  if (reconnect_pending_.exchange(true)) {
    return;
  }

  std::optional<diagnostics::ErrorInfo> last_err;
  {
    std::lock_guard<std::mutex> lock(last_err_mtx_);
    last_err = last_error_info_;
  }

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
    UNILINK_LOG_INFO("tcp_client", "retry", "Reconnect stopped by policy/config");
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

  UNILINK_LOG_INFO("tcp_client", "retry",
                   fmt::format("Scheduling retry in {:.3f}s", static_cast<double>(delay.count()) / 1000.0));

  retry_timer_.expires_after(delay);
  retry_timer_.async_wait([self, seq](const boost::system::error_code& ec) {
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
  socket_.async_read_some(net::buffer(rx_.data(), rx_.size()), [self, seq](auto ec, std::size_t n) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
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
    OnBytes on_bytes;
    {
      std::lock_guard<std::mutex> lock(self->impl_->callback_mtx_);
      on_bytes = self->impl_->on_bytes_;
    }

    self->impl_->stats_.record_received(n);

    if (on_bytes) {
      try {
        on_bytes(memory::ConstByteSpan(self->impl_->rx_.data(), n));
      } catch (const std::exception& e) {
        UNILINK_LOG_ERROR("tcp_client", "on_bytes", fmt::format("Exception in on_bytes callback: {}", e.what()));
        self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "on_bytes",
                                  boost::asio::error::connection_aborted,
                                  fmt::format("Exception in on_bytes: {}", e.what()), false, 0);
        self->impl_->handle_close(self, seq, make_error_code(boost::asio::error::connection_aborted));
        return;
      } catch (...) {
        UNILINK_LOG_ERROR("tcp_client", "on_bytes", "Unknown exception in on_bytes callback");
        self->impl_->handle_close(self, seq, make_error_code(boost::asio::error::connection_aborted));
        return;
      }
    }
    self->impl_->start_read(self, seq);
  });
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

  current_write_buffer_ = std::move(tx_.front());
  tx_.pop_front();

  auto& current = *current_write_buffer_;

  auto queued_bytes = std::visit(
      [](auto&& buf) -> size_t {
        using Buffer = std::decay_t<decltype(buf)>;
        if constexpr (std::is_same_v<Buffer, std::shared_ptr<const std::vector<uint8_t>>>) {
          return buf ? buf->size() : 0;
        } else {
          return buf.size();
        }
      },
      current);

  auto on_write = [self, queued_bytes, seq](auto ec, std::size_t bytes_written) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
      self->impl_->current_write_buffer_.reset();
      self->impl_->queue_bytes_ =
          (self->impl_->queue_bytes_ > queued_bytes) ? (self->impl_->queue_bytes_ - queued_bytes) : 0;
      self->impl_->report_backpressure(self, self->impl_->queue_bytes_);
      self->impl_->writing_ = false;
      return;
    }

    if (ec) {
      if (self->impl_->current_write_buffer_) {
        self->impl_->tx_.push_front(std::move(*self->impl_->current_write_buffer_));
      }
      self->impl_->current_write_buffer_.reset();

      UNILINK_LOG_ERROR("tcp_client", "do_write", fmt::format("Write failed: {}", ec.message()));
      self->impl_->record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::COMMUNICATION, "write", ec,
                                fmt::format("Write failed: {}", ec.message()), false, 0);
      self->impl_->writing_ = false;
      self->impl_->handle_close(self, seq, ec);
      return;
    }

    self->impl_->current_write_buffer_.reset();
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

  std::visit(
      [&](const auto& buf) {
        using T = std::decay_t<decltype(buf)>;
        if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
          net::async_write(socket_, net::buffer(buf->data(), buf->size()), on_write);
        } else {
          net::async_write(socket_, net::buffer(buf.data(), buf.size()), on_write);
        }
      },
      current);
}

void TcpClient::Impl::handle_close(std::shared_ptr<TcpClient> self, uint64_t seq, const boost::system::error_code& ec) {
  if (ec == net::error::operation_aborted || seq != current_seq_.load()) {
    return;
  }
  UNILINK_LOG_INFO("tcp_client", "handle_close", fmt::format("Closing connection. Error: {}", ec.message()));
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
  connected_.store(false);
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

  UNILINK_LOG_WARNING("tcp_client", "idle_timeout",
                      fmt::format("Idle timeout expired after {}ms; {}", idle_timeout_ms,
                                  should_reconnect ? "scheduling reconnect" : "closing connection"));
  record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "idle_timeout", ec,
               "Idle timeout expired", should_reconnect, current_attempts);

  connected_.store(false);
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

void TcpClient::Impl::close_socket() {
  boost::system::error_code ec;
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

void TcpClient::Impl::maybe_flush_for_keep_latest(size_t added) {
  const auto dropped =
      queue_util::maybe_flush_for_keep_latest(bp_strategy_, added, bp_high_, tx_, queue_bytes_, backpressure_active_);
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);
}

void TcpClient::Impl::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void TcpClient::Impl::report_backpressure(std::shared_ptr<TcpClient> self, size_t queued_bytes) {
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
        UNILINK_LOG_ERROR("tcp_client", "on_backpressure",
                          fmt::format("Exception in backpressure callback: {}", e.what()));
      } catch (...) {
        UNILINK_LOG_ERROR("tcp_client", "on_backpressure", "Unknown exception in backpressure callback");
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

void TcpClient::Impl::transition_to(LinkState next, const boost::system::error_code& ec) {
  if (ec == net::error::operation_aborted) {
    return;
  }

  const auto current = state_.state();
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

  state_.set_state(next);
  notify_state();
}

void TcpClient::Impl::perform_stop_cleanup() {
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
    connected_.store(false);
    backpressure_active_ = false;

    if (owns_ioc_ && work_guard_) {
      work_guard_->reset();
    }
    transition_to(LinkState::Closed);
  } catch (const std::exception& e) {
    UNILINK_LOG_ERROR("tcp_client", "stop_cleanup", fmt::format("Cleanup error: {}", e.what()));
    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "stop_cleanup", {},
                 fmt::format("Cleanup error: {}", e.what()), false, 0);
    diagnostics::error_reporting::report_system_error("tcp_client", "stop_cleanup",
                                                      fmt::format("Exception in stop cleanup: {}", e.what()));
  } catch (...) {
    UNILINK_LOG_ERROR("tcp_client", "stop_cleanup", "Unknown error in stop cleanup");
    diagnostics::error_reporting::report_system_error("tcp_client", "stop_cleanup", "Unknown error in stop cleanup");
  }
}

void TcpClient::Impl::reset_start_state() {
  stop_requested_.store(false);
  stopping_.store(false);
  terminal_state_notified_.store(false);
  reconnect_pending_.store(false);
  retry_attempts_ = 0;
  reconnect_attempt_count_ = 0;
  connected_.store(false);
  writing_ = false;
  queue_bytes_ = 0;
  pending_.clear();
  pending_bytes_ = 0;
  backpressure_active_ = false;
  state_.set_state(LinkState::Idle);
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
    UNILINK_LOG_ERROR("tcp_client", "join", "Join failed: " + std::string(e.what()));
  } catch (...) {
    UNILINK_LOG_ERROR("tcp_client", "join", "Join failed with unknown error");
  }
}

void TcpClient::Impl::notify_state() {
  if (stop_requested_.load() || stopping_.load()) return;

  OnState on_state;
  {
    std::lock_guard<std::mutex> lock(callback_mtx_);
    on_state = on_state_;
  }
  if (!on_state) return;

  try {
    on_state(state_.state());
  } catch (const std::exception& e) {
    UNILINK_LOG_ERROR("tcp_client", "on_state", "Exception in state callback: " + std::string(e.what()));
  } catch (...) {
    UNILINK_LOG_ERROR("tcp_client", "on_state", "Unknown exception in state callback");
  }
}

void TcpClient::Impl::record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat,
                                   std::string_view operation, const boost::system::error_code& ec,
                                   std::string_view msg, bool retryable, uint32_t retry_count) {
  std::lock_guard<std::mutex> lock(last_err_mtx_);
  diagnostics::ErrorInfo info(lvl, cat, "tcp_client", operation, msg, ec, retryable);
  info.retry_count = retry_count;
  last_error_info_ = info;
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
    UNILINK_LOG_ERROR("tcp_client", "reset_io_objects", fmt::format("Reset error: {}", e.what()));
    record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "reset_io_objects", {},
                 fmt::format("Reset error: {}", e.what()), false, 0);
    diagnostics::error_reporting::report_system_error(
        "tcp_client", "reset_io_objects", fmt::format("Exception while resetting io objects: {}", e.what()));
  } catch (...) {
    UNILINK_LOG_ERROR("tcp_client", "reset_io_objects", "Unknown reset error");
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
  idle_timer_.async_wait([self, seq](const boost::system::error_code& ec) {
    if (ec == net::error::operation_aborted || seq != self->impl_->current_seq_.load()) {
      return;
    }
    if (!ec) {
      self->impl_->handle_idle_timeout(self, seq);
    }
  });
}

void TcpClient::Impl::cancel_idle_timer() { idle_timer_.cancel(); }

}  // namespace transport
}  // namespace unilink
