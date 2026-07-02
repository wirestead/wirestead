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

#include "unilink/transport/serial/serial.hpp"

#include <spdlog/fmt/fmt.h>

#include <atomic>
#include <boost/asio.hpp>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/concurrency/io_context_manager.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/error_handler.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/interface/iserial_port.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/serial/boost_serial_port.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;
using base::LinkState;
using concurrency::ThreadSafeLinkState;

using BufferVariant =
    std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;

namespace {
net::io_context& acquire_shared_serial_context() {
  auto& manager = concurrency::IoContextManager::instance();
  manager.start();
  return manager.get_context();
}
}  // namespace

struct Serial::Impl {
  bool started_ = false;
  std::atomic<bool> stopping_{false};
  net::io_context& ioc_;
  bool owns_ioc_{false};
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;

  std::unique_ptr<interface::SerialPortInterface> port_;
  config::SerialConfig cfg_;
  net::steady_timer retry_timer_;

  std::vector<uint8_t> rx_;
  std::deque<BufferVariant> tx_;
  std::deque<BufferVariant> pending_;
  std::atomic<size_t> pending_bytes_{0};
  std::optional<BufferVariant> current_write_buffer_;
  bool writing_ = false;
  std::atomic<size_t> queued_bytes_{0};
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
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_bp_;

  std::atomic<bool> opened_{false};
  ThreadSafeLinkState state_{LinkState::Idle};

  void observe_queue() {
    stats_.observe_queue(queued_bytes_.load(std::memory_order_relaxed) +
                         pending_bytes_.load(std::memory_order_relaxed));
  }

  void maybe_flush_for_keep_latest(size_t added) {
    const auto dropped = queue_util::maybe_flush_for_keep_latest(bp_strategy_, added, bp_high_, tx_, queued_bytes_,
                                                                 backpressure_active_);
    if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);
  }

  explicit Impl(const config::SerialConfig& cfg)
      : ioc_(acquire_shared_serial_context()),
        owns_ioc_(false),
        strand_(ioc_.get_executor()),
        cfg_(cfg),
        retry_timer_(ioc_),
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
        bp_strategy_(cfg.backpressure_strategy),
        retry_interval_ms_(cfg.retry_interval_ms),
        bp_high_(cfg.backpressure_threshold) {
    init();
  }

  void init() {
    cfg_.validate_and_clamp();
    bp_high_ = cfg_.backpressure_threshold;
    bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                         base::constants::MAX_BUFFER_SIZE);
    bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
    if (bp_low_ == 0) bp_low_ = 1;
    rx_.resize(cfg_.read_chunk);
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
    boost::system::error_code ec;
    port_->open(cfg_.device, ec);
    if (ec) {
      UNILINK_LOG_ERROR("serial", "open", fmt::format("Failed to open device: {} - {}", cfg_.device, ec.message()));
      handle_error(self, "open", ec);
      return;
    }

    port_->set_option(net::serial_port_base::baud_rate(cfg_.baud_rate), ec);
    if (ec) {
      UNILINK_LOG_ERROR("serial", "configure", fmt::format("Failed baud rate: {}", ec.message()));
      handle_error(self, "baud_rate", ec);
      return;
    }

    port_->set_option(net::serial_port_base::character_size(cfg_.char_size), ec);
    if (ec) {
      UNILINK_LOG_ERROR("serial", "configure", fmt::format("Failed char size: {}", ec.message()));
      handle_error(self, "char_size", ec);
      return;
    }

    using sb = net::serial_port_base::stop_bits;
    port_->set_option(sb(cfg_.stop_bits == 2 ? sb::two : sb::one), ec);
    if (ec) {
      UNILINK_LOG_ERROR("serial", "configure", fmt::format("Failed stop bits: {}", ec.message()));
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
      UNILINK_LOG_ERROR("serial", "configure", fmt::format("Failed parity: {}", ec.message()));
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
      UNILINK_LOG_ERROR("serial", "configure", fmt::format("Failed flow control: {}", ec.message()));
      handle_error(self, "flow_control", ec);
      return;
    }

    UNILINK_LOG_INFO("serial", "connect", fmt::format("Device opened: {}", cfg_.device));
    start_read(self);

    opened_.store(true);
    state_.set_state(LinkState::Connected);
    notify_state();
    do_write(self);
  }

  void start_read(std::shared_ptr<Serial> self) {
    port_->async_read_some(
        net::buffer(rx_.data(), rx_.size()), net::bind_executor(strand_, [self](auto ec, std::size_t n) {
          auto impl = self->get_impl();
          if (ec) {
            impl->handle_error(self, "read", ec);
            return;
          }
          if (n > 0) impl->stats_.record_received(n);
          OnBytes on_bytes;
          {
            std::lock_guard<std::mutex> lock(impl->callback_mtx_);
            on_bytes = impl->on_bytes_;
          }
          if (on_bytes) {
            try {
              on_bytes(memory::ConstByteSpan(impl->rx_.data(), n));
            } catch (const std::exception& e) {
              UNILINK_LOG_ERROR("serial", "on_bytes", fmt::format("Exception in callback: {}", e.what()));
              if (impl->cfg_.stop_on_callback_exception) {
                impl->opened_.store(false);
                impl->close_port();
                impl->state_.set_state(LinkState::Error);
                impl->notify_state();
                return;
              }
              impl->handle_error(self, "on_bytes_callback", make_error_code(boost::system::errc::io_error));
              return;
            } catch (...) {
              if (impl->cfg_.stop_on_callback_exception) {
                impl->opened_.store(false);
                impl->close_port();
                impl->state_.set_state(LinkState::Error);
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
    if (stopping_.load() || tx_.empty()) {
      writing_ = false;
      return;
    }
    writing_ = true;

    current_write_buffer_ = std::move(tx_.front());
    tx_.pop_front();

    auto& current = *current_write_buffer_;

    auto on_write = [self](const boost::system::error_code& ec, std::size_t n) {
      auto impl = self->get_impl();
      impl->current_write_buffer_.reset();

      if (impl->queued_bytes_ >= n) {
        impl->queued_bytes_ -= n;
      } else {
        impl->queued_bytes_ = 0;
      }
      impl->report_backpressure(impl->queued_bytes_);

      if (impl->stopping_.load()) {
        impl->writing_ = false;
        return;
      }

      if (ec) {
        impl->handle_error(self, "write", ec);
        return;
      }
      impl->stats_.record_sent(n);
      impl->do_write(self);
    };

    std::visit(
        [&](auto&& buf) {
          using T = std::decay_t<decltype(buf)>;
          auto* data_ptr = [&]() {
            if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>)
              return buf->data();
            else
              return buf.data();
          }();
          auto size = [&]() {
            if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>)
              return buf->size();
            else
              return buf.size();
          }();
          port_->async_write(net::buffer(data_ptr, size), net::bind_executor(strand_, on_write));
        },
        current);
  }

  void perform_cleanup() {
    try {
      retry_timer_.cancel();
      close_port();
      tx_.clear();
      queued_bytes_ = 0;
      pending_.clear();
      pending_bytes_ = 0;
      writing_ = false;
      report_backpressure(queued_bytes_);
      opened_.store(false);
      state_.set_state(LinkState::Closed);
      notify_state();
    } catch (...) {
    }
  }

  void handle_error(std::shared_ptr<Serial> self, const char* where, const boost::system::error_code& ec) {
    if (ec == boost::asio::error::eof) {
      if (self) start_read(self);
      return;
    }

    if (stopping_.load()) {
      perform_cleanup();
      return;
    }

    if (ec == boost::asio::error::operation_aborted) {
      if (state_.is_state(LinkState::Error)) return;
      perform_cleanup();
      return;
    }

    bool retryable = cfg_.reopen_on_error;
    diagnostics::error_reporting::report_connection_error("serial", where, ec, retryable);

    UNILINK_LOG_ERROR("serial", where, fmt::format("Error: {}", ec.message()));

    if (cfg_.reopen_on_error) {
      opened_.store(false);
      close_port();
      state_.set_state(LinkState::Connecting);
      notify_state();
      if (self) schedule_retry(self, where, ec);
    } else {
      opened_.store(false);
      close_port();
      state_.set_state(LinkState::Error);
      notify_state();
    }
  }

  void schedule_retry(std::shared_ptr<Serial> self, const char* where, const boost::system::error_code& ec) {
    (void)ec;
    UNILINK_LOG_INFO("serial", "retry", fmt::format("Scheduling retry at {}", where));
    if (stopping_.load()) return;
    retry_timer_.expires_after(std::chrono::milliseconds(retry_interval_ms_.load()));
    retry_timer_.async_wait([self](auto e) {
      if (!e && self && !self->get_impl()->stopping_.load()) self->get_impl()->open_and_configure(self);
    });
  }

  void close_port() {
    boost::system::error_code ec;
    if (port_ && port_->is_open()) {
      port_->close(ec);
    }
  }

  void notify_state() {
    if (stopping_.load()) return;
    OnState on_state;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_state = on_state_;
    }
    if (!on_state) return;
    try {
      on_state(state_.state());
    } catch (...) {
    }
  }

  void report_backpressure(size_t qb) {
    if (stopping_.load()) return;
    observe_queue();

    OnBackpressure on_bp;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_bp = on_bp_;
    }

    if (!backpressure_active_ && qb >= bp_high_) {
      backpressure_active_ = true;
      stats_.record_backpressure_event();
      if (on_bp) {
        try {
          on_bp(qb);
        } catch (...) {
        }
      }
    } else if (backpressure_active_ && qb <= bp_low_) {
      // Flush pending_ → tx_
      const size_t moved = pending_bytes_.exchange(0);
      queued_bytes_ += moved;
      while (!pending_.empty()) {
        tx_.emplace_back(std::move(pending_.front()));
        pending_.pop_front();
      }
      observe_queue();
      backpressure_active_ = false;
      stats_.record_backpressure_event();
      if (on_bp) {
        try {
          on_bp(qb);
        } catch (...) {
        }  // fire OFF with pre-flush queue size
      }
      // If post-flush queue is still high, fire ON again
      if (queued_bytes_ >= bp_high_) {
        backpressure_active_ = true;
        stats_.record_backpressure_event();
        if (on_bp) {
          try {
            on_bp(queued_bytes_);
          } catch (...) {
          }
        }
      }
    }
  }
};

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg) {
  return std::shared_ptr<Serial>(new Serial(cfg));
}

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg, net::io_context& ioc) {
  return std::shared_ptr<Serial>(new Serial(cfg, std::make_unique<BoostSerialPort>(ioc), ioc));
}

std::shared_ptr<Serial> Serial::create(const config::SerialConfig& cfg,
                                       std::unique_ptr<interface::SerialPortInterface> port, net::io_context& ioc) {
  return std::shared_ptr<Serial>(new Serial(cfg, std::move(port), ioc));
}

Serial::Serial(const config::SerialConfig& cfg) : impl_(std::make_unique<Impl>(cfg)) {}

Serial::Serial(const config::SerialConfig& cfg, std::unique_ptr<interface::SerialPortInterface> port,
               net::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, std::move(port), ioc)) {}

Serial::~Serial() {
  if (impl_ && impl_->started_ && !impl_->state_.is_state(LinkState::Closed)) {
    // In destructor, stop without shared_from_this
    impl_->stopping_.store(true);
    impl_->perform_cleanup();
    if (impl_->owns_ioc_ && impl_->ioc_thread_.joinable()) {
      impl_->ioc_thread_.join();
    }
  }
}

Serial::Serial(Serial&&) noexcept = default;
Serial& Serial::operator=(Serial&&) noexcept = default;

void Serial::start() {
  auto impl = get_impl();
  if (impl->started_) return;
  impl->stopping_.store(false);
  UNILINK_LOG_INFO("serial", "start", fmt::format("Starting device: {}", impl->cfg_.device));
  if (!impl->owns_ioc_) {
    auto& manager = concurrency::IoContextManager::instance();
    if (!manager.is_running()) manager.start();
    if (impl->ioc_.stopped()) impl->ioc_.restart();
  }
  impl->work_guard_ =
      std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl->ioc_.get_executor());
  if (impl->owns_ioc_) {
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_.stop(); });
        impl->ioc_.run();
      } catch (...) {
      }
    });
  }
  auto self = shared_from_this();
  net::post(impl->strand_, [self] {
    auto impl = self->get_impl();
    if (!impl->stopping_.load()) {
      impl->state_.set_state(LinkState::Connecting);
      impl->notify_state();
      impl->open_and_configure(self);
    }
  });
  impl->started_ = true;
}

void Serial::stop() {
  auto impl = get_impl();
  if (!impl->started_) {
    impl->state_.set_state(LinkState::Closed);
    return;
  }

  if (impl->stopping_.exchange(true)) return;

  auto self = shared_from_this();
  net::post(impl->strand_, [self] {
    auto impl = self->get_impl();
    impl->perform_cleanup();
    if (impl->owns_ioc_) impl->ioc_.stop();
  });

  if (impl->owns_ioc_ && impl->ioc_thread_.joinable()) {
    impl->ioc_thread_.join();
    impl->ioc_.restart();
  }
  impl->started_ = false;
}

bool Serial::is_connected() const { return get_impl()->opened_.load(); }
bool Serial::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
wrapper::RuntimeStats Serial::stats() const {
  auto impl = get_impl();
  return impl->stats_.snapshot(impl->queued_bytes_.load(std::memory_order_relaxed),
                               impl->pending_bytes_.load(std::memory_order_relaxed),
                               impl->backpressure_active_.load(std::memory_order_relaxed));
}
void Serial::reset_stats() {
  auto impl = get_impl();
  impl->stats_.reset(impl->queued_bytes_.load(std::memory_order_relaxed) +
                     impl->pending_bytes_.load(std::memory_order_relaxed));
}

boost::asio::any_io_executor Serial::get_executor() { return impl_->strand_; }

bool Serial::async_write_copy(memory::ConstByteSpan data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }

  size_t n = data.size();
  if (n == 0) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (n > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("serial", "write", "Write size exceeds maximum");
    impl->stats_.record_failed_send();
    return false;
  }

  if (n <= 65536) {
    memory::PooledBuffer pooled(n);
    if (pooled.valid()) {
      base::safe_memory::safe_memcpy(pooled.data(), data.data(), n);
      if (impl->queued_bytes_ + impl->pending_bytes_ + n > impl->bp_limit_) {
        impl->stats_.record_failed_send();
        return false;
      }
      impl->stats_.record_accepted(n);
      net::post(impl->strand_, [self = shared_from_this(), buf = std::move(pooled)]() mutable {
        auto impl = self->get_impl();
        const auto added = buf.size();

        // Reliable: route to pending_ when backpressure is active
        if (impl->bp_strategy_ == base::constants::BackpressureStrategy::Reliable &&
            impl->backpressure_active_.load()) {
          if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
            UNILINK_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
            return;
          }
          impl->pending_bytes_ += added;
          impl->pending_.emplace_back(std::move(buf));
          impl->observe_queue();
          return;
        }

        impl->maybe_flush_for_keep_latest(added);

        if (impl->queued_bytes_ + added > impl->bp_limit_) {
          UNILINK_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
          impl->report_backpressure(impl->queued_bytes_ + added);
          return;
        }
        impl->queued_bytes_ += added;
        impl->tx_.emplace_back(std::move(buf));
        impl->observe_queue();
        impl->report_backpressure(impl->queued_bytes_);
        if (!impl->writing_) impl->do_write(self);
      });
      return true;
    }
  }

  if (impl->queued_bytes_ + impl->pending_bytes_ + n > impl->bp_limit_) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::vector<uint8_t> fallback(data.begin(), data.end());
  impl->stats_.record_accepted(n);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(fallback)]() mutable {
    auto impl = self->get_impl();
    const auto added = buf.size();

    // Reliable: route to pending_ when backpressure is active
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::Reliable && impl->backpressure_active_.load()) {
      if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
        UNILINK_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
        return;
      }
      impl->pending_bytes_ += added;
      impl->pending_.emplace_back(std::move(buf));
      impl->observe_queue();
      return;
    }

    impl->maybe_flush_for_keep_latest(added);

    if (impl->queued_bytes_ + added > impl->bp_limit_) {
      impl->report_backpressure(impl->queued_bytes_ + added);
      impl->tx_.clear();
      impl->queued_bytes_ = 0;
      impl->writing_ = false;
      impl->state_.set_state(LinkState::Error);
      impl->notify_state();
      impl->handle_error(self, "write_queue_overflow", make_error_code(boost::system::errc::no_buffer_space));
      return;
    }
    impl->queued_bytes_ += added;
    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  return true;
}

bool Serial::async_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto added = data.size();
  if (added == 0) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
    impl->stats_.record_failed_send();
    return false;
  }
  impl->stats_.record_accepted(added);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    auto impl = self->get_impl();

    // Reliable: route to pending_ when backpressure is active
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::Reliable && impl->backpressure_active_.load()) {
      if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
        UNILINK_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
        return;
      }
      impl->pending_bytes_ += added;
      impl->pending_.emplace_back(std::move(buf));
      impl->observe_queue();
      return;
    }

    impl->maybe_flush_for_keep_latest(added);

    if (impl->queued_bytes_ + added > impl->bp_limit_) {
      impl->report_backpressure(impl->queued_bytes_ + added);
      impl->tx_.clear();
      impl->queued_bytes_ = 0;
      impl->writing_ = false;
      impl->state_.set_state(LinkState::Error);
      impl->notify_state();
      impl->handle_error(self, "write_queue_overflow", make_error_code(boost::system::errc::no_buffer_space));
      return;
    }
    impl->queued_bytes_ += added;
    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  return true;
}

bool Serial::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (!data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
    impl->stats_.record_failed_send();
    return false;
  }
  impl->stats_.record_accepted(added);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    auto impl = self->get_impl();

    // Reliable: route to pending_ when backpressure is active
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::Reliable && impl->backpressure_active_.load()) {
      if (impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
        UNILINK_LOG_ERROR("serial", "write", "Queue limit exceeded, dropping message");
        return;
      }
      impl->pending_bytes_ += added;
      impl->pending_.emplace_back(std::move(buf));
      impl->observe_queue();
      return;
    }

    impl->maybe_flush_for_keep_latest(added);

    if (impl->queued_bytes_ + added > impl->bp_limit_) {
      impl->report_backpressure(impl->queued_bytes_ + added);
      impl->tx_.clear();
      impl->queued_bytes_ = 0;
      impl->writing_ = false;
      impl->state_.set_state(LinkState::Error);
      impl->notify_state();
      impl->handle_error(self, "write_queue_overflow", make_error_code(boost::system::errc::no_buffer_space));
      return;
    }
    impl->queued_bytes_ += added;
    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  return true;
}

bool Serial::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty() || data.size() > base::constants::MAX_BUFFER_SIZE) {
    get_impl()->stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool Serial::async_try_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto added = data.size();
  if (added == 0 || added > base::constants::MAX_BUFFER_SIZE) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [impl, added]() {
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl->stats_.record_dropped(1, added);
    } else {
      impl->stats_.record_failed_send();
    }
  };
  if (impl->backpressure_active_.load() || impl->queued_bytes_ + added > impl->bp_high_ ||
      impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl->queued_bytes_, impl->pending_bytes_, impl->backpressure_active_, added,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl->stats_.record_accepted(added);

  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    auto impl = self->get_impl();
    if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queued_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  return true;
}

bool Serial::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error) ||
      !data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  if (added > base::constants::MAX_BUFFER_SIZE) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [impl, added]() {
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl->stats_.record_dropped(1, added);
    } else {
      impl->stats_.record_failed_send();
    }
  };
  if (impl->backpressure_active_.load() || impl->queued_bytes_ + added > impl->bp_high_ ||
      impl->queued_bytes_ + impl->pending_bytes_ + added > impl->bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl->queued_bytes_, impl->pending_bytes_, impl->backpressure_active_, added,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl->stats_.record_accepted(added);

  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    auto impl = self->get_impl();
    if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queued_bytes_, added);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.emplace_back(std::move(buf));
    impl->observe_queue();
    impl->report_backpressure(impl->queued_bytes_);
    if (!impl->writing_) impl->do_write(self);
  });
  return true;
}

void Serial::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(cb);
}
void Serial::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(cb);
}
void Serial::on_backpressure(OnBackpressure cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(cb);
}

void Serial::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  get_impl()->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

void Serial::set_retry_interval(unsigned interval_ms) {
  get_impl()->retry_interval_ms_.store(interval_ms, std::memory_order_relaxed);
}

}  // namespace transport
}  // namespace unilink
