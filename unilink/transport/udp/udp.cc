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

#include "unilink/transport/udp/udp.hpp"

#include <spdlog/fmt/fmt.h>

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/error_handler.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/bp_state_machine.hpp"
#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/base/error_info_holder.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;
using udp = net::ip::udp;
using base::LinkState;
using concurrency::ThreadSafeLinkState;

struct UdpChannel::Impl {
  std::unique_ptr<net::io_context> owned_ioc_;
  net::io_context* ioc_;
  bool owns_ioc_{true};
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;

  udp::socket socket_;
  udp::endpoint local_endpoint_;
  udp::endpoint recv_endpoint_;
  std::optional<udp::endpoint> remote_endpoint_;

  using BufferVariant =
      std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  struct TxItem {
    BufferVariant buffer;
    std::optional<udp::endpoint> destination;
  };

  std::array<uint8_t, 65536> rx_{};
  std::deque<TxItem> tx_;
  std::deque<TxItem> pending_;
  std::atomic<size_t> pending_bytes_{0};
  bool writing_{false};
  std::atomic<size_t> queue_bytes_{0};
  config::UdpConfig cfg_;
  // Atomic rather than mutex-guarded: read both from the strand (report_backpressure,
  // do_write) and from arbitrary caller threads (the async_try_write_* fast-fail
  // prechecks) - a strand-post here would only protect the former, not the latter (#436).
  std::atomic<base::constants::BackpressureStrategy> bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_high_;
  size_t bp_low_;
  size_t bp_limit_;
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> opened_{false};
  std::atomic<bool> connected_{false};
  bool started_{false};
  ThreadSafeLinkState state_{LinkState::Idle};
  std::atomic<bool> terminal_state_notified_{false};

  // Guards on_bytes_/on_bytes_from_/on_state_/on_bp_. Setters (called from
  // any user thread) and the strand-confined read sites below both take
  // this lock; readers copy the callback under lock then invoke the copy
  // outside the lock, matching the pattern already used correctly by
  // TcpClient/UdsClient/both servers (see #436).
  mutable std::mutex callback_mtx_;
  OnBytes on_bytes_;
  UdpChannel::OnBytesFrom on_bytes_from_;
  OnState on_state_;
  OnBackpressure on_bp_;

  ErrorInfoHolder error_info_holder_{"udp"};

  explicit Impl(const config::UdpConfig& config)
      : owned_ioc_(std::make_unique<net::io_context>()),
        ioc_(owned_ioc_.get()),
        owns_ioc_(true),
        strand_(ioc_->get_executor()),
        socket_(strand_),
        cfg_(config),
        bp_strategy_(config.backpressure_strategy),
        bp_high_(config.backpressure_threshold) {
    init();
  }

  Impl(const config::UdpConfig& config, net::io_context& external_ioc)
      : ioc_(&external_ioc),
        owns_ioc_(false),
        strand_(external_ioc.get_executor()),
        socket_(strand_),
        cfg_(config),
        bp_strategy_(config.backpressure_strategy),
        bp_high_(config.backpressure_threshold) {
    init();
  }

  void init() {
    cfg_.validate_and_clamp();
    bp_high_ = cfg_.backpressure_threshold;
    bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
    if (bp_low_ == 0) bp_low_ = 1;
    bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                         base::constants::MAX_BUFFER_SIZE);
    if (bp_limit_ < bp_high_) {
      bp_limit_ = bp_high_;
    }
    set_remote_from_config();
  }

  ~Impl() {
    try {
      stop_requested_.store(true);
      stopping_.store(true);
      if (owns_ioc_ && work_guard_) {
        work_guard_.reset();
      }
      if (ioc_thread_.joinable()) {
        if (std::this_thread::get_id() == ioc_thread_.get_id()) {
          ioc_thread_.detach();
        } else {
          ioc_thread_.request_stop();
          ioc_thread_.join();
        }
      }
      perform_stop_cleanup();
    } catch (...) {
    }
  }

  void open_socket(std::shared_ptr<UdpChannel> self) {
    if (stopping_.load() || stop_requested_.load()) return;

    boost::system::error_code ec;
    auto address = net::ip::make_address(cfg_.bind_address, ec);
    if (ec) {
      std::string msg = fmt::format("Invalid bind address: {}", cfg_.bind_address);
      UNILINK_LOG_ERROR("udp", "bind", msg);
      transition_to(LinkState::Error, ec, "bind", msg);
      return;
    }

    local_endpoint_ = udp::endpoint(address, cfg_.local_port);
    socket_.open(local_endpoint_.protocol(), ec);
    if (ec) {
      std::string msg = fmt::format("Socket open failed: {}", ec.message());
      UNILINK_LOG_ERROR("udp", "open", msg);
      transition_to(LinkState::Error, ec, "open", msg);
      return;
    }

    if (cfg_.reuse_address) {
      socket_.set_option(net::socket_base::reuse_address(true), ec);
      if (ec) {
        std::string msg = fmt::format("Failed to set reuse_address: {}", ec.message());
        UNILINK_LOG_ERROR("udp", "open", msg);
        transition_to(LinkState::Error, ec, "open", msg);
        return;
      }
    }

    if (cfg_.enable_broadcast) {
      socket_.set_option(net::socket_base::broadcast(true), ec);
      if (ec) {
        std::string msg = fmt::format("Failed to set broadcast: {}", ec.message());
        UNILINK_LOG_ERROR("udp", "open", msg);
        transition_to(LinkState::Error, ec, "open", msg);
        return;
      }
    }

    socket_.bind(local_endpoint_, ec);
    if (ec) {
      std::string msg = fmt::format("Bind failed: {}", ec.message());
      UNILINK_LOG_ERROR("udp", "bind", msg);
      transition_to(LinkState::Error, ec, "bind", msg);
      return;
    }

    // Set large OS buffers for UDP to prevent drops unless explicitly configured.
    const int automatic_buf_size = std::max(static_cast<int>(bp_high_), 4 * 1024 * 1024);
    const int recv_buf_size =
        cfg_.receive_buffer_size > 0 ? static_cast<int>(cfg_.receive_buffer_size) : automatic_buf_size;
    const int send_buf_size = cfg_.send_buffer_size > 0 ? static_cast<int>(cfg_.send_buffer_size) : automatic_buf_size;

    socket_.set_option(net::socket_base::receive_buffer_size(recv_buf_size), ec);
    if (ec) {
      UNILINK_LOG_WARNING("udp", "open", fmt::format("Failed to set receive buffer size: {}", ec.message()));
      ec.clear();
    }
    socket_.set_option(net::socket_base::send_buffer_size(send_buf_size), ec);
    if (ec) {
      UNILINK_LOG_WARNING("udp", "open", fmt::format("Failed to set send buffer size: {}", ec.message()));
      ec.clear();
    }

    opened_.store(true);
    if (remote_endpoint_) {
      connected_.store(true);
      transition_to(LinkState::Connected);
    } else {
      transition_to(LinkState::Listening);
    }
    start_receive(self);
  }

  void start_receive(std::shared_ptr<UdpChannel> self) {
    if (stopping_.load() || stop_requested_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error) || !socket_.is_open()) {
      return;
    }

    socket_.async_receive_from(net::buffer(rx_), recv_endpoint_,
                               [self](const boost::system::error_code& ec, std::size_t bytes) {
                                 auto impl = self->get_impl();
                                 impl->handle_receive(self, ec, bytes);
                               });
  }

  void handle_receive(std::shared_ptr<UdpChannel> self, const boost::system::error_code& ec, std::size_t bytes) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }

    if (stopping_.load() || stop_requested_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error)) {
      return;
    }

    if (ec == boost::asio::error::message_size || bytes >= rx_.size()) {
      UNILINK_LOG_ERROR("udp", "receive", "Datagram truncated (buffer too small)");
      transition_to(LinkState::Error, ec, "receive", "Datagram truncated (buffer too small)");
      return;
    }

    if (ec) {
      std::string msg = fmt::format("Receive failed: {}", ec.message());
      UNILINK_LOG_ERROR("udp", "receive", msg);
      transition_to(LinkState::Error, ec, "receive", msg);
      return;
    }

    if (!remote_endpoint_) {
      remote_endpoint_ = recv_endpoint_;
      connected_.store(true);
      transition_to(LinkState::Connected);
    }

    if (bytes > 0) {
      stats_.record_received(bytes);
      OnBytes on_bytes;
      UdpChannel::OnBytesFrom on_bytes_from;
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bytes = on_bytes_;
        on_bytes_from = on_bytes_from_;
      }
      if (on_bytes) {
        try {
          on_bytes(memory::ConstByteSpan(rx_.data(), bytes));
        } catch (const std::exception& e) {
          std::string msg = fmt::format("Exception in bytes callback: {}", e.what());
          UNILINK_LOG_ERROR("udp", "on_bytes", msg);
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes", msg);
            return;
          }
        } catch (...) {
          UNILINK_LOG_ERROR("udp", "on_bytes", "Unknown exception in bytes callback");
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes", "Unknown exception in bytes callback");
            return;
          }
        }
      }

      if (on_bytes_from) {
        try {
          on_bytes_from(memory::ConstByteSpan(rx_.data(), bytes), recv_endpoint_);
        } catch (const std::exception& e) {
          std::string msg = fmt::format("Exception in bytes callback: {}", e.what());
          UNILINK_LOG_ERROR("udp", "on_bytes_from", msg);
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes_from", msg);
            return;
          }
        } catch (...) {
          UNILINK_LOG_ERROR("udp", "on_bytes_from", "Unknown exception in bytes callback");
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes_from", "Unknown exception in bytes callback");
            return;
          }
        }
      }
    }

    start_receive(self);
  }

  queue_util::BackpressureFields bp_fields() {
    return queue_util::BackpressureFields{queue_bytes_,
                                          pending_bytes_,
                                          backpressure_active_,
                                          bp_high_,
                                          bp_low_,
                                          bp_limit_,
                                          bp_strategy_.load(std::memory_order_relaxed)};
  }

  // Drops all queued (tx_) and pending (pending_, reliable-mode overflow) writes and clears
  // backpressure, notifying any waiter directly. Mirrors perform_stop_cleanup()'s approach
  // rather than calling report_backpressure(), because report_backpressure() flushes pending_
  // back into tx_ and can immediately re-arm backpressure_active_ if enough was queued there -
  // which would leave a Reliable-mode sender blocked in send_blocking()'s bp_cv_ wait forever
  // since nothing will ever call do_write() again once the channel has stopped/errored (#427).
  void drain_queue_and_clear_backpressure() {
    OnBackpressure on_bp;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_bp = on_bp_;
    }
    auto f = bp_fields();
    queue_util::drain_and_clear_backpressure(f, on_bp, [&]() {
      tx_.clear();
      queue_bytes_ = 0;
      pending_.clear();
      pending_bytes_ = 0;
    });
  }

  void do_write(std::shared_ptr<UdpChannel> self) {
    if (writing_ || tx_.empty()) return;
    if (stop_requested_.load() || stopping_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error)) {
      writing_ = false;
      drain_queue_and_clear_backpressure();
      return;
    }

    auto current = std::move(tx_.front());
    tx_.pop_front();

    const auto& dest_endpoint = current.destination ? current.destination : remote_endpoint_;

    if (!dest_endpoint) {
      UNILINK_LOG_WARNING("udp", "write", "Remote endpoint not set; dropping write request");
      writing_ = false;
      do_write(self);  // Process next in queue
      return;
    }

    writing_ = true;

    auto bytes_queued = std::visit(
        [](auto&& buf) -> size_t {
          using Buffer = std::decay_t<decltype(buf)>;
          if constexpr (std::is_same_v<Buffer, std::shared_ptr<const std::vector<uint8_t>>>) {
            return buf ? buf->size() : 0;
          } else {
            return buf.size();
          }
        },
        current.buffer);

    auto on_write = [self, bytes_queued](const boost::system::error_code& ec, std::size_t bytes_written) {
      auto impl = self->get_impl();
      impl->queue_bytes_ = (impl->queue_bytes_ > bytes_queued) ? (impl->queue_bytes_ - bytes_queued) : 0;
      impl->report_backpressure(self, impl->queue_bytes_);

      if (ec == boost::asio::error::operation_aborted) {
        impl->writing_ = false;
        return;
      }

      if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
          impl->state_.is_state(LinkState::Error)) {
        impl->writing_ = false;
        impl->drain_queue_and_clear_backpressure();
        return;
      }

      if (ec) {
        std::string msg = fmt::format("Send failed: {}", ec.message());
        UNILINK_LOG_ERROR("udp", "write", msg);
        impl->transition_to(LinkState::Error, ec, "write", msg);
        impl->writing_ = false;
        // do_write() will never run again to reach the "already in Error" cleanup above, so
        // drain everything and clear backpressure here directly.
        impl->drain_queue_and_clear_backpressure();
        return;
      }

      impl->stats_.record_sent(bytes_written);
      impl->writing_ = false;
      impl->do_write(self);
    };

    std::visit(
        [&](auto&& buf) {
          using T = std::decay_t<decltype(buf)>;

          auto* data_ptr = [&]() {
            if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
              return buf->data();
            } else {
              return buf.data();
            }
          }();

          auto size = [&]() {
            if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
              return buf->size();
            } else {
              return buf.size();
            }
          }();

          socket_.async_send_to(
              net::buffer(data_ptr, size), *dest_endpoint,
              [buf_captured = std::move(buf), on_write = std::move(on_write)](
                  const boost::system::error_code& ec, std::size_t bytes) mutable { on_write(ec, bytes); });
        },
        std::move(current.buffer));
  }

  void close_socket() {
    boost::system::error_code ec;
    socket_.cancel(ec);
    socket_.close(ec);
  }

  void notify_state() {
    OnState on_state;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_state = on_state_;
    }
    if (!on_state) return;
    try {
      on_state(state_.state());
    } catch (const std::exception& e) {
      UNILINK_LOG_ERROR("udp", "on_state", fmt::format("Exception in state callback: {}", e.what()));
    } catch (...) {
      UNILINK_LOG_ERROR("udp", "on_state", "Unknown exception in state callback");
    }
  }

  void report_backpressure(std::shared_ptr<UdpChannel> self, size_t queued_bytes) {
    if (stop_requested_.load()) return;
    observe_queue();

    OnBackpressure on_bp;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_bp = on_bp_;
    }

    auto f = bp_fields();
    queue_util::report_backpressure(
        f, queued_bytes, on_bp, stats_,
        [&]() -> size_t {
          // Flush pending_ → tx_
          const size_t moved = pending_bytes_.exchange(0);
          while (!pending_.empty()) {
            tx_.emplace_back(std::move(pending_.front()));
            pending_.pop_front();
          }
          return moved;
        },
        [&]() {
          // Post-flush sample: queue_bytes_ has already been updated by the
          // time this kick runs, unlike the flush hook above (#434).
          observe_queue();
          if (!writing_) do_write(self);
        });
  }

  void observe_queue() {
    stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
  }

  bool enqueue_buffer(std::shared_ptr<UdpChannel> self, BufferVariant&& buffer, size_t size,
                      std::optional<udp::endpoint> dest = std::nullopt) {
    if (stopping_.load() || stop_requested_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error)) {
      return false;
    }

    auto f = bp_fields();
    queue_util::DropAccounting dropped;
    // TxItem carries a destination endpoint alongside the BufferVariant that
    // decide_enqueue()'s BestEffort trim needs to visit - project onto
    // `.buffer` rather than the item itself (#434).
    auto decision =
        queue_util::decide_enqueue(f, size, tx_, dropped, [](TxItem& item) -> BufferVariant& { return item.buffer; });
    if (dropped.any()) {
      stats_.record_dropped(dropped.messages, dropped.bytes);
    }

    if (decision == queue_util::EnqueueDecision::Rejected) {
      UNILINK_LOG_ERROR("udp", "write",
                        fmt::format("Queue limit exceeded ({} bytes)", queue_bytes_ + pending_bytes_ + size));
      // Always reporting here (rather than only for the non-Reliable-pending
      // rejection path, as the pre-#434 code did) is a no-op in practice for
      // the Reliable+pending case: queued_bytes is already >= bp_high_ or
      // this rejection couldn't have happened, so report_backpressure()'s
      // OFF-transition check (<= bp_low_) can't fire here either way.
      report_backpressure(self, queue_bytes_ + size);
      return false;
    }

    if (decision == queue_util::EnqueueDecision::Pending) {
      pending_bytes_ += size;
      pending_.push_back({std::move(buffer), dest});
      observe_queue();
      return true;
    }

    queue_bytes_ += size;
    tx_.push_back({std::move(buffer), dest});
    observe_queue();
    report_backpressure(self, queue_bytes_);
    return true;
  }

  void set_remote_from_config() {
    if (!cfg_.remote_address || !cfg_.remote_port) return;
    boost::system::error_code ec;
    auto addr = net::ip::make_address(*cfg_.remote_address, ec);
    if (ec) {
      throw std::runtime_error("Invalid remote address: " + *cfg_.remote_address);
    }
    remote_endpoint_ = udp::endpoint(addr, *cfg_.remote_port);
  }

  void transition_to(LinkState target, const boost::system::error_code& ec = {}, std::string_view operation = {},
                     std::string_view msg = {}) {
    if (ec == net::error::operation_aborted) {
      return;
    }

    const auto current = state_.state();
    if ((current == LinkState::Closed || current == LinkState::Error) &&
        (target == LinkState::Closed || target == LinkState::Error)) {
      return;
    }

    if (target == LinkState::Error) {
      error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, operation,
                                      ec, msg.empty() ? std::string_view(ec.message()) : msg, static_cast<bool>(ec), 0);
    }

    if (target == LinkState::Closed || target == LinkState::Error) {
      if (terminal_state_notified_.exchange(true)) {
        return;
      }
    } else if (current == target) {
      return;
    }

    state_.set_state(target);
    notify_state();
  }

  void perform_stop_cleanup() {
    try {
      close_socket();
      writing_ = false;
      OnBackpressure on_bp;
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bp = on_bp_;
      }
      auto f = bp_fields();
      queue_util::drain_and_clear_backpressure(f, on_bp, [&]() {
        tx_.clear();
        queue_bytes_ = 0;
        pending_.clear();
        pending_bytes_ = 0;
      });
      connected_.store(false);
      opened_.store(false);
      if (owns_ioc_ && work_guard_) {
        work_guard_->reset();
      }
      transition_to(LinkState::Closed);
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bytes_ = nullptr;
        on_state_ = nullptr;
        on_bp_ = nullptr;
      }
    } catch (...) {
    }
  }

  void join_ioc_thread(bool allow_detach) {
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
    } catch (...) {
    }
  }
};

std::shared_ptr<UdpChannel> UdpChannel::create(const config::UdpConfig& cfg) {
  return std::shared_ptr<UdpChannel>(new UdpChannel(cfg));
}

std::shared_ptr<UdpChannel> UdpChannel::create(const config::UdpConfig& cfg, net::io_context& ioc) {
  return std::shared_ptr<UdpChannel>(new UdpChannel(cfg, ioc));
}

UdpChannel::UdpChannel(const config::UdpConfig& cfg) : impl_(std::make_unique<Impl>(cfg)) {}

UdpChannel::UdpChannel(const config::UdpConfig& cfg, net::io_context& ioc) : impl_(std::make_unique<Impl>(cfg, ioc)) {}

UdpChannel::~UdpChannel() {
  if (impl_) {
    // Cannot use shared_from_this in destructor. Use internal cleanup directly.
    impl_->stop_requested_.store(true);
    impl_->stopping_.store(true);
    impl_->perform_stop_cleanup();
    impl_->join_ioc_thread(true);
  }
}

UdpChannel::UdpChannel(UdpChannel&&) noexcept = default;
UdpChannel& UdpChannel::operator=(UdpChannel&&) noexcept = default;

void UdpChannel::start() {
  auto impl = get_impl();
  if (impl->started_) return;
  if (!impl->cfg_.is_valid()) {
    throw std::runtime_error("Invalid UDP configuration");
  }

  if (impl->owns_ioc_ && impl->owned_ioc_ && impl->owned_ioc_->stopped()) {
    impl->owned_ioc_->restart();
  }

  if (impl->ioc_thread_.joinable()) {
    impl->join_ioc_thread(false);
  }

  if (impl->owns_ioc_) {
    impl->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl->ioc_->get_executor());
  }

  auto self = shared_from_this();
  net::dispatch(impl->strand_, [self]() {
    auto impl = self->get_impl();
    impl->stop_requested_.store(false);
    impl->stopping_.store(false);
    impl->terminal_state_notified_.store(false);
    impl->connected_.store(false);
    impl->opened_.store(false);
    impl->writing_ = false;
    impl->queue_bytes_ = 0;
    impl->backpressure_active_ = false;
    impl->state_.set_state(LinkState::Idle);

    impl->transition_to(LinkState::Connecting);
    impl->open_socket(self);
  });

  if (impl->owns_ioc_) {
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_->stop(); });
        impl->ioc_->run();
      } catch (...) {
      }
    });
  }

  impl->started_ = true;
}

void UdpChannel::stop() {
  auto impl = get_impl();
  if (impl->stop_requested_.exchange(true)) return;

  if (!impl->started_) {
    impl->transition_to(LinkState::Closed);
    std::lock_guard<std::mutex> lock(impl->callback_mtx_);
    impl->on_bytes_ = nullptr;
    impl->on_state_ = nullptr;
    impl->on_bp_ = nullptr;
    return;
  }

  impl->stopping_.store(true);
  auto self = shared_from_this();
  net::post(impl->strand_, [self]() { self->get_impl()->perform_stop_cleanup(); });

  impl->join_ioc_thread(false);

  if (impl->owns_ioc_ && impl->owned_ioc_) {
    impl->owned_ioc_->restart();
  }

  impl->started_ = false;
}

bool UdpChannel::is_connected() const { return get_impl()->connected_.load(); }
bool UdpChannel::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
wrapper::RuntimeStats UdpChannel::stats() const {
  auto impl = get_impl();
  return impl->stats_.snapshot(impl->queue_bytes_.load(std::memory_order_relaxed),
                               impl->pending_bytes_.load(std::memory_order_relaxed),
                               impl->backpressure_active_.load(std::memory_order_relaxed));
}
void UdpChannel::reset_stats() {
  auto impl = get_impl();
  impl->stats_.reset(impl->queue_bytes_.load(std::memory_order_relaxed) +
                     impl->pending_bytes_.load(std::memory_order_relaxed));
}

std::optional<diagnostics::ErrorInfo> UdpChannel::last_error_info() const {
  return get_impl()->error_info_holder_.last_error_info();
}

bool UdpChannel::async_write_copy(memory::ConstByteSpan data) {
  auto impl = get_impl();
  if (data.empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stop_requested_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (!impl->remote_endpoint_) {
    impl->stats_.record_failed_send();
    return false;
  }

  size_t size = data.size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("udp", "write", "Write size exceeds maximum allowed");
    impl->stats_.record_failed_send();
    return false;
  }

  if (impl->cfg_.enable_memory_pool && size <= 65536) {
    memory::PooledBuffer pooled(size);
    if (pooled.valid()) {
      base::safe_memory::safe_memcpy(pooled.data(), data.data(), size);
      if (impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
        impl->stats_.record_failed_send();
        return false;
      }
      impl->stats_.record_accepted(size);
      net::post(impl->strand_, [self = shared_from_this(), buf = std::move(pooled), size]() mutable {
        auto impl = self->get_impl();
        if (!impl->enqueue_buffer(self, std::move(buf), size)) return;
        impl->do_write(self);
      });
      return true;
    }
  }

  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(copy), size]() mutable {
    auto impl = self->get_impl();
    if (!impl->enqueue_buffer(self, std::move(buf), size)) return;
    impl->do_write(self);
  });
  return true;
}

bool UdpChannel::async_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  auto size = data.size();
  if (size == 0) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stop_requested_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (!impl->remote_endpoint_) {
    impl->stats_.record_failed_send();
    return false;
  }

  if (size > impl->bp_limit_) {
    UNILINK_LOG_ERROR("udp", "write", "Queue limit exceeded by single write");
    impl->transition_to(LinkState::Error, {}, "write", "Queue limit exceeded by single write");
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
    impl->stats_.record_failed_send();
    return false;
  }
  impl->stats_.record_accepted(size);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), size]() mutable {
    auto impl = self->get_impl();
    if (!impl->enqueue_buffer(self, std::move(buf), size)) return;
    impl->do_write(self);
  });
  return true;
}

bool UdpChannel::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (!data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stop_requested_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (!impl->remote_endpoint_) {
    UNILINK_LOG_WARNING("udp", "write", "Remote endpoint not set; dropping write request");
    impl->stats_.record_failed_send();
    return false;
  }

  auto size = data->size();
  if (size > impl->bp_limit_) {
    UNILINK_LOG_ERROR("udp", "write", "Queue limit exceeded by single write");
    impl->transition_to(LinkState::Error, {}, "write", "Queue limit exceeded by single write");
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
    impl->stats_.record_failed_send();
    return false;
  }
  impl->stats_.record_accepted(size);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), size]() mutable {
    auto impl = self->get_impl();
    if (!impl->enqueue_buffer(self, std::move(buf), size)) return;
    impl->do_write(self);
  });
  return true;
}

bool UdpChannel::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty() || data.size() > base::constants::MAX_BUFFER_SIZE) {
    get_impl()->stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool UdpChannel::async_try_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
      impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto size = data.size();
  if (size == 0 || size > base::constants::MAX_BUFFER_SIZE) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [impl, size]() {
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl->stats_.record_dropped(1, size);
    } else {
      impl->stats_.record_failed_send();
    }
  };
  if (impl->backpressure_active_.load() || impl->queue_bytes_ + size > impl->bp_high_ ||
      impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl->stats_.record_accepted(size);

  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), size]() mutable {
    auto impl = self->get_impl();
    if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back({std::move(buf), std::nullopt});
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    impl->do_write(self);
  });
  return true;
}

bool UdpChannel::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
      impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_ || !data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto size = data->size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [impl, size]() {
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl->stats_.record_dropped(1, size);
    } else {
      impl->stats_.record_failed_send();
    }
  };
  if (impl->backpressure_active_.load() || impl->queue_bytes_ + size > impl->bp_high_ ||
      impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  impl->stats_.record_accepted(size);

  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(data), size]() mutable {
    auto impl = self->get_impl();
    if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back({std::move(buf), std::nullopt});
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    impl->do_write(self);
  });
  return true;
}

void UdpChannel::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(cb);
}

void UdpChannel::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(cb);
}

void UdpChannel::on_backpressure(OnBackpressure cb) {
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(cb);
}

void UdpChannel::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

bool UdpChannel::async_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination) {
  auto impl = get_impl();
  if (data.empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stop_requested_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }

  size_t size = data.size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("udp", "write_to", "Write size exceeds maximum allowed");
    impl->stats_.record_failed_send();
    return false;
  }
  if (impl->cfg_.enable_memory_pool && size <= 65536) {
    memory::PooledBuffer pooled(size);
    if (pooled.valid()) {
      base::safe_memory::safe_memcpy(pooled.data(), data.data(), size);
      if (impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
        impl->stats_.record_failed_send();
        return false;
      }
      impl->stats_.record_accepted(size);
      net::post(impl->strand_, [self = shared_from_this(), buf = std::move(pooled), size, destination]() mutable {
        auto impl = self->get_impl();
        if (!impl->enqueue_buffer(self, std::move(buf), size, destination)) return;
        impl->do_write(self);
      });
      return true;
    }
  }

  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(copy), size, destination]() mutable {
    auto impl = self->get_impl();
    if (!impl->enqueue_buffer(self, std::move(buf), size, destination)) return;
    impl->do_write(self);
  });
  return true;
}

bool UdpChannel::async_try_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination) {
  auto impl = get_impl();
  if (data.empty() || impl->stop_requested_.load() || impl->stopping_.load() ||
      impl->state_.is_state(LinkState::Closed) || impl->state_.is_state(LinkState::Error)) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto size = data.size();
  if (size > base::constants::MAX_BUFFER_SIZE) {
    impl->stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [impl, size]() {
    if (impl->bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      impl->stats_.record_dropped(1, size);
    } else {
      impl->stats_.record_failed_send();
    }
  };
  if (impl->backpressure_active_.load() || impl->queue_bytes_ + size > impl->bp_high_ ||
      impl->queue_bytes_ + impl->pending_bytes_ + size > impl->bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return false;
  }

  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  net::post(impl->strand_, [self = shared_from_this(), buf = std::move(copy), size, destination]() mutable {
    auto impl = self->get_impl();
    if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back({std::move(buf), destination});
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    impl->do_write(self);
  });
  return true;
}

void UdpChannel::on_bytes_from(OnBytesFrom cb) { impl_->on_bytes_from_ = std::move(cb); }

boost::asio::ip::udp::endpoint UdpChannel::local_endpoint() const { return get_impl()->local_endpoint_; }

boost::asio::any_io_executor UdpChannel::get_executor() { return get_impl()->ioc_->get_executor(); }

}  // namespace transport
}  // namespace unilink
