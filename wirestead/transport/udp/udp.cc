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

#include "wirestead/transport/udp/udp.hpp"

#include <spdlog/fmt/fmt.h>

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/diagnostics/send_accounting.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/base/bp_state_machine.hpp"
#include "wirestead/transport/base/bp_utils.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/udp/detail/write_wait.hpp"
#include "wirestead/wrapper/send_validation.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using udp = net::ip::udp;
using base::LinkState;
using concurrency::AtomicLinkState;

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
  using Ledger = diagnostics::SendAccountingLedger;
  Ledger send_accounting_;
  struct TxItem {
    BufferVariant buffer;
    std::optional<udp::endpoint> destination;
    Ledger::Request request;
  };

  std::array<uint8_t, 65536> rx_{};
  std::deque<TxItem> tx_;
  std::deque<TxItem> pending_;
  std::atomic<size_t> pending_bytes_{0};
  bool writing_{false};
  std::atomic<size_t> queue_bytes_{0};
  // Bytes accepted by a plain async_write_* call but not yet routed onto the
  // strand - reserved via try_reserve_limit_bytes() to close the
  // accept-then-drop race (jwsung91/wirestead#517). inflight_bytes_ mutations
  // and the queue_bytes_/pending_bytes_ increments that promote a
  // reservation both go through write_reserve_mtx_ - see bp_utils.hpp.
  std::atomic<size_t> inflight_bytes_{0};
  std::mutex write_reserve_mtx_;
  config::UdpConfig cfg_;
  // #443: per-channel pool instead of the process-wide GlobalMemoryPool
  // singleton - avoids cross-channel contention on the singleton's bucket
  // mutexes. Capacity is much smaller than the old shared default since
  // it's no longer amortized across every channel in the process.
  // Prefill stays 0. This literal was written while MemoryPool discarded
  // initial_pool_size, so 50 allocated nothing; #575 made the parameter real
  // and turned it into ~1 MiB eagerly allocated per channel at construction.
  // The pool fills as buffers are released.
  memory::MemoryPool pool_{0, 200};
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
  std::atomic<bool> started_{false};
  AtomicLinkState state_{LinkState::Idle};
  std::atomic<bool> terminal_state_notified_{false};

  std::atomic<uint64_t> generation_{0};
  std::mutex submission_mtx_;
  std::mutex stop_mtx_;
  std::condition_variable stop_cv_;
  bool cleanup_done_ = false;
  bool cleanup_started_ = false;
  bool cleanup_finished_ = false;
  size_t pending_io_ = 0;  // Strand-confined.
  std::mutex join_mtx_;
  static inline thread_local Impl* active_cleanup_ = nullptr;

  std::vector<std::weak_ptr<detail::UdpWriteWait>> write_waits_;
  // Caller holds submission_mtx_. A UDP run cannot change its selected remote.
  std::optional<wrapper::SendRejection> admission_rejection(bool require_remote,
                                                            std::optional<uint64_t> expected_run = std::nullopt) {
    if (stop_requested_.load()) {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      return cleanup_done_ ? wrapper::SendRejection::NotStarted : wrapper::SendRejection::Stopping;
    }
    if (!started_) return wrapper::SendRejection::NotStarted;
    if ((expected_run && *expected_run != generation_) || !opened_ || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error) || (require_remote && !remote_endpoint_))
      return wrapper::SendRejection::NotReady;
    return std::nullopt;
  }
  void end_write_waits(wrapper::SendRejection reason) {
    for (auto& weak : write_waits_)
      if (auto wait = weak.lock()) wait->end(reason);
    write_waits_.clear();
  }

  // Caller holds submission_mtx_. For UDP, loss means terminal local socket
  // failure; there is no peer connection or delivery acknowledgement.
  void fail_writes_locked() {
    send_accounting_.end(Ledger::Cause::ConnectionLoss);
    opened_ = false;
    connected_ = false;
    end_write_waits(wrapper::SendRejection::NotReady);
  }

  void mark_cleanup_done() {
    detail::stop_test_hook(this, true);
    if (owns_ioc_) work_guard_.reset();
    started_ = false;
    {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      cleanup_done_ = true;
    }
    stop_cv_.notify_all();
  }

  struct IoCompletion {
    Impl* impl;
    ~IoCompletion() {
      if (impl->cleanup_finished_ && impl->pending_io_ == 1) {
        if (auto hook = detail::g_udp_io_completion_hook.load()) hook();
      }
      if (--impl->pending_io_ == 0 && impl->cleanup_finished_) impl->mark_cleanup_done();
    }
  };

  // Guards on_bytes_/on_bytes_from_/on_state_/on_bp_. Setters (called from
  // any user thread) and the strand-confined read sites below both take
  // this lock; readers copy the callback under lock then invoke the copy
  // outside the lock, matching the pattern already used correctly by
  // TcpClient/UdsClient/both servers (see #436).
  mutable std::mutex callback_mtx_;
  // Shared snapshots: the strand copies one out per received datagram, and a
  // std::function copy allocates whenever the target outgrows its small-object
  // buffer. See interface::SharedCallback.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<UdpChannel::OnBytesFrom> on_bytes_from_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;

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
      WIRESTEAD_LOG_ERROR("udp", "bind", msg);
      transition_to(LinkState::Error, ec, "bind", msg);
      return;
    }

    local_endpoint_ = udp::endpoint(address, cfg_.local_port);
    socket_.open(local_endpoint_.protocol(), ec);
    if (ec) {
      std::string msg = fmt::format("Socket open failed: {}", ec.message());
      WIRESTEAD_LOG_ERROR("udp", "open", msg);
      transition_to(LinkState::Error, ec, "open", msg);
      return;
    }

    if (cfg_.reuse_address) {
      socket_.set_option(net::socket_base::reuse_address(true), ec);
      if (ec) {
        std::string msg = fmt::format("Failed to set reuse_address: {}", ec.message());
        WIRESTEAD_LOG_ERROR("udp", "open", msg);
        transition_to(LinkState::Error, ec, "open", msg);
        return;
      }
    }

    if (cfg_.enable_broadcast) {
      socket_.set_option(net::socket_base::broadcast(true), ec);
      if (ec) {
        std::string msg = fmt::format("Failed to set broadcast: {}", ec.message());
        WIRESTEAD_LOG_ERROR("udp", "open", msg);
        transition_to(LinkState::Error, ec, "open", msg);
        return;
      }
    }

    socket_.bind(local_endpoint_, ec);
    if (ec) {
      std::string msg = fmt::format("Bind failed: {}", ec.message());
      WIRESTEAD_LOG_ERROR("udp", "bind", msg);
      transition_to(LinkState::Error, ec, "bind", msg);
      return;
    }

    // After bind, which is what a group join attaches to. Fatal on failure:
    // a receiver that silently did not join looks identical to a sensor that
    // stopped sending, and that is the exact confusion this whole feature is
    // meant to remove.
    if (cfg_.multicast_group) {
      const auto group = net::ip::make_address(*cfg_.multicast_group, ec);
      if (!ec) {
        if (group.is_v4() && cfg_.multicast_interface) {
          const auto iface = net::ip::make_address_v4(*cfg_.multicast_interface, ec);
          if (!ec) socket_.set_option(net::ip::multicast::join_group(group.to_v4(), iface), ec);
        } else {
          socket_.set_option(net::ip::multicast::join_group(group), ec);
        }
      }
      if (ec) {
        std::string msg = fmt::format("Failed to join multicast group {}: {}", *cfg_.multicast_group, ec.message());
        WIRESTEAD_LOG_ERROR("udp", "multicast", msg);
        transition_to(LinkState::Error, ec, "multicast", msg);
        return;
      }
      WIRESTEAD_LOG_INFO("udp", "multicast", fmt::format("Joined multicast group {}", *cfg_.multicast_group));
    }

    // Set large OS buffers for UDP to prevent drops unless explicitly configured.
    const int automatic_buf_size = std::max(static_cast<int>(bp_high_), 4 * 1024 * 1024);
    const int recv_buf_size =
        cfg_.receive_buffer_size > 0 ? static_cast<int>(cfg_.receive_buffer_size) : automatic_buf_size;
    const int send_buf_size = cfg_.send_buffer_size > 0 ? static_cast<int>(cfg_.send_buffer_size) : automatic_buf_size;

    socket_.set_option(net::socket_base::receive_buffer_size(recv_buf_size), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("udp", "open", fmt::format("Failed to set receive buffer size: {}", ec.message()));
      ec.clear();
    }
    socket_.set_option(net::socket_base::send_buffer_size(send_buf_size), ec);
    if (ec) {
      WIRESTEAD_LOG_WARNING("udp", "open", fmt::format("Failed to set send buffer size: {}", ec.message()));
      ec.clear();
    }

    {
      std::lock_guard<std::mutex> lock(submission_mtx_);
      if (stop_requested_) return;
      opened_.store(true);
      connected_.store(remote_endpoint_.has_value());
    }
    if (remote_endpoint_) {
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

    ++pending_io_;
    socket_.async_receive_from(net::buffer(rx_), recv_endpoint_,
                               [self](const boost::system::error_code& ec, std::size_t bytes) {
                                 auto impl = self->get_impl();
                                 IoCompletion completed{impl};
                                 impl->handle_receive(self, ec, bytes);
                               });
  }

  void handle_receive(std::shared_ptr<UdpChannel> self, boost::system::error_code ec, std::size_t bytes) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }

    if (stopping_.load() || stop_requested_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error)) {
      return;
    }

    if (auto hook = detail::g_udp_receive_result_hook.load()) hook(ec);

    if (ec == boost::asio::error::message_size || bytes >= rx_.size()) {
      WIRESTEAD_LOG_ERROR("udp", "receive", "Datagram truncated (buffer too small)");
      transition_to(LinkState::Error, ec, "receive", "Datagram truncated (buffer too small)");
      return;
    }

    if (ec) {
      std::string msg = fmt::format("Receive failed: {}", ec.message());
      WIRESTEAD_LOG_ERROR("udp", "receive", msg);
      transition_to(LinkState::Error, ec, "receive", msg);
      return;
    }

    bool learned_remote = false;
    {
      std::lock_guard<std::mutex> lock(submission_mtx_);
      if (!remote_endpoint_) {
        remote_endpoint_ = recv_endpoint_;
        connected_.store(true);
        learned_remote = true;
      }
    }
    if (learned_remote) transition_to(LinkState::Connected);

    // #435: once a remote peer is configured or locked in (above), only
    // deliver data from that exact sender through on_bytes() - the
    // point-to-point API. Without this, any other host could send spoofed
    // datagrams after the fact and have them treated as legitimate data.
    // on_bytes_from() is intentionally NOT filtered here: it's the
    // multi-sender API (used by the UdpServer wrapper), which tracks and
    // trusts each sender as its own session by design.
    const bool from_established_remote = remote_endpoint_ && recv_endpoint_ == *remote_endpoint_;

    if (bytes > 0) {
      stats_.record_received(bytes);
      interface::SharedCallback<OnBytes> on_bytes;
      interface::SharedCallback<UdpChannel::OnBytesFrom> on_bytes_from;
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bytes = on_bytes_;
        on_bytes_from = on_bytes_from_;
      }
      if (on_bytes && from_established_remote) {
        try {
          (*on_bytes)(memory::ConstByteSpan(rx_.data(), bytes));
        } catch (const std::exception& e) {
          std::string msg = fmt::format("Exception in bytes callback: {}", e.what());
          WIRESTEAD_LOG_ERROR("udp", "on_bytes", msg);
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes", msg);
            return;
          }
        } catch (...) {
          WIRESTEAD_LOG_ERROR("udp", "on_bytes", "Unknown exception in bytes callback");
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes", "Unknown exception in bytes callback");
            return;
          }
        }
      }

      if (on_bytes_from) {
        try {
          (*on_bytes_from)(memory::ConstByteSpan(rx_.data(), bytes), recv_endpoint_);
        } catch (const std::exception& e) {
          std::string msg = fmt::format("Exception in bytes callback: {}", e.what());
          WIRESTEAD_LOG_ERROR("udp", "on_bytes_from", msg);
          if (cfg_.stop_on_callback_exception) {
            transition_to(LinkState::Error, {}, "on_bytes_from", msg);
            return;
          }
        } catch (...) {
          WIRESTEAD_LOG_ERROR("udp", "on_bytes_from", "Unknown exception in bytes callback");
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
    interface::SharedCallback<OnBackpressure> on_bp;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_bp = on_bp_;
    }
    static const OnBackpressure kNoCallback;
    auto f = bp_fields();
    queue_util::drain_and_clear_backpressure(f, on_bp ? *on_bp : kNoCallback, [&]() {
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

    std::unique_lock<std::mutex> submission_lock(submission_mtx_);
    if (stop_requested_.load() || !opened_.load()) return;
    auto current = std::move(tx_.front());
    tx_.pop_front();

    const auto& dest_endpoint = current.destination ? current.destination : remote_endpoint_;

    if (!dest_endpoint) {
      WIRESTEAD_LOG_WARNING("udp", "write", "Remote endpoint not set; dropping write request");
      send_accounting_.discard(current.request, Ledger::Cause::ConnectionLoss);
      const auto bytes = std::visit([](const auto& b) { return queue_util::variant_buffer_size(b); }, current.buffer);
      queue_util::release_reserved_write_bytes(queue_bytes_, bytes);
      writing_ = false;
      submission_lock.unlock();
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

    const auto request = current.request;
    const auto generation = generation_.load();
    send_accounting_.begin(request);
    auto on_write = [self, bytes_queued, request, generation](boost::system::error_code ec, std::size_t bytes_written) {
      auto impl = self->get_impl();
      {
        std::lock_guard<std::mutex> lock(impl->submission_mtx_);
        if (generation != impl->generation_) return;
        impl->send_accounting_.complete(request, bytes_written);
        if (!ec && bytes_written != bytes_queued) ec = net::error::message_size;
        if (ec && !impl->stop_requested_) {
          impl->fail_writes_locked();
          if (ec == net::error::operation_aborted) ec = net::error::connection_aborted;
        }
      }
      impl->queue_bytes_ = (impl->queue_bytes_ > bytes_queued) ? (impl->queue_bytes_ - bytes_queued) : 0;
      impl->report_backpressure(self, impl->queue_bytes_);

      if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
          impl->state_.is_state(LinkState::Error)) {
        impl->writing_ = false;
        impl->drain_queue_and_clear_backpressure();
        return;
      }

      if (ec) {
        std::string msg = fmt::format("Send failed: {}", ec.message());
        WIRESTEAD_LOG_ERROR("udp", "write", msg);
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

    ++pending_io_;
    try {
      if (auto hook = detail::g_udp_write_initiation_hook.load()) hook();
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

            socket_.async_send_to(net::buffer(data_ptr, size), *dest_endpoint,
                                  [self, buf_captured = std::move(buf), on_write = std::move(on_write)](
                                      const boost::system::error_code& ec, std::size_t bytes) mutable {
                                    IoCompletion completed{self->get_impl()};
                                    // Return pooled storage while its owning transport is alive,
                                    // before signalling the run's I/O completion.
                                    auto buffer = std::move(buf_captured);
                                    (void)buffer;
                                    on_write(ec, bytes);
                                  });
          },
          std::move(current.buffer));
    } catch (...) {
      --pending_io_;
      fail_writes_locked();
      submission_lock.unlock();
      writing_ = false;
      transition_to(LinkState::Error, make_error_code(boost::system::errc::no_buffer_space), "write",
                    "Failed to initiate datagram write");
      return;
    }
    submission_lock.unlock();
    if (auto hook = detail::g_udp_write_started_hook.load()) hook();
  }

  void close_socket() {
    boost::system::error_code ec;
    socket_.cancel(ec);
    socket_.close(ec);
  }

  void notify_state() {
    interface::SharedCallback<OnState> on_state;
    {
      std::lock_guard<std::mutex> lock(callback_mtx_);
      on_state = on_state_;
    }
    if (!on_state) return;
    try {
      (*on_state)(state_.get());
    } catch (const std::exception& e) {
      WIRESTEAD_LOG_ERROR("udp", "on_state", fmt::format("Exception in state callback: {}", e.what()));
    } catch (...) {
      WIRESTEAD_LOG_ERROR("udp", "on_state", "Unknown exception in state callback");
    }
  }

  void report_backpressure(std::shared_ptr<UdpChannel> self, size_t queued_bytes) {
    if (stop_requested_.load()) return;
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

  bool enqueue_buffer(std::shared_ptr<UdpChannel> self, BufferVariant&& buffer, size_t size, Ledger::Request request,
                      std::optional<udp::endpoint> dest = std::nullopt) {
    if (stopping_.load() || stop_requested_.load() || state_.is_state(LinkState::Closed) ||
        state_.is_state(LinkState::Error)) {
      queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, size);
      stats_.record_failed_send();
      return false;
    }

    auto f = bp_fields();
    queue_util::DropAccounting dropped;
    // TxItem carries a destination endpoint alongside the BufferVariant that
    // decide_enqueue()'s BestEffort trim needs to visit - project onto
    // `.buffer` rather than the item itself (#434).
    auto decision = queue_util::decide_enqueue(
        f, size, tx_, dropped, [](TxItem& item) -> BufferVariant& { return item.buffer; },
        [this](const TxItem& item) { send_accounting_.discard(item.request, Ledger::Cause::QueuePressure); });
    if (dropped.any()) {
      stats_.record_dropped(dropped.messages, dropped.bytes);
    }

    if (decision == queue_util::EnqueueDecision::Rejected) {
      send_accounting_.discard(request, Ledger::Cause::QueuePressure);
      WIRESTEAD_LOG_ERROR("udp", "write",
                          fmt::format("Queue limit exceeded ({} bytes)", queue_bytes_ + pending_bytes_ + size));
      // Always reporting here (rather than only for the non-Reliable-pending
      // rejection path, as the pre-#434 code did) is a no-op in practice for
      // the Reliable+pending case: queued_bytes is already >= bp_high_ or
      // this rejection couldn't have happened, so report_backpressure()'s
      // OFF-transition check (<= bp_low_) can't fire here either way.
      // #448: record as dropped so it's reflected in RuntimeStats instead of
      // silently vanishing after being counted as accepted.
      stats_.record_dropped(1, size);
      queue_util::release_reserved_limit_bytes(write_reserve_mtx_, inflight_bytes_, size);
      report_backpressure(self, queue_bytes_ + size);
      return false;
    }

    if (decision == queue_util::EnqueueDecision::Pending) {
      queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, pending_bytes_, inflight_bytes_, size);
      pending_.push_back({std::move(buffer), dest, request});
      observe_queue();
      return true;
    }

    queue_util::commit_reserved_limit_bytes(write_reserve_mtx_, queue_bytes_, inflight_bytes_, size);
    tx_.push_back({std::move(buffer), dest, request});
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

    const auto current = state_.get();
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

    {
      std::lock_guard<std::mutex> lock(submission_mtx_);
      if (target == LinkState::Closed || target == LinkState::Error) fail_writes_locked();
      state_.set(target);
    }
    if (target == LinkState::Closed || target == LinkState::Error) drain_queue_and_clear_backpressure();
    notify_state();
  }

  void perform_stop_cleanup() {
    if (cleanup_started_) return;
    cleanup_started_ = true;
    struct CompletionSignal {
      Impl* impl;
      Impl* previous;
      ~CompletionSignal() {
        impl->cleanup_finished_ = true;
        if (impl->pending_io_ == 0) impl->mark_cleanup_done();
        active_cleanup_ = previous;
      }
    } completion{this, active_cleanup_};
    active_cleanup_ = this;
    try {
      close_socket();
      writing_ = false;
      interface::SharedCallback<OnBackpressure> on_bp;
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bp = on_bp_;
      }
      static const OnBackpressure kNoCallback;
      auto f = bp_fields();
      queue_util::drain_and_clear_backpressure(f, on_bp ? *on_bp : kNoCallback, [&]() {
        tx_.clear();
        queue_bytes_ = 0;
        pending_.clear();
        pending_bytes_ = 0;
      });
      connected_.store(false);
      opened_.store(false);
      transition_to(LinkState::Closed);
      {
        std::lock_guard<std::mutex> lock(callback_mtx_);
        on_bytes_ = nullptr;
        on_bytes_from_ = nullptr;
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
  if (impl_) stop();
}

UdpChannel::UdpChannel(UdpChannel&&) noexcept = default;
UdpChannel& UdpChannel::operator=(UdpChannel&&) noexcept = default;

void UdpChannel::start() {
  auto impl = get_impl();
  if (impl->started_) return;
  if (!impl->cfg_.is_valid()) throw std::runtime_error("Invalid UDP configuration");
  if (impl->ioc_thread_.joinable()) impl->join_ioc_thread(false);
  if (impl->owns_ioc_ && impl->ioc_->stopped()) impl->ioc_->restart();

  const auto generation = impl->generation_.fetch_add(1) + 1;
  {
    std::lock_guard<std::mutex> lock(impl->submission_mtx_);
    impl->remote_endpoint_.reset();
    impl->set_remote_from_config();
    impl->inflight_bytes_ = 0;
  }
  {
    std::lock_guard<std::mutex> lock(impl->stop_mtx_);
    impl->cleanup_done_ = false;
  }
  impl->cleanup_started_ = false;
  impl->cleanup_finished_ = false;
  impl->stop_requested_ = false;
  impl->stopping_ = false;
  impl->terminal_state_notified_ = false;
  impl->connected_ = false;
  impl->opened_ = false;
  impl->writing_ = false;
  impl->queue_bytes_ = 0;
  impl->pending_bytes_ = 0;
  impl->backpressure_active_ = false;
  impl->state_.set(LinkState::Idle);
  impl->started_ = true;

  if (impl->owns_ioc_) {
    impl->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl->ioc_->get_executor());
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_->stop(); });
        impl->ioc_->run();
      } catch (...) {
      }
    });
  }
  net::post(impl->strand_, [self = shared_from_this(), generation] {
    auto impl = self->get_impl();
    if (impl->stop_requested_ || generation != impl->generation_) return;
    impl->transition_to(LinkState::Connecting);
    impl->open_socket(self);
  });
}

void UdpChannel::stop() {
  auto impl = get_impl();
  if (Impl::active_cleanup_ == impl) return;
  const bool on_executor = impl->ioc_->get_executor().running_in_this_thread();
  bool inline_cleanup = false;
  {
    // Accepted writes are posted before this cleanup request.
    std::lock_guard<std::mutex> lock(impl->submission_mtx_);
    if (!impl->stop_requested_.exchange(true)) {
      impl->send_accounting_.end(Impl::Ledger::Cause::ExplicitStop);
      impl->end_write_waits(wrapper::SendRejection::CancelledWhileWaiting);
      impl->stopping_ = true;
      if (impl->generation_.load() == 0) {
        inline_cleanup = true;
      } else {
        auto self = weak_from_this().lock();
        net::post(impl->strand_, [self, impl] { impl->perform_stop_cleanup(); });
      }
    }
  }
  if (inline_cleanup) impl->perform_stop_cleanup();
  if (on_executor) return;
  detail::stop_test_hook(impl, false);
  {
    std::unique_lock<std::mutex> lock(impl->stop_mtx_);
    impl->stop_cv_.wait(lock, [impl] { return impl->cleanup_done_; });
  }
  std::lock_guard<std::mutex> join_lock(impl->join_mtx_);
  impl->join_ioc_thread(false);
}

bool UdpChannel::is_connected() const { return get_impl()->connected_.load(); }
bool UdpChannel::is_backpressure_active() const { return get_impl()->backpressure_active_.load(); }
std::optional<size_t> UdpChannel::write_queue_limit() const { return get_impl()->bp_limit_; }
wrapper::RuntimeStats UdpChannel::stats() const {
  auto impl = get_impl();
  auto result = impl->stats_.snapshot(impl->queue_bytes_.load(std::memory_order_relaxed),
                                      impl->pending_bytes_.load(std::memory_order_relaxed),
                                      impl->backpressure_active_.load(std::memory_order_relaxed));
  result.send_accounting = impl->send_accounting_.snapshot();
  return result;
}
void UdpChannel::reset_stats() {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->submission_mtx_);
  impl->send_accounting_.reset();
  impl->stats_.reset(impl->queue_bytes_.load(std::memory_order_relaxed) +
                     impl->pending_bytes_.load(std::memory_order_relaxed));
}

std::optional<diagnostics::ErrorInfo> UdpChannel::last_error_info() const {
  return get_impl()->error_info_holder_.last_error_info();
}

std::shared_ptr<detail::UdpWriteWait> UdpChannel::capture_write_wait(bool require_remote) {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->admission_rejection(require_remote)) return {};
  auto wait = std::make_shared<detail::UdpWriteWait>(
      detail::UdpWriteWait{impl_->generation_.load(), require_remote, std::nullopt});
  std::erase_if(impl_->write_waits_, [](const auto& weak) { return weak.expired(); });
  impl_->write_waits_.push_back(wait);
  return wait;
}
std::optional<wrapper::SendResult> UdpChannel::poll_write_wait(const std::shared_ptr<detail::UdpWriteWait>& wait) {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (!wait) return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  if (wait->ended_by) return wrapper::SendResult::reject(*wait->ended_by);
  if (auto reason = impl_->admission_rejection(wait->require_remote, wait->sequence))
    return wrapper::SendResult::reject(*reason);
  if (!impl_->backpressure_active_) return wrapper::SendResult::accept();
  return std::nullopt;
}
void UdpChannel::end_write_wait(const std::shared_ptr<detail::UdpWriteWait>& wait, wrapper::SendRejection reason) {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (wait) wait->end(reason);
}
void UdpChannel::cancel_write_waits() {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  impl_->end_write_waits(wrapper::SendRejection::CancelledWhileWaiting);
}
std::optional<uint64_t> UdpChannel::write_connection() const {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (impl_->admission_rejection(true)) return std::nullopt;
  return impl_->generation_.load();
}
wrapper::SendResult UdpChannel::write_state(bool require_remote) {
  std::lock_guard<std::mutex> lock(impl_->submission_mtx_);
  if (auto reason = impl_->admission_rejection(require_remote)) return wrapper::SendResult::reject(*reason);
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdpChannel::async_write_copy_result(memory::ConstByteSpan data) {
  const auto result = write_copy(data);
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data.size();
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(true, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (impl->cfg_.enable_memory_pool && size <= 65536) {
    memory::PooledBuffer pooled(size, impl->pool_);
    if (pooled.valid()) {
      base::safe_memory::safe_memcpy(pooled.data(), data.data(), size);
      if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                               impl->inflight_bytes_, size, impl->bp_limit_)) {
        impl->stats_.record_failed_send();
        return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
      }
      impl->stats_.record_accepted(size);
      Impl::Ledger::Admission admission(impl->send_accounting_, size);
      const auto request = admission.request();
      net::post(impl->strand_,
                [self = shared_from_this(), generation, buf = std::move(pooled), size, request]() mutable {
                  auto impl = self->get_impl();
                  if (generation != impl->generation_) return;
                  if (!impl->enqueue_buffer(self, std::move(buf), size, request)) return;
                  impl->do_write(self);
                });
      admission.commit();
      return wrapper::SendResult::accept();
    }
  }

  if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                           impl->inflight_bytes_, size, impl->bp_limit_)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();
  net::post(impl->strand_, [self = shared_from_this(), generation, buf = std::move(copy), size, request]() mutable {
    auto impl = self->get_impl();
    if (generation != impl->generation_) return;
    if (!impl->enqueue_buffer(self, std::move(buf), size, request)) return;
    impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdpChannel::async_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = write_move(std::move(data));
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data.size();
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(true, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (size > impl->bp_limit_) {
    WIRESTEAD_LOG_ERROR("udp", "write", "Queue limit exceeded by single write");
    // Keep error callbacks ordered with the same run's cleanup.
    net::post(impl->strand_, [self = shared_from_this(), generation] {
      auto impl = self->get_impl();
      if (generation != impl->generation_ || impl->stop_requested_) return;
      impl->transition_to(LinkState::Error, {}, "write", "Queue limit exceeded by single write");
    });
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }
  if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                           impl->inflight_bytes_, size, impl->bp_limit_)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();
  net::post(impl->strand_, [self = shared_from_this(), generation, buf = std::move(data), size, request]() mutable {
    auto impl = self->get_impl();
    if (generation != impl->generation_) return;
    if (!impl->enqueue_buffer(self, std::move(buf), size, request)) return;
    impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdpChannel::async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = write_shared(std::move(data));
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                             std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data ? data->size() : 0;
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(true, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (size > impl->bp_limit_) {
    WIRESTEAD_LOG_ERROR("udp", "write", "Queue limit exceeded by single write");
    // Keep error callbacks ordered with the same run's cleanup.
    net::post(impl->strand_, [self = shared_from_this(), generation] {
      auto impl = self->get_impl();
      if (generation != impl->generation_ || impl->stop_requested_) return;
      impl->transition_to(LinkState::Error, {}, "write", "Queue limit exceeded by single write");
    });
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::TooLarge);
  }
  if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                           impl->inflight_bytes_, size, impl->bp_limit_)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();
  net::post(impl->strand_, [self = shared_from_this(), generation, buf = std::move(data), size, request]() mutable {
    auto impl = self->get_impl();
    if (generation != impl->generation_) return;
    if (!impl->enqueue_buffer(self, std::move(buf), size, request)) return;
    impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdpChannel::async_try_write_copy_result(memory::ConstByteSpan data) {
  const auto result = try_write_copy(data);
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::try_write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_run) {
  const auto validation = wrapper::detail::validate_payload_size(data.size());
  if (!validation.accepted()) {
    get_impl()->stats_.record_failed_send();
    return validation;
  }
  return try_write_move(std::vector<uint8_t>(data.begin(), data.end()), expected_run);
}

wrapper::SendResult UdpChannel::async_try_write_move_result(std::vector<uint8_t>&& data) {
  const auto result = try_write_move(std::move(data));
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::try_write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data.size();
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(true, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
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
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();

  net::post(impl->strand_, [self = shared_from_this(), generation, buf = std::move(data), size, request]() mutable {
    auto impl = self->get_impl();
    if (generation != impl->generation_) return;
    if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back({std::move(buf), std::nullopt, request});
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

wrapper::SendResult UdpChannel::async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) {
  const auto result = try_write_shared(std::move(data));
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result;
}

wrapper::SendResult UdpChannel::try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                                 std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data ? data->size() : 0;
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(true, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
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
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();

  net::post(impl->strand_, [self = shared_from_this(), generation, buf = std::move(data), size, request]() mutable {
    auto impl = self->get_impl();
    if (generation != impl->generation_) return;
    if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
        impl->state_.is_state(LinkState::Error) || !impl->remote_endpoint_) {
      queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
      impl->stats_.record_failed_send();
      return;
    }

    impl->tx_.push_back({std::move(buf), std::nullopt, request});
    impl->observe_queue();
    impl->report_backpressure(self, impl->queue_bytes_);
    impl->do_write(self);
  });
  admission.commit();
  return wrapper::SendResult::accept();
}

void UdpChannel::on_bytes(OnBytes cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_ = std::move(shared);
}

void UdpChannel::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_state_ = std::move(shared);
}

void UdpChannel::on_backpressure(OnBackpressure cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bp_ = std::move(shared);
}

void UdpChannel::set_backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  impl_->bp_strategy_.store(strategy, std::memory_order_relaxed);
}

bool UdpChannel::async_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination) {
  const auto result = write_to(data, destination);
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult UdpChannel::write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination,
                                         std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data.size();
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(false, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
  }
  if (impl->cfg_.enable_memory_pool && size <= 65536) {
    memory::PooledBuffer pooled(size, impl->pool_);
    if (pooled.valid()) {
      base::safe_memory::safe_memcpy(pooled.data(), data.data(), size);
      if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                               impl->inflight_bytes_, size, impl->bp_limit_)) {
        impl->stats_.record_failed_send();
        return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
      }
      impl->stats_.record_accepted(size);
      Impl::Ledger::Admission admission(impl->send_accounting_, size);
      const auto request = admission.request();
      net::post(impl->strand_,
                [self = shared_from_this(), generation, buf = std::move(pooled), size, destination, request]() mutable {
                  auto impl = self->get_impl();
                  if (generation != impl->generation_) return;
                  if (!impl->enqueue_buffer(self, std::move(buf), size, request, destination)) return;
                  impl->do_write(self);
                });
      admission.commit();
      return wrapper::SendResult::accept();
    }
  }

  if (!queue_util::try_reserve_limit_bytes(impl->write_reserve_mtx_, impl->queue_bytes_, impl->pending_bytes_,
                                           impl->inflight_bytes_, size, impl->bp_limit_)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();
  net::post(impl->strand_,
            [self = shared_from_this(), generation, buf = std::move(copy), size, destination, request]() mutable {
              auto impl = self->get_impl();
              if (generation != impl->generation_) return;
              if (!impl->enqueue_buffer(self, std::move(buf), size, request, destination)) return;
              impl->do_write(self);
            });
  admission.commit();
  return wrapper::SendResult::accept();
}

bool UdpChannel::async_try_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination) {
  const auto result = try_write_to(data, destination);
  if (auto hook = detail::g_udp_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult UdpChannel::try_write_to(memory::ConstByteSpan data,
                                             const boost::asio::ip::udp::endpoint& destination,
                                             std::optional<uint64_t> expected_run) {
  if (expected_run) {
    if (auto hook = detail::g_udp_pinned_write_hook.load()) hook();
  }
  auto impl = get_impl();
  std::lock_guard<std::mutex> submission_lock(impl->submission_mtx_);
  const auto generation = impl->generation_.load();
  const size_t size = data.size();
  const auto validation = wrapper::detail::validate_payload_size(size);
  if (!validation.accepted()) {
    impl->stats_.record_failed_send();
    return validation;
  }
  if (auto reason = impl->admission_rejection(false, expected_run)) {
    impl->stats_.record_failed_send();
    return wrapper::SendResult::reject(*reason);
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
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }
  if (!queue_util::try_reserve_write_bytes(impl->queue_bytes_, impl->pending_bytes_, impl->backpressure_active_, size,
                                           impl->bp_high_, impl->bp_limit_)) {
    reject_for_pressure();
    return wrapper::SendResult::reject(wrapper::SendRejection::WouldBlock);
  }

  std::vector<uint8_t> copy(data.begin(), data.end());
  impl->stats_.record_accepted(size);
  Impl::Ledger::Admission admission(impl->send_accounting_, size);
  const auto request = admission.request();
  net::post(impl->strand_,
            [self = shared_from_this(), generation, buf = std::move(copy), size, destination, request]() mutable {
              auto impl = self->get_impl();
              if (generation != impl->generation_) return;
              if (impl->stop_requested_.load() || impl->stopping_.load() || impl->state_.is_state(LinkState::Closed) ||
                  impl->state_.is_state(LinkState::Error)) {
                queue_util::release_reserved_write_bytes(impl->queue_bytes_, size);
                impl->stats_.record_failed_send();
                return;
              }

              impl->tx_.push_back({std::move(buf), destination, request});
              impl->observe_queue();
              impl->report_backpressure(self, impl->queue_bytes_);
              impl->do_write(self);
            });
  admission.commit();
  return wrapper::SendResult::accept();
}

// Takes callback_mtx_ like every other setter here. It previously assigned
// without the lock while the strand-confined read site took it, which raced
// against a concurrent on_bytes_from() replacement.
void UdpChannel::on_bytes_from(OnBytesFrom cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->callback_mtx_);
  impl_->on_bytes_from_ = std::move(shared);
}

boost::asio::ip::udp::endpoint UdpChannel::local_endpoint() const { return get_impl()->local_endpoint_; }

boost::asio::any_io_executor UdpChannel::get_executor() { return get_impl()->ioc_->get_executor(); }

}  // namespace transport
}  // namespace wirestead
