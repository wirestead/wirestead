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

#include "unilink/transport/tcp_server/tcp_server_session.hpp"

#include <cstring>
#include <iostream>

#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/tcp_server/boost_tcp_socket.hpp"

namespace unilink {
namespace transport {

TcpServerSession::TcpServerSession(net::io_context& ioc, tcp::socket sock, size_t backpressure_threshold,
                                   int idle_timeout_ms, base::constants::BackpressureStrategy strategy,
                                   bool enable_memory_pool)
    : ioc_(ioc),
      strand_(ioc.get_executor()),
      idle_timer_(ioc),
      socket_(std::make_unique<BoostTcpSocket>(std::move(sock))),
      enable_memory_pool_(enable_memory_pool),
      writing_(false),
      queue_bytes_(0),
      bp_strategy_(strategy),
      bp_high_(backpressure_threshold),
      idle_timeout_ms_(idle_timeout_ms),
      alive_(false),
      cleanup_done_(false) {
  bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                       base::constants::MAX_BUFFER_SIZE);
  bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
  if (bp_low_ == 0) bp_low_ = 1;
}

TcpServerSession::TcpServerSession(net::io_context& ioc, std::unique_ptr<interface::TcpSocketInterface> socket,
                                   size_t backpressure_threshold, int idle_timeout_ms,
                                   base::constants::BackpressureStrategy strategy, bool enable_memory_pool)
    : ioc_(ioc),
      strand_(ioc.get_executor()),
      idle_timer_(ioc),
      socket_(std::move(socket)),
      enable_memory_pool_(enable_memory_pool),
      writing_(false),
      queue_bytes_(0),
      bp_strategy_(strategy),
      bp_high_(backpressure_threshold),
      idle_timeout_ms_(idle_timeout_ms),
      alive_(false),
      cleanup_done_(false) {
  bp_limit_ = std::min(std::max(bp_high_ * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                       base::constants::MAX_BUFFER_SIZE);
  bp_low_ = bp_high_ > 1 ? bp_high_ / 2 : bp_high_;
  if (bp_low_ == 0) bp_low_ = 1;
}

void TcpServerSession::start() {
  if (alive_.exchange(true)) return;
  auto self = shared_from_this();
  net::dispatch(strand_, [self] {
    self->reset_idle_timer();
    self->start_read();
  });
}

bool TcpServerSession::async_write_copy(memory::ConstByteSpan data) {
  if (!alive_ || closing_) {
    stats_.record_failed_send();
    return false;
  }  // Don't queue writes if session is not alive

  size_t size = data.size();
  if (size == 0) {
    stats_.record_failed_send();
    return false;
  }
  if (size > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_server_session", "write", "Write size exceeds maximum allowed");
    stats_.record_failed_send();
    return false;
  }

  // Use memory pool for better performance (only for reasonable sizes)
  if (size <= base::constants::LARGE_BUFFER_THRESHOLD && enable_memory_pool_) {  // Only use pool for buffers <= 64KB
    memory::PooledBuffer pooled_buffer(size, pool_);
    if (pooled_buffer.valid()) {
      // Copy data to pooled buffer safely
      base::safe_memory::safe_memcpy(pooled_buffer.data(), data.data(), size);
      if (!queue_util::try_reserve_limit_bytes(queue_bytes_, pending_bytes_, inflight_bytes_, size, bp_limit_)) {
        stats_.record_failed_send();
        return false;
      }
      stats_.record_accepted(size);
      net::post(strand_, [self = shared_from_this(), buf = std::move(pooled_buffer)]() mutable {
        const auto added = buf.size();
        if (!self->alive_ || self->closing_) {  // Double-check in case session was closed
          queue_util::release_reserved_limit_bytes(self->inflight_bytes_, added);
          self->stats_.record_failed_send();
          return;
        }
        self->route_enqueued_buffer(BufferVariant{std::move(buf)}, added);
      });
      return true;
    }
  }

  // Fallback to regular allocation for large buffers or pool exhaustion
  if (!queue_util::try_reserve_limit_bytes(queue_bytes_, pending_bytes_, inflight_bytes_, size, bp_limit_)) {
    stats_.record_failed_send();
    return false;
  }
  std::vector<uint8_t> fallback(data.begin(), data.end());
  stats_.record_accepted(size);

  net::post(strand_, [self = shared_from_this(), buf = std::move(fallback), size]() mutable {
    if (!self->alive_ || self->closing_) {  // Double-check in case session was closed
      queue_util::release_reserved_limit_bytes(self->inflight_bytes_, size);
      self->stats_.record_failed_send();
      return;
    }
    self->route_enqueued_buffer(BufferVariant{std::move(buf)}, size);
  });
  return true;
}

bool TcpServerSession::async_write_move(std::vector<uint8_t>&& data) {
  if (!alive_ || closing_) {
    stats_.record_failed_send();
    return false;
  }
  const auto added = data.size();
  if (added == 0) {
    stats_.record_failed_send();
    return false;
  }
  if (added > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_server_session", "write", "Write size exceeds maximum allowed");
    stats_.record_failed_send();
    return false;
  }
  if (!queue_util::try_reserve_limit_bytes(queue_bytes_, pending_bytes_, inflight_bytes_, added, bp_limit_)) {
    stats_.record_failed_send();
    return false;
  }
  stats_.record_accepted(added);
  net::post(strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (!self->alive_ || self->closing_) {
      queue_util::release_reserved_limit_bytes(self->inflight_bytes_, added);
      self->stats_.record_failed_send();
      return;
    }
    self->route_enqueued_buffer(BufferVariant{std::move(buf)}, added);
  });
  return true;
}

bool TcpServerSession::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!alive_ || closing_ || !data || data->empty()) {
    stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  if (added > base::constants::MAX_BUFFER_SIZE) {
    UNILINK_LOG_ERROR("tcp_server_session", "write", "Write size exceeds maximum allowed");
    stats_.record_failed_send();
    return false;
  }
  if (!queue_util::try_reserve_limit_bytes(queue_bytes_, pending_bytes_, inflight_bytes_, added, bp_limit_)) {
    stats_.record_failed_send();
    return false;
  }
  stats_.record_accepted(added);
  net::post(strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (!self->alive_ || self->closing_) {
      queue_util::release_reserved_limit_bytes(self->inflight_bytes_, added);
      self->stats_.record_failed_send();
      return;
    }
    self->route_enqueued_buffer(BufferVariant{std::move(buf)}, added);
  });
  return true;
}

bool TcpServerSession::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty() || data.size() > base::constants::MAX_BUFFER_SIZE) {
    stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool TcpServerSession::async_try_write_move(std::vector<uint8_t>&& data) {
  if (!alive_ || closing_) {
    stats_.record_failed_send();
    return false;
  }
  const auto added = data.size();
  if (added == 0 || added > base::constants::MAX_BUFFER_SIZE) {
    stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [this, added]() {
    if (bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      stats_.record_dropped(1, added);
    } else {
      stats_.record_failed_send();
    }
  };
  if (backpressure_active_.load() || queue_bytes_ + added > bp_high_ ||
      queue_bytes_ + pending_bytes_ + added > bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(queue_bytes_, pending_bytes_, backpressure_active_, added, bp_high_,
                                           bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  stats_.record_accepted(added);

  net::post(strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (!self->alive_ || self->closing_) {
      queue_util::release_reserved_write_bytes(self->queue_bytes_, added);
      self->stats_.record_failed_send();
      return;
    }

    self->tx_.emplace_back(std::move(buf));
    self->observe_queue();
    self->report_backpressure(self->queue_bytes_);
    if (!self->writing_) self->do_write();
  });
  return true;
}

bool TcpServerSession::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!alive_ || closing_ || !data || data->empty()) {
    stats_.record_failed_send();
    return false;
  }
  const auto added = data->size();
  if (added > base::constants::MAX_BUFFER_SIZE) {
    stats_.record_failed_send();
    return false;
  }
  const auto reject_for_pressure = [this, added]() {
    if (bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      stats_.record_dropped(1, added);
    } else {
      stats_.record_failed_send();
    }
  };
  if (backpressure_active_.load() || queue_bytes_ + added > bp_high_ ||
      queue_bytes_ + pending_bytes_ + added > bp_limit_) {
    reject_for_pressure();
    return false;
  }
  if (!queue_util::try_reserve_write_bytes(queue_bytes_, pending_bytes_, backpressure_active_, added, bp_high_,
                                           bp_limit_)) {
    reject_for_pressure();
    return false;
  }
  stats_.record_accepted(added);

  net::post(strand_, [self = shared_from_this(), buf = std::move(data), added]() mutable {
    if (!self->alive_ || self->closing_) {
      queue_util::release_reserved_write_bytes(self->queue_bytes_, added);
      self->stats_.record_failed_send();
      return;
    }

    self->tx_.emplace_back(std::move(buf));
    self->observe_queue();
    self->report_backpressure(self->queue_bytes_);
    if (!self->writing_) self->do_write();
  });
  return true;
}

void TcpServerSession::on_bytes(OnBytes cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load() || self->cleanup_done_.load()) return;
    self->on_bytes_ = std::move(cb);
  });
}
void TcpServerSession::on_backpressure(OnBackpressure cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load() || self->cleanup_done_.load()) return;
    self->on_bp_ = std::move(cb);
  });
}
void TcpServerSession::on_close(OnClose cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load() || self->cleanup_done_.load()) return;
    self->on_close_ = std::move(cb);
  });
}

bool TcpServerSession::alive() const { return alive_.load(); }

wrapper::RuntimeStats TcpServerSession::stats() const {
  return stats_.snapshot(queue_bytes_.load(std::memory_order_relaxed), pending_bytes_.load(std::memory_order_relaxed),
                         backpressure_active_.load(std::memory_order_relaxed));
}

void TcpServerSession::reset_stats() {
  stats_.reset(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void TcpServerSession::stop() {
  if (closing_.exchange(true)) return;
  auto self = shared_from_this();
  net::post(strand_, [self] {
    // Clear callbacks on the strand to block further user callbacks after stop.
    self->on_bytes_ = nullptr;
    self->on_bp_ = nullptr;
    self->on_close_ = nullptr;
    self->idle_timer_.cancel();
    self->do_close();
  });
}

void TcpServerSession::cancel() {
  auto self = shared_from_this();
  net::dispatch(strand_, [self] {
    self->idle_timer_.cancel();
    boost::system::error_code ec;
    // Cancelling the socket via close() causes ongoing operations to complete with operation_aborted.
    // Unlike stop(), this does NOT set closing_ flag immediately, allowing the
    // error handler to run normally and trigger do_close() via the error path.
    if (self->socket_) {
      self->socket_->close(ec);
    }
  });
}

void TcpServerSession::start_read() {
  auto self = shared_from_this();
  socket_->async_read_some(
      net::buffer(rx_.data(), rx_.size()), net::bind_executor(strand_, [self](auto ec, std::size_t n) {
        if (self->closing_ || !self->alive_) return;
        if (ec) {
          self->do_close();
          return;
        }
        self->reset_idle_timer();
        if (n > 0) self->stats_.record_received(n);
        if (self->on_bytes_) {
          try {
            self->on_bytes_(memory::ConstByteSpan(self->rx_.data(), n));
          } catch (const std::exception& e) {
            UNILINK_LOG_ERROR("tcp_server_session", "on_bytes",
                              "Exception in on_bytes callback: " + std::string(e.what()));
            self->do_close();
            return;
          } catch (...) {
            UNILINK_LOG_ERROR("tcp_server_session", "on_bytes", "Unknown exception in on_bytes callback");
            self->do_close();
            return;
          }
        }
        self->start_read();
      }));
}

void TcpServerSession::do_write() {
  if (tx_.empty()) {
    writing_ = false;
    return;
  }
  writing_ = true;
  auto self = shared_from_this();

  // Move buffer out of queue immediately to ensure lifetime safety during async op
  // Optimization: Move into current_write_buffer_ to keep it alive during async op
  // without allocating a shared_ptr control block.
  current_write_buffer_ = std::move(tx_.front());
  tx_.pop_front();

  auto& current = *current_write_buffer_;

  auto on_write = [self](const boost::system::error_code& ec, std::size_t n) {
    // Release the buffer immediately
    self->current_write_buffer_.reset();

    if (self->closing_ || !self->alive_) return;
    if (self->queue_bytes_ >= n) {
      self->queue_bytes_ -= n;
    } else {
      self->queue_bytes_ = 0;
    }
    self->report_backpressure(self->queue_bytes_);

    if (ec) {
      self->do_close();
      return;
    }
    self->stats_.record_sent(n);
    self->reset_idle_timer();
    self->do_write();
  };

  std::visit(
      [&](const auto& buf) {
        using T = std::decay_t<decltype(buf)>;
        if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
          socket_->async_write(net::buffer(buf->data(), buf->size()), net::bind_executor(strand_, on_write));
        } else {
          socket_->async_write(net::buffer(buf.data(), buf.size()), net::bind_executor(strand_, on_write));
        }
      },
      current);
}

void TcpServerSession::do_close() {
  if (cleanup_done_.exchange(true)) return;  // Ensures cleanup runs only once

  alive_.store(false);
  closing_.store(true);  // Redundant, but ensures consistency

  // Safely invoke on_close callback
  auto close_cb = std::move(on_close_);

  UNILINK_LOG_INFO("tcp_server_session", "disconnect", "Client disconnected");
  boost::system::error_code ec;
  socket_->shutdown(tcp::socket::shutdown_both, ec);
  socket_->close(ec);

  // Drain queued/pending writes and unconditionally clear backpressure,
  // notifying any waiter directly - shares UdpChannel's terminal-drain
  // helper (#434). Must run before on_bp_ is cleared below: otherwise a
  // Reliable-mode caller blocked in send_to_blocking() for this client
  // would never be woken up when the client disconnects via a read error
  // or idle timeout (jwsung91/unilink#452).
  {
    auto f = bp_fields();
    queue_util::drain_and_clear_backpressure(f, on_bp_, [&]() {
      tx_.clear();
      queue_bytes_ = 0;
      pending_.clear();
      pending_bytes_ = 0;
    });
  }

  // Clear all callbacks to prevent any further invocations
  on_bytes_ = nullptr;
  on_bp_ = nullptr;
  on_close_ = nullptr;
  idle_timer_.cancel();

  if (close_cb) {
    try {
      close_cb();
    } catch (const std::exception& e) {
      UNILINK_LOG_ERROR("tcp_server_session", "on_close", "Exception in on_close callback: " + std::string(e.what()));
    } catch (...) {
      UNILINK_LOG_ERROR("tcp_server_session", "on_close", "Unknown exception in on_close callback");
    }
  }
}

queue_util::BackpressureFields TcpServerSession::bp_fields() {
  return queue_util::BackpressureFields{queue_bytes_, pending_bytes_, backpressure_active_, bp_high_,
                                        bp_low_,      bp_limit_,      bp_strategy_};
}

void TcpServerSession::route_enqueued_buffer(BufferVariant&& buf, size_t added) {
  auto f = bp_fields();
  queue_util::DropAccounting dropped;
  auto decision = queue_util::decide_enqueue(f, added, tx_, dropped);
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);

  if (decision == queue_util::EnqueueDecision::Rejected) {
    UNILINK_LOG_ERROR("tcp_server_session", "write", "Queue limit exceeded, dropping message");
    // #448: record as dropped so it's reflected in RuntimeStats instead of
    // silently vanishing after being counted as accepted.
    stats_.record_dropped(1, added);
    queue_util::release_reserved_limit_bytes(inflight_bytes_, added);
    report_backpressure(queue_bytes_ + added);
    return;
  }
  if (decision == queue_util::EnqueueDecision::Pending) {
    pending_bytes_ += added;
    queue_util::release_reserved_limit_bytes(inflight_bytes_, added);
    pending_.emplace_back(std::move(buf));
    observe_queue();
    return;
  }
  queue_bytes_ += added;
  queue_util::release_reserved_limit_bytes(inflight_bytes_, added);
  tx_.emplace_back(std::move(buf));
  observe_queue();
  report_backpressure(queue_bytes_);
  if (!writing_) do_write();
}

void TcpServerSession::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void TcpServerSession::report_backpressure(size_t queued_bytes) {
  if (closing_ || !alive_) return;
  observe_queue();
  auto f = bp_fields();
  queue_util::report_backpressure(
      f, queued_bytes, on_bp_, stats_,
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
        if (!writing_) do_write();
      });
}

void TcpServerSession::reset_idle_timer() {
  if (idle_timeout_ms_ <= 0) return;

  // Cancel any existing timer
  idle_timer_.cancel();

  // Reset timer
  idle_timer_.expires_after(std::chrono::milliseconds(idle_timeout_ms_));

  auto self = shared_from_this();
  idle_timer_.async_wait(net::bind_executor(strand_, [self](const boost::system::error_code& ec) {
    if (ec == boost::asio::error::operation_aborted) return;
    if (!self->alive_ || self->closing_) return;

    UNILINK_LOG_WARNING("tcp_server_session", "timeout", "Connection idle timeout expired, closing session");
    self->do_close();
  }));
}

}  // namespace transport
}  // namespace unilink
