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

#include "unilink/transport/uds/uds_server_session.hpp"

#include "unilink/transport/base/bp_utils.hpp"
#include "unilink/transport/uds/boost_uds_socket.hpp"

namespace unilink {
namespace transport {

UdsServerSession::UdsServerSession(net::io_context& ioc, uds::socket sock, size_t backpressure_threshold,
                                   int idle_timeout_ms, base::constants::BackpressureStrategy strategy)
    : ioc_(ioc),
      strand_(net::make_strand(ioc_)),
      idle_timer_(ioc),
      socket_(std::make_unique<BoostUdsSocket>(std::move(sock))),
      bp_strategy_(strategy),
      bp_high_(backpressure_threshold),
      bp_low_(backpressure_threshold > 1 ? backpressure_threshold / 2 : backpressure_threshold),
      bp_limit_(std::min(std::max(backpressure_threshold * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                         base::constants::MAX_BUFFER_SIZE)),
      idle_timeout_ms_(idle_timeout_ms) {}

UdsServerSession::UdsServerSession(net::io_context& ioc, std::unique_ptr<interface::UdsSocketInterface> socket,
                                   size_t backpressure_threshold, int idle_timeout_ms,
                                   base::constants::BackpressureStrategy strategy)
    : ioc_(ioc),
      strand_(net::make_strand(ioc_)),
      idle_timer_(ioc),
      socket_(std::move(socket)),
      bp_strategy_(strategy),
      bp_high_(backpressure_threshold),
      bp_low_(backpressure_threshold > 1 ? backpressure_threshold / 2 : backpressure_threshold),
      bp_limit_(std::min(std::max(backpressure_threshold * 4, base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
                         base::constants::MAX_BUFFER_SIZE)),
      idle_timeout_ms_(idle_timeout_ms) {}

void UdsServerSession::start() {
  alive_ = true;
  net::dispatch(strand_, [self = shared_from_this()]() {
    self->reset_idle_timer();
    self->start_read();
  });
}

void UdsServerSession::stop() {
  if (closing_.exchange(true)) return;
  net::post(strand_, [this, self = shared_from_this()]() {
    on_bytes_ = nullptr;
    on_bp_ = nullptr;
    do_close();
  });
}

bool UdsServerSession::alive() const { return alive_.load(); }

wrapper::RuntimeStats UdsServerSession::stats() const {
  return stats_.snapshot(queue_bytes_.load(std::memory_order_relaxed), pending_bytes_.load(std::memory_order_relaxed),
                         backpressure_active_.load(std::memory_order_relaxed));
}

void UdsServerSession::reset_stats() {
  stats_.reset(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

bool UdsServerSession::async_write_copy(memory::ConstByteSpan data) {
  std::vector<uint8_t> vec(data.begin(), data.end());
  return async_write_move(std::move(vec));
}

bool UdsServerSession::async_write_move(std::vector<uint8_t>&& data) {
  if (!alive_ || closing_) {
    stats_.record_failed_send();
    return false;
  }
  if (data.empty()) {
    stats_.record_failed_send();
    return false;
  }
  if (queue_bytes_ + pending_bytes_ + data.size() > bp_limit_) {
    stats_.record_failed_send();
    return false;
  }
  stats_.record_accepted(data.size());
  net::post(strand_, [this, self = shared_from_this(), data = std::move(data)]() mutable {
    if (!alive_) return;
    size_t added = data.size();

    // Reliable: route to pending_ when backpressure is active
    if (bp_strategy_ == base::constants::BackpressureStrategy::Reliable && backpressure_active_.load()) {
      if (queue_bytes_ + pending_bytes_ + added > bp_limit_) {
        UNILINK_LOG_ERROR("uds_server_session", "write", "Queue limit exceeded, dropping message");
        return;
      }
      pending_bytes_ += added;
      pending_.emplace_back(std::move(data));
      observe_queue();
      return;
    }

    maybe_flush_for_keep_latest(added);
    if (queue_bytes_ + added > bp_limit_) {
      UNILINK_LOG_ERROR("uds_server_session", "write", "Queue limit exceeded, dropping message");
      report_backpressure(queue_bytes_ + added);
      return;
    }

    queue_bytes_ += added;
    tx_.emplace_back(std::move(data));
    observe_queue();
    report_backpressure(queue_bytes_);
    if (!writing_) do_write();
  });
  return true;
}

bool UdsServerSession::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (!alive_ || closing_ || !data || data->empty()) {
    stats_.record_failed_send();
    return false;
  }
  if (queue_bytes_ + pending_bytes_ + data->size() > bp_limit_) {
    stats_.record_failed_send();
    return false;
  }
  stats_.record_accepted(data->size());
  net::post(strand_, [this, self = shared_from_this(), data = std::move(data)]() mutable {
    if (!alive_) return;
    size_t added = data->size();

    // Reliable: route to pending_ when backpressure is active
    if (bp_strategy_ == base::constants::BackpressureStrategy::Reliable && backpressure_active_.load()) {
      if (queue_bytes_ + pending_bytes_ + added > bp_limit_) {
        UNILINK_LOG_ERROR("uds_server_session", "write", "Queue limit exceeded, dropping message");
        return;
      }
      pending_bytes_ += added;
      pending_.emplace_back(std::move(data));
      observe_queue();
      return;
    }

    maybe_flush_for_keep_latest(added);
    if (queue_bytes_ + added > bp_limit_) {
      UNILINK_LOG_ERROR("uds_server_session", "write", "Queue limit exceeded, dropping message");
      report_backpressure(queue_bytes_ + added);
      return;
    }

    queue_bytes_ += added;
    tx_.emplace_back(std::move(data));
    observe_queue();
    report_backpressure(queue_bytes_);
    if (!writing_) do_write();
  });
  return true;
}

bool UdsServerSession::async_try_write_copy(memory::ConstByteSpan data) {
  if (data.empty() || data.size() > base::constants::MAX_BUFFER_SIZE) {
    stats_.record_failed_send();
    return false;
  }
  return async_try_write_move(std::vector<uint8_t>(data.begin(), data.end()));
}

bool UdsServerSession::async_try_write_move(std::vector<uint8_t>&& data) {
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

  net::post(strand_, [this, self = shared_from_this(), data = std::move(data), added]() mutable {
    if (!alive_ || closing_) {
      queue_util::release_reserved_write_bytes(queue_bytes_, added);
      stats_.record_failed_send();
      return;
    }

    tx_.emplace_back(std::move(data));
    observe_queue();
    report_backpressure(queue_bytes_);
    if (!writing_) do_write();
  });
  return true;
}

bool UdsServerSession::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
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

  net::post(strand_, [this, self = shared_from_this(), data = std::move(data), added]() mutable {
    if (!alive_ || closing_) {
      queue_util::release_reserved_write_bytes(queue_bytes_, added);
      stats_.record_failed_send();
      return;
    }

    tx_.emplace_back(std::move(data));
    observe_queue();
    report_backpressure(queue_bytes_);
    if (!writing_) do_write();
  });
  return true;
}

// Dispatched onto the strand rather than assigned directly: these setters
// may be called from any user thread (e.g. UdsServer::on_backpressure()
// forwarding to an already-accepted session), while the strand-confined
// read sites below access the same fields with no other synchronization.
// Matches the pattern already used correctly by TcpServerSession (#436).
void UdsServerSession::on_bytes(OnBytes cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load()) return;
    self->on_bytes_ = std::move(cb);
  });
}
void UdsServerSession::on_backpressure(OnBackpressure cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load()) return;
    self->on_bp_ = std::move(cb);
  });
}
void UdsServerSession::on_close(OnClose cb) {
  auto self = shared_from_this();
  net::dispatch(strand_, [self, cb = std::move(cb)]() mutable {
    if (self->closing_.load()) return;
    self->on_close_ = std::move(cb);
  });
}

void UdsServerSession::start_read() {
  socket_->async_read_some(
      net::buffer(rx_),
      net::bind_executor(strand_, [this, self = shared_from_this()](const boost::system::error_code& ec, size_t bytes) {
        if (closing_ || !alive_) return;
        if (ec) {
          do_close();
          return;
        }
        if (bytes > 0) stats_.record_received(bytes);
        if (on_bytes_) on_bytes_(memory::ConstByteSpan(rx_.data(), bytes));
        reset_idle_timer();
        start_read();
      }));
}

void UdsServerSession::do_write() {
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
  socket_->async_write(buffer, net::bind_executor(strand_, [this, self = shared_from_this(), bytes_to_write](
                                                               const boost::system::error_code& ec, size_t written) {
                         if (closing_ || !alive_) return;
                         writing_ = false;
                         current_write_buffer_ = std::nullopt;
                         queue_bytes_ = (queue_bytes_ >= bytes_to_write) ? (queue_bytes_ - bytes_to_write) : 0;
                         report_backpressure(queue_bytes_);

                         if (ec) {
                           do_close();
                           return;
                         }
                         stats_.record_sent(written);
                         if (!tx_.empty()) do_write();
                       }));
}

void UdsServerSession::do_close() {
  if (!closing_.exchange(true) && !alive_) return;
  alive_ = false;
  auto close_cb = std::move(on_close_);

  boost::system::error_code ec;
  socket_->close(ec);

  // Drain queued/pending writes and unconditionally clear backpressure,
  // notifying any waiter directly - mirrors UdpChannel's
  // drain_queue_and_clear_backpressure(). Must run before on_bp_ is cleared
  // below: otherwise a Reliable-mode caller blocked in send_to_blocking()
  // for this client would never be woken up when the client disconnects via
  // a read error or idle timeout (jwsung91/unilink#452).
  tx_.clear();
  current_write_buffer_ = std::nullopt;
  queue_bytes_ = 0;
  pending_.clear();
  pending_bytes_ = 0;
  writing_ = false;
  const bool had_backpressure = backpressure_active_;
  backpressure_active_ = false;
  if (had_backpressure && on_bp_) {
    try {
      on_bp_(queue_bytes_);
    } catch (...) {
    }
  }

  on_bytes_ = nullptr;
  on_bp_ = nullptr;
  on_close_ = nullptr;
  if (close_cb) {
    try {
      close_cb();
    } catch (...) {
    }
  }
}

void UdsServerSession::maybe_flush_for_keep_latest(size_t added) {
  const auto dropped =
      queue_util::maybe_flush_for_keep_latest(bp_strategy_, added, bp_high_, tx_, queue_bytes_, backpressure_active_);
  if (dropped.any()) stats_.record_dropped(dropped.messages, dropped.bytes);
}

void UdsServerSession::observe_queue() {
  stats_.observe_queue(queue_bytes_.load(std::memory_order_relaxed) + pending_bytes_.load(std::memory_order_relaxed));
}

void UdsServerSession::report_backpressure(size_t queued_bytes) {
  if (closing_ || !alive_) return;
  observe_queue();

  if (!backpressure_active_ && queued_bytes >= bp_high_) {
    backpressure_active_ = true;
    stats_.record_backpressure_event();
    if (on_bp_) {
      try {
        on_bp_(queued_bytes);
      } catch (...) {
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
    if (on_bp_) {
      try {
        on_bp_(queued_bytes);
      } catch (...) {
      }  // fire OFF with pre-flush queue size
    }
    // If post-flush queue is still high, fire ON again
    if (queue_bytes_ >= bp_high_) {
      backpressure_active_ = true;
      stats_.record_backpressure_event();
      if (on_bp_) {
        try {
          on_bp_(queue_bytes_);
        } catch (...) {
        }
      }
    }
    if (!writing_) do_write();
  }
}

void UdsServerSession::reset_idle_timer() {
  if (idle_timeout_ms_ <= 0) return;

  idle_timer_.cancel();
  idle_timer_.expires_after(std::chrono::milliseconds(idle_timeout_ms_));

  auto self = shared_from_this();
  idle_timer_.async_wait(net::bind_executor(strand_, [self](const boost::system::error_code& ec) {
    if (ec == boost::asio::error::operation_aborted) return;
    if (!self->alive_ || self->closing_) return;
    UNILINK_LOG_WARNING("uds_server_session", "timeout", "Connection idle timeout expired, closing session");
    self->do_close();
  }));
}

}  // namespace transport
}  // namespace unilink
