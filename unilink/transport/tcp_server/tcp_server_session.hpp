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

#pragma once

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include "unilink/base/constants.hpp"
#include "unilink/base/platform.hpp"
#include "unilink/base/visibility.hpp"
#include "unilink/diagnostics/error_handler.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/interface/itcp_socket.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/memory/safe_span.hpp"
#include "unilink/transport/base/bp_state_machine.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;

using base::LinkState;
using interface::TcpSocketInterface;
using tcp = net::ip::tcp;

class UNILINK_API TcpServerSession : public std::enable_shared_from_this<TcpServerSession> {
 public:
  using OnBytes = interface::Channel::OnBytes;
  using OnBackpressure = interface::Channel::OnBackpressure;
  using OnClose = std::function<void()>;
  using BufferVariant =
      std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;

  TcpServerSession(net::io_context& ioc, tcp::socket sock,
                   size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD,
                   int idle_timeout_ms = 0,
                   base::constants::BackpressureStrategy strategy = base::constants::BackpressureStrategy::Reliable,
                   bool enable_memory_pool = true);
  // Constructor for testing with dependency injection
  TcpServerSession(net::io_context& ioc, std::unique_ptr<interface::TcpSocketInterface> socket,
                   size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD,
                   int idle_timeout_ms = 0,
                   base::constants::BackpressureStrategy strategy = base::constants::BackpressureStrategy::Reliable,
                   bool enable_memory_pool = true);

  void start();
  bool async_write_copy(memory::ConstByteSpan data);
  bool async_write_move(std::vector<uint8_t>&& data);
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data);
  bool async_try_write_copy(memory::ConstByteSpan data);
  bool async_try_write_move(std::vector<uint8_t>&& data);
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data);
  void on_bytes(OnBytes cb);
  void on_backpressure(OnBackpressure cb);
  void on_close(OnClose cb);
  bool alive() const;
  bool is_backpressure_active() const { return backpressure_active_.load(); }
  wrapper::RuntimeStats stats() const;
  void reset_stats();
  void stop();
  void cancel();

 private:
  void start_read();
  void do_write();
  void do_close();
  void report_backpressure(size_t queued_bytes);
  void reset_idle_timer();
  void observe_queue();
  // Shared decide_enqueue()/route dispatch used by all 3 async_write_* variants (#434).
  void route_enqueued_buffer(BufferVariant&& buf, size_t added);
  queue_util::BackpressureFields bp_fields();

 private:
  net::io_context& ioc_;
  net::strand<net::io_context::executor_type> strand_;
  net::steady_timer idle_timer_;
  std::unique_ptr<interface::TcpSocketInterface> socket_;
  // #443: per-session pool instead of the process-wide GlobalMemoryPool
  // singleton - avoids cross-channel contention on the singleton's bucket
  // mutexes. Capacity is much smaller than the old shared default since
  // it's no longer amortized across every connected client in the process.
  memory::MemoryPool pool_{50, 200};
  bool enable_memory_pool_ = true;
  std::array<uint8_t, base::constants::DEFAULT_READ_BUFFER_SIZE> rx_{};
  std::deque<BufferVariant> tx_;
  std::deque<BufferVariant> pending_;
  std::atomic<size_t> pending_bytes_{0};
  std::optional<BufferVariant> current_write_buffer_;
  bool writing_ = false;
  std::atomic<size_t> queue_bytes_{0};
  base::constants::BackpressureStrategy bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_high_;   // Configurable backpressure threshold
  size_t bp_limit_;  // Hard cap for queued bytes
  size_t bp_low_;    // Backpressure relief threshold
  std::atomic<bool> backpressure_active_{false};
  diagnostics::RuntimeStatsCounters stats_;
  int idle_timeout_ms_ = 0;

  OnBytes on_bytes_;
  OnBackpressure on_bp_;
  OnClose on_close_;
  std::atomic<bool> alive_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> cleanup_done_{false};
};
}  // namespace transport
}  // namespace unilink
