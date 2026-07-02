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

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "unilink/base/constants.hpp"
#include "unilink/base/platform.hpp"
#include "unilink/base/visibility.hpp"
#include "unilink/config/tcp_client_config.hpp"
#include "unilink/diagnostics/error_types.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/memory/memory_pool.hpp"
#include "unilink/transport/base/reconnect_policy.hpp"

// Forward declare boost components
namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace unilink {
namespace transport {

using base::LinkState;
using config::TcpClientConfig;
using interface::Channel;

// Use static create() helpers to construct safely
class UNILINK_API TcpClient : public Channel, public std::enable_shared_from_this<TcpClient> {
 public:
  using BufferVariant =
      std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;

  static std::shared_ptr<TcpClient> create(const TcpClientConfig& cfg);
  static std::shared_ptr<TcpClient> create(const TcpClientConfig& cfg, boost::asio::io_context& ioc);
  ~TcpClient() override;

  // Move semantics
  TcpClient(TcpClient&&) noexcept;
  TcpClient& operator=(TcpClient&&) noexcept;

  // Disable copy (should be already disabled by unique_ptr, but being explicit)
  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

  void start() override;
  void stop() override;
  bool is_connected() const override;
  bool is_backpressure_active() const override;
  wrapper::RuntimeStats stats() const override;
  void reset_stats() override;
  boost::asio::any_io_executor get_executor() override;

  bool async_write_copy(memory::ConstByteSpan data) override;
  bool async_write_move(std::vector<uint8_t>&& data) override;
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  bool async_try_write_copy(memory::ConstByteSpan data) override;
  bool async_try_write_move(std::vector<uint8_t>&& data) override;
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;

  // Thread-safe: may be called at any time, including after start(). Each
  // setter takes effect for subsequent operations (callback replacement is
  // synchronized against concurrent reads on the io thread; see #436).
  void on_bytes(OnBytes cb) override;
  void on_state(OnState cb) override;
  void on_backpressure(OnBackpressure cb) override;

  std::optional<diagnostics::ErrorInfo> last_error_info() const;

  // Dynamic configuration methods. Thread-safe; take effect for the next
  // reconnect/idle-timeout decision, not retroactively for one already in
  // flight.
  void set_backpressure_strategy(base::constants::BackpressureStrategy strategy);
  void set_retry_interval(unsigned interval_ms);
  void set_max_retries(int max_retries);
  void set_connection_timeout(unsigned timeout_ms);
  void set_idle_timeout(unsigned timeout_ms);
  void set_idle_timeout_action(IdleTimeoutAction action);
  void set_reconnect_policy(ReconnectPolicy policy);

 private:
  explicit TcpClient(const TcpClientConfig& cfg);
  explicit TcpClient(const TcpClientConfig& cfg, boost::asio::io_context& ioc);

  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};
}  // namespace transport
}  // namespace unilink
