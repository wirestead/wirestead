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

#include "wirestead/base/constants.hpp"
#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/config/uds_config.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/interface/iuds_socket.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"

// Forward declare boost components
namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace wirestead {
namespace transport {

using base::LinkState;
using config::UdsClientConfig;
using interface::Channel;

/**
 * @brief Thread-safe UDS Client implementation
 */
class WIRESTEAD_API UdsClient : public Channel, public std::enable_shared_from_this<UdsClient> {
 public:
  using BufferVariant =
      std::variant<memory::PooledBuffer, std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;

  static std::shared_ptr<UdsClient> create(const UdsClientConfig& cfg);
  static std::shared_ptr<UdsClient> create(const UdsClientConfig& cfg, boost::asio::io_context& ioc);
  static std::shared_ptr<UdsClient> create(const UdsClientConfig& cfg,
                                           std::unique_ptr<interface::UdsSocketInterface> socket,
                                           boost::asio::io_context& ioc);
  ~UdsClient();

  // Move semantics
  UdsClient(UdsClient&&) noexcept;
  UdsClient& operator=(UdsClient&&) noexcept;

  // Disable copy
  UdsClient(const UdsClient&) = delete;
  UdsClient& operator=(const UdsClient&) = delete;

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

  void on_bytes(OnBytes cb) override;
  void on_state(OnState cb) override;
  void on_backpressure(OnBackpressure cb) override;

  std::optional<diagnostics::ErrorInfo> last_error_info() const override;

  void set_backpressure_strategy(base::constants::BackpressureStrategy strategy);
  void set_retry_interval(unsigned interval_ms);
  void set_reconnect_policy(ReconnectPolicy policy);

 private:
  explicit UdsClient(const UdsClientConfig& cfg);
  explicit UdsClient(const UdsClientConfig& cfg, boost::asio::io_context& ioc);
  UdsClient(const UdsClientConfig& cfg, std::unique_ptr<interface::UdsSocketInterface> socket,
            boost::asio::io_context& ioc);

  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};

}  // namespace transport
}  // namespace wirestead
