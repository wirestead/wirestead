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

#include <boost/asio/ip/udp.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/interface/result_channel.hpp"
#include "wirestead/wrapper/send_result.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace wirestead {
namespace wrapper {
class UdpClient;
class UdpServer;
}  // namespace wrapper
namespace transport {
namespace detail {
struct UdpWriteWait;
}

/**
 * @brief UDP Transport implementation with 1:N support
 */
class WIRESTEAD_API UdpChannel : public interface::ResultChannel, public std::enable_shared_from_this<UdpChannel> {
 public:
  using OnBytesFrom = std::function<void(memory::ConstByteSpan, const boost::asio::ip::udp::endpoint&)>;

  static std::shared_ptr<UdpChannel> create(const config::UdpConfig& cfg);
  static std::shared_ptr<UdpChannel> create(const config::UdpConfig& cfg, boost::asio::io_context& ioc);
  ~UdpChannel() override;

  // Move semantics
  UdpChannel(UdpChannel&&) noexcept;
  UdpChannel& operator=(UdpChannel&&) noexcept;

  // Non-copyable
  UdpChannel(const UdpChannel&) = delete;
  UdpChannel& operator=(const UdpChannel&) = delete;

  // Channel implementation
  void start() override;
  void stop() override;
  bool is_connected() const override;
  bool is_backpressure_active() const override;
  std::optional<size_t> write_queue_limit() const override;
  wrapper::RuntimeStats stats() const override;
  void reset_stats() override;
  std::optional<diagnostics::ErrorInfo> last_error_info() const override;

  // 1:1 writes (using configured remote_endpoint_)
  [[nodiscard]] wrapper::SendResult async_write_copy_result(memory::ConstByteSpan data) override;
  [[nodiscard]] wrapper::SendResult async_write_move_result(std::vector<uint8_t>&& data) override;
  [[nodiscard]] wrapper::SendResult async_write_shared_result(
      std::shared_ptr<const std::vector<uint8_t>> data) override;
  [[nodiscard]] wrapper::SendResult async_try_write_copy_result(memory::ConstByteSpan data) override;
  [[nodiscard]] wrapper::SendResult async_try_write_move_result(std::vector<uint8_t>&& data) override;
  [[nodiscard]] wrapper::SendResult async_try_write_shared_result(
      std::shared_ptr<const std::vector<uint8_t>> data) override;

  // 1:N writes (explicit destination)
  virtual bool async_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination);
  virtual bool async_try_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination);

  // Callbacks
  void on_bytes(OnBytes cb) override;
  virtual void on_bytes_from(OnBytesFrom cb);
  void on_state(OnState cb) override;
  void on_backpressure(OnBackpressure cb) override;

  void set_backpressure_strategy(base::constants::BackpressureStrategy strategy);

  /**
   * @brief Get the local endpoint the socket is bound to.
   */
  boost::asio::ip::udp::endpoint local_endpoint() const;

  /**
   * @brief Get the ASIO executor for this channel.
   */
  boost::asio::any_io_executor get_executor() override;

 private:
  friend class wrapper::UdpClient;
  friend class wrapper::UdpServer;
  std::shared_ptr<detail::UdpWriteWait> capture_write_wait(bool require_remote = true);
  std::optional<wrapper::SendResult> poll_write_wait(const std::shared_ptr<detail::UdpWriteWait>& wait);
  void end_write_wait(const std::shared_ptr<detail::UdpWriteWait>& wait, wrapper::SendRejection reason);
  void cancel_write_waits();
  std::optional<uint64_t> write_connection() const;
  wrapper::SendResult write_state(bool require_remote = true);
  wrapper::SendResult write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult try_write_copy(memory::ConstByteSpan data, std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult try_write_move(std::vector<uint8_t>&& data, std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                   std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data,
                                       std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination,
                               std::optional<uint64_t> expected_run = std::nullopt);
  wrapper::SendResult try_write_to(memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& destination,
                                   std::optional<uint64_t> expected_run = std::nullopt);
  explicit UdpChannel(const config::UdpConfig& cfg);
  UdpChannel(const config::UdpConfig& cfg, boost::asio::io_context& ioc);

  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};

}  // namespace transport
}  // namespace wirestead
