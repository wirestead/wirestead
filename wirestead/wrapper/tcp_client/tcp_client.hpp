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

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "wirestead/base/constants.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/wrapper/ichannel.hpp"
#include "wirestead/wrapper/receive_limits.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace wirestead {

namespace interface {
class Channel;
}

namespace wrapper {

/**
 * @brief Modernized TCP Client Wrapper
 */
class WIRESTEAD_API TcpClient : public ChannelInterface {
 public:
  TcpClient(const std::string& host, uint16_t port);
  TcpClient(const std::string& host, uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc);
  /// Requires the matching native transport or ConnectionChannel; otherwise throws std::invalid_argument.
  explicit TcpClient(std::shared_ptr<interface::Channel> channel);
  ~TcpClient() override;

  // Move semantics
  TcpClient(TcpClient&&) noexcept;
  TcpClient& operator=(TcpClient&&) noexcept;

  // Disable copy
  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

  // ChannelInterface implementation
  [[nodiscard]] std::future<bool> start() override;
  void stop() override;
  [[nodiscard]] SendResult send(std::string_view data) override;
  [[nodiscard]] SendResult send_line(std::string_view line) override;
  [[nodiscard]] SendResult send_blocking(std::string_view data) override;
  [[nodiscard]] SendResult send_line_blocking(std::string_view line) override;
  [[nodiscard]] SendResult try_send(std::string_view data) override;
  [[nodiscard]] SendResult try_send_line(std::string_view line) override;
  [[nodiscard]] SendResult send_move(std::vector<uint8_t>&& data) override;
  [[nodiscard]] SendResult try_send_move(std::vector<uint8_t>&& data) override;
  [[nodiscard]] SendResult send_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  [[nodiscard]] SendResult try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  bool connected() const override;
  RuntimeStats stats() const override;
  /// Configure library receive storage while stopped. Custom framer internals are excluded.
  TcpClient& receive_limits(ReceiveLimits limits);
  ReceiveMemoryStats receive_stats() const;
  void reset_stats() override;

  TcpClient& on_data(MessageHandler handler) override;
  TcpClient& on_data_batch(BatchMessageHandler handler) override;
  TcpClient& on_connect(ConnectionHandler handler) override;
  TcpClient& on_disconnect(ConnectionHandler handler) override;
  TcpClient& on_error(ErrorHandler handler) override;
  TcpClient& on_backpressure(std::function<void(size_t)> handler) override;

  TcpClient& framer(std::unique_ptr<framer::IFramer> framer) override;
  TcpClient& on_message(MessageHandler handler) override;
  TcpClient& on_message_batch(BatchMessageHandler handler) override;

  TcpClient& auto_start(bool manage = true) override;

  // Configuration
  TcpClient& batch_size(size_t size);
  TcpClient& batch_latency(std::chrono::milliseconds latency);
  TcpClient& retry_interval(std::chrono::milliseconds interval);
  TcpClient& max_retries(int max_retries);
  /**
   * @brief Connect over TLS, verifying the server's certificate.
   *
   * Verification is not optional: an unverified TLS connection encrypts traffic
   * to whoever answered. With no argument the system trust store is used; pass
   * a PEM to trust a private CA or a self-signed certificate instead. The host
   * given to the constructor is the name the certificate must match.
   *
   * Must be set before start(). Requires a build with WIRESTEAD_ENABLE_TLS.
   */
  TcpClient& tls(const std::string& ca_file = "");

  TcpClient& connection_timeout(std::chrono::milliseconds timeout);
  /**
   * @brief Configure application-level idle timeout.
   *
   * A value of 0ms disables idle timeout. When enabled, inbound or outbound
   * activity resets the timer.
   */
  TcpClient& idle_timeout(std::chrono::milliseconds timeout);
  /**
   * @brief Configure what happens when an enabled idle timeout expires.
   *
   * The default is IdleTimeoutAction::Reconnect. This setting has no effect
   * while idle_timeout is 0ms.
   */
  TcpClient& idle_timeout_action(IdleTimeoutAction action);
  TcpClient& backpressure_threshold(size_t threshold);
  TcpClient& backpressure_strategy(base::constants::BackpressureStrategy strategy);

  /// Returns the configured backpressure threshold in bytes.
  size_t backpressure_threshold() const;
  /// Returns the configured backpressure strategy.
  base::constants::BackpressureStrategy backpressure_strategy() const;
  // Socket-option setters below have no live/runtime effect on an already-open
  // socket: they stage a value that is only applied the next time start()
  // builds a fresh connection (e.g. after stop() + start() again). Calling
  // one of these while already connected is a deferred no-op until restart,
  // not an immediate change (#436).
  TcpClient& tcp_no_delay(bool enable = true);
  TcpClient& keep_alive(bool enable = true);
  TcpClient& send_buffer_size(size_t bytes);
  TcpClient& receive_buffer_size(size_t bytes);

  /**
   * @brief Size of the userspace buffer each read fills, in bytes.
   *
   * Distinct from receive_buffer_size(), which sets the kernel's SO_RCVBUF.
   * Raising this reduces read completions and callback dispatches on bulk
   * transfers, at the cost of that much memory per connection. Clamped to
   * [MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE]; takes effect on the next
   * start().
   */
  TcpClient& read_buffer_size(size_t bytes);
  TcpClient& manage_external_context(bool manage);

 private:
  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  // #450: shared_ptr (not unique_ptr) so in-flight callbacks on an
  // externally-owned io_context can extend Impl's lifetime for the
  // duration of their invocation via weak_from_this(), rather than only
  // checking a staleness flag that says nothing about whether Impl itself
  // still exists.
  std::shared_ptr<Impl> impl_;
};

}  // namespace wrapper
}  // namespace wirestead
