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

#include "unilink/base/constants.hpp"
#include "unilink/base/visibility.hpp"
#include "unilink/wrapper/ichannel.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace unilink {

namespace interface {
class Channel;
}

namespace wrapper {

/**
 * @brief Modernized TCP Client Wrapper
 */
class UNILINK_API TcpClient : public ChannelInterface {
 public:
  TcpClient(const std::string& host, uint16_t port);
  TcpClient(const std::string& host, uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc);
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
  bool send(std::string_view data) override;
  bool send_line(std::string_view line) override;
  bool send_blocking(std::string_view data) override;
  bool send_line_blocking(std::string_view line) override;
  bool try_send(std::string_view data) override;
  bool try_send_line(std::string_view line) override;
  bool send_move(std::vector<uint8_t>&& data) override;
  bool try_send_move(std::vector<uint8_t>&& data) override;
  bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  bool try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  bool connected() const override;
  RuntimeStats stats() const override;
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
  // Socket-option setters below have no live/runtime effect on an already-open
  // socket: they stage a value that is only applied the next time start()
  // builds a fresh connection (e.g. after stop() + start() again). Calling
  // one of these while already connected is a deferred no-op until restart,
  // not an immediate change (#436).
  TcpClient& tcp_no_delay(bool enable = true);
  TcpClient& keep_alive(bool enable = true);
  TcpClient& send_buffer_size(size_t bytes);
  TcpClient& receive_buffer_size(size_t bytes);
  TcpClient& manage_external_context(bool manage);

 private:
  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};

}  // namespace wrapper
}  // namespace unilink
