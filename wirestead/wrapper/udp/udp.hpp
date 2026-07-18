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
#include "wirestead/config/udp_config.hpp"
#include "wirestead/wrapper/ichannel.hpp"

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
 * @brief Modernized UDP Wrapper
 */
class WIRESTEAD_API UdpClient : public ChannelInterface {
 public:
  explicit UdpClient(const config::UdpConfig& cfg);
  UdpClient(const config::UdpConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc);
  explicit UdpClient(std::shared_ptr<interface::Channel> channel);
  ~UdpClient() override;

  // Move semantics
  UdpClient(UdpClient&&) noexcept;
  UdpClient& operator=(UdpClient&&) noexcept;

  // Disable copy
  UdpClient(const UdpClient&) = delete;
  UdpClient& operator=(const UdpClient&) = delete;

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

  UdpClient& on_data(MessageHandler handler) override;
  UdpClient& on_data_batch(BatchMessageHandler handler) override;
  UdpClient& on_connect(ConnectionHandler handler) override;
  UdpClient& on_disconnect(ConnectionHandler handler) override;
  UdpClient& on_error(ErrorHandler handler) override;
  UdpClient& on_backpressure(std::function<void(size_t)> handler) override;

  UdpClient& framer(std::unique_ptr<framer::IFramer> framer) override;
  UdpClient& on_message(MessageHandler handler) override;
  UdpClient& on_message_batch(BatchMessageHandler handler) override;

  UdpClient& auto_start(bool manage = true) override;

  UdpClient& backpressure_threshold(size_t threshold);
  UdpClient& backpressure_strategy(base::constants::BackpressureStrategy strategy);
  UdpClient& send_buffer_size(size_t bytes);
  UdpClient& receive_buffer_size(size_t bytes);
  UdpClient& manage_external_context(bool manage);
  UdpClient& batch_size(size_t size);
  UdpClient& batch_latency(std::chrono::milliseconds latency);

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
