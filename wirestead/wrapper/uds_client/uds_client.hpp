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
 * @brief Modernized UDS Client Wrapper
 */
class WIRESTEAD_API UdsClient : public ChannelInterface {
 public:
  explicit UdsClient(const std::string& socket_path);
  UdsClient(const std::string& socket_path, std::shared_ptr<boost::asio::io_context> external_ioc);
  /// Requires the matching native transport or ConnectionChannel; otherwise throws std::invalid_argument.
  explicit UdsClient(std::shared_ptr<interface::Channel> channel);
  ~UdsClient() override;

  // Move semantics
  UdsClient(UdsClient&&) noexcept;
  UdsClient& operator=(UdsClient&&) noexcept;

  // Disable copy
  UdsClient(const UdsClient&) = delete;
  UdsClient& operator=(const UdsClient&) = delete;

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
  UdsClient& receive_limits(ReceiveLimits limits);
  ReceiveMemoryStats receive_stats() const;
  void reset_stats() override;

  UdsClient& on_data(MessageHandler handler) override;
  UdsClient& on_data_batch(BatchMessageHandler handler) override;
  UdsClient& on_connect(ConnectionHandler handler) override;
  UdsClient& on_disconnect(ConnectionHandler handler) override;
  UdsClient& on_error(ErrorHandler handler) override;
  UdsClient& on_backpressure(std::function<void(size_t)> handler) override;

  UdsClient& framer(std::unique_ptr<framer::IFramer> framer) override;
  UdsClient& on_message(MessageHandler handler) override;
  UdsClient& on_message_batch(BatchMessageHandler handler) override;

  UdsClient& auto_start(bool manage = true) override;

  // Configuration
  UdsClient& retry_interval(std::chrono::milliseconds interval);
  UdsClient& max_retries(int max_retries);
  UdsClient& connection_timeout(std::chrono::milliseconds timeout);
  UdsClient& backpressure_threshold(size_t threshold);

  /**
   * @brief Size of the userspace buffer each read fills, in bytes.
   *
   * Raising this reduces read completions and callback dispatches on bulk
   * transfers, at the cost of that much memory per connection. Clamped to
   * [MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE]; takes effect on the next
   * start().
   */
  UdsClient& read_buffer_size(size_t bytes);
  UdsClient& backpressure_strategy(base::constants::BackpressureStrategy strategy);

  /// Returns the configured backpressure threshold in bytes.
  size_t backpressure_threshold() const;
  /// Returns the configured backpressure strategy.
  base::constants::BackpressureStrategy backpressure_strategy() const;
  UdsClient& manage_external_context(bool manage);
  UdsClient& batch_size(size_t size);
  UdsClient& batch_latency(std::chrono::milliseconds latency);

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
