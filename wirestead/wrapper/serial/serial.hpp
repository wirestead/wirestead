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
#include "wirestead/config/serial_config.hpp"
#include "wirestead/wrapper/ichannel.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

// Forward declaration so Serial can grant it test-only access to
// build_config() (see the friend declaration below).
class SerialBuilderConfigTest;

namespace wirestead {
namespace interface {
class Channel;
}

namespace wrapper {

/**
 * @brief Modernized Serial Wrapper
 */
class WIRESTEAD_API Serial : public ChannelInterface {
 public:
  Serial(const std::string& device, uint32_t baud_rate);
  Serial(const std::string& device, uint32_t baud_rate, std::shared_ptr<boost::asio::io_context> external_ioc);
  explicit Serial(std::shared_ptr<interface::Channel> channel);
  ~Serial() override;

  // Move semantics
  Serial(Serial&&) noexcept;
  Serial& operator=(Serial&&) noexcept;

  // Disable copy
  Serial(const Serial&) = delete;
  Serial& operator=(const Serial&) = delete;

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

  Serial& on_data(MessageHandler handler) override;
  Serial& on_data_batch(BatchMessageHandler handler) override;
  Serial& on_connect(ConnectionHandler handler) override;
  Serial& on_disconnect(ConnectionHandler handler) override;
  Serial& on_error(ErrorHandler handler) override;
  Serial& on_backpressure(std::function<void(size_t)> handler) override;

  Serial& framer(std::unique_ptr<framer::IFramer> framer) override;
  Serial& on_message(MessageHandler handler) override;
  Serial& on_message_batch(BatchMessageHandler handler) override;

  Serial& auto_start(bool manage = true) override;
  // Opt into the shared IoContextManager singleton instead of the default
  // dedicated io_context + thread (#440). Must be set before the first
  // start() call to take effect. Only meaningful for deliberately trading
  // per-instance parallelism for reduced thread/memory overhead across many
  // instances in one process; most callers should not need this.
  Serial& shared_context(bool use_shared = true);

  // Serial-specific methods
  Serial& baud_rate(uint32_t baud_rate);
  Serial& data_bits(int data_bits);
  Serial& stop_bits(int stop_bits);
  Serial& parity(const std::string& parity);
  Serial& flow_control(const std::string& flow_control);
  Serial& reopen_on_error(bool enable);
  Serial& retry_interval(std::chrono::milliseconds interval);
  Serial& backpressure_threshold(size_t threshold);
  Serial& backpressure_strategy(base::constants::BackpressureStrategy strategy);
  Serial& manage_external_context(bool manage);
  Serial& batch_size(size_t size);
  Serial& batch_latency(std::chrono::milliseconds latency);

 protected:
  // Exposed for testing only — not part of the public API.
  friend class ::SerialBuilderConfigTest;
  config::SerialConfig build_config() const;

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
