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

#include <boost/asio/io_context.hpp>
#include <memory>
#include <string>
#include <vector>

#include "wirestead/base/constants.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/wrapper/iserver.hpp"

namespace wirestead {
namespace interface {
class Channel;
}

namespace wrapper {

/**
 * @brief UDP implementation of ServerInterface using virtual sessions.
 */
class WIRESTEAD_API UdpServer : public ServerInterface {
 public:
  explicit UdpServer(uint16_t port);
  explicit UdpServer(const config::UdpConfig& cfg);
  UdpServer(const config::UdpConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc);
  explicit UdpServer(std::shared_ptr<interface::Channel> channel);
  ~UdpServer() override;

  UdpServer(UdpServer&&) noexcept;
  UdpServer& operator=(UdpServer&&) noexcept;

  UdpServer(const UdpServer&) = delete;
  UdpServer& operator=(const UdpServer&) = delete;

  // Lifecycle
  // ServerInterface implementation
  [[nodiscard]] std::future<bool> start() override;
  void stop() override;
  bool listening() const override;
  RuntimeStats stats() const override;
  void reset_stats() override;

  // Transmission
  bool broadcast(std::string_view data) override;
  bool send_to(ClientId client_id, std::string_view data) override;
  bool send_to_blocking(ClientId client_id, std::string_view data) override;
  bool try_send_to(ClientId client_id, std::string_view data) override;
  bool try_broadcast(std::string_view data) override;
  bool broadcast_line(std::string_view line) override;
  bool send_to_line(ClientId client_id, std::string_view line) override;
  bool try_broadcast_line(std::string_view line) override;
  bool try_send_to_line(ClientId client_id, std::string_view line) override;

  // Event handlers
  UdpServer& on_connect(ConnectionHandler handler) override;
  UdpServer& on_disconnect(ConnectionHandler handler) override;
  UdpServer& on_data(MessageHandler handler) override;
  UdpServer& on_data_batch(BatchMessageHandler handler) override;
  UdpServer& on_error(ErrorHandler handler) override;
  UdpServer& on_backpressure(std::function<void(size_t)> handler) override;

  UdpServer& framer(FramerFactory factory) override;
  UdpServer& on_message(MessageHandler handler) override;
  UdpServer& on_message_batch(BatchMessageHandler handler) override;

  // Client count and management
  size_t client_count() const override;
  std::vector<ClientId> connected_clients() const override;

  // Configuration (Fluent API)
  UdpServer& auto_start(bool manage = true) override;
  UdpServer& bind_address(const std::string& address);
  /**
   * @brief Configure application-level idle timeout for virtual sessions.
   *
   * A value of 0ms disables idle timeout. When enabled, stale UDP virtual
   * sessions are removed and a later datagram from the same endpoint creates a
   * new virtual session.
   */
  UdpServer& idle_timeout(std::chrono::milliseconds timeout);
  UdpServer& max_clients(size_t max);
  UdpServer& backpressure_threshold(size_t threshold);
  UdpServer& backpressure_strategy(base::constants::BackpressureStrategy strategy);
  UdpServer& send_buffer_size(size_t bytes);
  UdpServer& receive_buffer_size(size_t bytes);
  UdpServer& manage_external_context(bool manage);
  UdpServer& batch_size(size_t size);
  UdpServer& batch_latency(std::chrono::milliseconds latency);

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
