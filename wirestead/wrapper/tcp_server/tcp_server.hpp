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

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "wirestead/base/constants.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/wrapper/iserver.hpp"

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
 * @brief Modernized TCP Server Wrapper
 */
class WIRESTEAD_API TcpServer : public ServerInterface {
 public:
  explicit TcpServer(uint16_t port);
  TcpServer(uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc);
  explicit TcpServer(std::shared_ptr<interface::Channel> channel);
  ~TcpServer() override;

  // Move semantics
  TcpServer(TcpServer&&) noexcept;
  TcpServer& operator=(TcpServer&&) noexcept;

  // Disable copy
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

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
  TcpServer& on_connect(ConnectionHandler handler) override;
  TcpServer& on_disconnect(ConnectionHandler handler) override;
  TcpServer& on_data(MessageHandler handler) override;
  TcpServer& on_data_batch(BatchMessageHandler handler) override;
  TcpServer& on_error(ErrorHandler handler) override;
  TcpServer& on_backpressure(std::function<void(size_t)> handler) override;

  TcpServer& framer(FramerFactory factory) override;
  TcpServer& on_message(MessageHandler handler) override;
  TcpServer& on_message_batch(BatchMessageHandler handler) override;

  // Client count and management
  size_t client_count() const override;
  std::vector<ClientId> connected_clients() const override;

  // Configuration (Fluent API)
  TcpServer& auto_start(bool manage = true) override;
  TcpServer& bind_address(const std::string& address);
  // Opt into the shared IoContextManager singleton instead of the default
  // dedicated io_context + thread (#440). Must be set before the first
  // start() call to take effect. Only meaningful for deliberately trading
  // per-instance parallelism for reduced thread/memory overhead across many
  // servers in one process; most callers should not need this.
  TcpServer& shared_context(bool use_shared = true);
  TcpServer& port_retry(bool enable = true, int max_retries = 3, int retry_interval_ms = 1000);
  /**
   * @brief Configure application-level idle timeout for accepted sessions.
   *
   * A value of 0ms disables idle timeout. When enabled, only the idle client
   * session is closed; the server keeps listening for new connections.
   */
  TcpServer& idle_timeout(std::chrono::milliseconds timeout);
  TcpServer& max_clients(size_t max);
  TcpServer& backpressure_threshold(size_t threshold);
  TcpServer& backpressure_strategy(base::constants::BackpressureStrategy strategy);
  TcpServer& tcp_no_delay(bool enable = true);
  TcpServer& keep_alive(bool enable = true);
  TcpServer& send_buffer_size(size_t bytes);
  TcpServer& receive_buffer_size(size_t bytes);
  TcpServer& manage_external_context(bool manage);
  TcpServer& batch_size(size_t size);
  TcpServer& batch_latency(std::chrono::milliseconds latency);

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
