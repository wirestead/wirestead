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
#include <string_view>
#include <vector>

#include "unilink/base/visibility.hpp"
#include "unilink/config/tcp_server_config.hpp"
#include "unilink/diagnostics/error_types.hpp"
#include "unilink/interface/channel.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace unilink {

namespace interface {
class TcpAcceptorInterface;
}

namespace transport {

/**
 * @brief Thread-safe TCP Server implementation
 */
class UNILINK_API TcpServer : public interface::Channel, public std::enable_shared_from_this<TcpServer> {
 public:
  static std::shared_ptr<TcpServer> create(const config::TcpServerConfig& cfg);
  static std::shared_ptr<TcpServer> create(const config::TcpServerConfig& cfg,
                                           std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
                                           boost::asio::io_context& ioc);
  ~TcpServer() override;

  // Move semantics
  TcpServer(TcpServer&&) noexcept;
  TcpServer& operator=(TcpServer&&) noexcept;

  // Non-copyable
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  // Channel implementation
  void start() override;
  void stop() override;
  bool is_connected() const override;
  bool is_backpressure_active() const override;
  bool is_backpressure_active(ClientId client_id) const;
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
  wrapper::RuntimeStats stats() const override;
  void reset_stats() override;
  std::optional<diagnostics::ErrorInfo> last_error_info() const override;

  // Multi-client support
  bool broadcast(std::string_view message);
  bool broadcast(memory::ConstByteSpan data);
  bool send_to_client(ClientId client_id, std::string_view message);
  bool send_to_client(ClientId client_id, memory::ConstByteSpan data);
  bool try_send_to_client(ClientId client_id, std::string_view message);
  bool try_send_to_client(ClientId client_id, memory::ConstByteSpan data);
  size_t client_count() const;
  std::vector<ClientId> connected_clients() const;

  void request_stop();

  using MultiClientConnectHandler = std::function<void(ClientId client_id, const std::string& client_info)>;
  using MultiClientDataHandler = std::function<void(ClientId client_id, memory::ConstByteSpan data)>;
  using MultiClientDisconnectHandler = std::function<void(ClientId client_id)>;

  void on_multi_connect(MultiClientConnectHandler handler);
  void on_multi_data(MultiClientDataHandler handler);
  void on_multi_disconnect(MultiClientDisconnectHandler handler);

  void set_client_limit(size_t max_clients);

  base::LinkState state() const;

 private:
  explicit TcpServer(const config::TcpServerConfig& cfg);
  TcpServer(const config::TcpServerConfig& cfg, std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
            boost::asio::io_context& ioc);

  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};
}  // namespace transport
}  // namespace unilink
