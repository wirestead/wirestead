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

#include "wirestead/factory/channel_factory.hpp"

#include "wirestead/transport/serial/boost_serial_port.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server.hpp"

namespace wirestead {
namespace factory {

std::shared_ptr<interface::Channel> ChannelFactory::create(const ChannelOptions& options,
                                                           std::shared_ptr<boost::asio::io_context> external_ioc) {
  return std::visit(
      [&external_ioc](const auto& config) -> std::shared_ptr<interface::Channel> {
        using T = std::decay_t<decltype(config)>;

        if constexpr (std::is_same_v<T, config::TcpClientConfig>) {
          return create_tcp_client(config, external_ioc);
        } else if constexpr (std::is_same_v<T, config::TcpServerConfig>) {
          return create_tcp_server(config, external_ioc);
        } else if constexpr (std::is_same_v<T, config::SerialConfig>) {
          return create_serial(config, external_ioc);
        } else if constexpr (std::is_same_v<T, config::UdpConfig>) {
          return create_udp(config, external_ioc);
        } else if constexpr (std::is_same_v<T, config::UdsClientConfig>) {
          return create_uds_client(config, external_ioc);
        } else if constexpr (std::is_same_v<T, config::UdsServerConfig>) {
          return create_uds_server(config, external_ioc);
        } else {
          static_assert(std::is_same_v<T, void>, "Unsupported config type");
          return nullptr;
        }
      },
      options);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_tcp_server(
    const config::TcpServerConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    auto acceptor = std::make_unique<transport::BoostTcpAcceptor>(*external_ioc);
    return transport::TcpServer::create(cfg, std::move(acceptor), *external_ioc);
  }
  return transport::TcpServer::create(cfg, cfg.use_shared_context);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_tcp_client(
    const config::TcpClientConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    return transport::TcpClient::create(cfg, *external_ioc);
  }
  return transport::TcpClient::create(cfg);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_serial(
    const config::SerialConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    return transport::Serial::create(cfg, std::make_unique<transport::BoostSerialPort>(*external_ioc), *external_ioc);
  }
  return transport::Serial::create(cfg, cfg.use_shared_context);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_udp(const config::UdpConfig& cfg,
                                                               std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    return transport::UdpChannel::create(cfg, *external_ioc);
  }
  return transport::UdpChannel::create(cfg);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_uds_server(
    const config::UdsServerConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    auto acceptor = std::make_unique<transport::BoostUdsAcceptor>(*external_ioc);
    return transport::UdsServer::create(cfg, std::move(acceptor), *external_ioc);
  }
  return transport::UdsServer::create(cfg);
}

std::shared_ptr<interface::Channel> ChannelFactory::create_uds_client(
    const config::UdsClientConfig& cfg, std::shared_ptr<boost::asio::io_context> external_ioc) {
  if (external_ioc) {
    return transport::UdsClient::create(cfg, *external_ioc);
  }
  return transport::UdsClient::create(cfg);
}

}  // namespace factory
}  // namespace wirestead
