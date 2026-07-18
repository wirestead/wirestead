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
#include <variant>

#include "wirestead/base/visibility.hpp"
#include "wirestead/config/serial_config.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/config/uds_config.hpp"
#include "wirestead/interface/channel.hpp"

namespace wirestead {
namespace factory {

/**
 * Channel Factory
 * - Uses existing Transport classes
 * - Maintains backward compatibility
 */
class WIRESTEAD_API ChannelFactory {
 public:
  using ChannelOptions = std::variant<config::TcpClientConfig, config::TcpServerConfig, config::SerialConfig,
                                      config::UdpConfig, config::UdsClientConfig, config::UdsServerConfig>;

  // Channel creation
  static std::shared_ptr<interface::Channel> create(const ChannelOptions& options,
                                                    std::shared_ptr<boost::asio::io_context> external_ioc = nullptr);

 private:
  // Creation functions for each Transport type
  static std::shared_ptr<interface::Channel> create_tcp_server(const config::TcpServerConfig& cfg,
                                                               std::shared_ptr<boost::asio::io_context> external_ioc);
  static std::shared_ptr<interface::Channel> create_tcp_client(const config::TcpClientConfig& cfg,
                                                               std::shared_ptr<boost::asio::io_context> external_ioc);
  static std::shared_ptr<interface::Channel> create_serial(const config::SerialConfig& cfg,
                                                           std::shared_ptr<boost::asio::io_context> external_ioc);
  static std::shared_ptr<interface::Channel> create_udp(const config::UdpConfig& cfg,
                                                        std::shared_ptr<boost::asio::io_context> external_ioc);
  static std::shared_ptr<interface::Channel> create_uds_server(const config::UdsServerConfig& cfg,
                                                               std::shared_ptr<boost::asio::io_context> external_ioc);
  static std::shared_ptr<interface::Channel> create_uds_client(const config::UdsClientConfig& cfg,
                                                               std::shared_ptr<boost::asio::io_context> external_ioc);
};

}  // namespace factory
}  // namespace wirestead
