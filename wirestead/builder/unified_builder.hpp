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

#include <string>

#include "wirestead/builder/serial_builder.hpp"
#include "wirestead/builder/tcp_client_builder.hpp"
#include "wirestead/builder/tcp_server_builder.hpp"
#include "wirestead/builder/udp_builder.hpp"
#include "wirestead/builder/uds_builder.hpp"

namespace wirestead {
namespace builder {

/**
 * @brief Main entry point for creating communication builders
 *
 * UnifiedBuilder provides a static interface for creating various
 * types of communication builders (TCP, UDP, UDS, Serial).
 */
class WIRESTEAD_API UnifiedBuilder {
 public:
  /**
   * @brief Create a TcpServer builder
   * @param port The port to listen on
   * @return TcpServerBuilderDefault A configured builder for TCP server
   */
  static TcpServerBuilderDefault tcp_server(uint16_t port);

  /**
   * @brief Create a TcpClient builder
   * @param host The host address to connect to
   * @param port The port number to connect to
   * @return TcpClientBuilderDefault A configured builder for TcpClient
   */
  static TcpClientBuilderDefault tcp_client(const std::string& host, uint16_t port);

  /**
   * @brief Create a Serial builder
   * @param device The serial device path (e.g., "/dev/ttyUSB0")
   * @param baud_rate The baud rate for serial communication
   * @return SerialBuilderDefault A configured builder for serial communication
   */
  static SerialBuilderDefault serial(const std::string& device, uint32_t baud_rate);

  /**
   * @brief Create a UdpClient builder
   * @param local_port The local port to bind
   * @return UdpClientBuilderDefault A configured builder for UDP communication
   */
  static UdpClientBuilderDefault udp_client(uint16_t local_port);

  /**
   * @brief Create a UdpServer builder
   * @param local_port The local port to bind
   * @return UdpServerBuilderDefault A configured builder for UDP communication
   */
  static UdpServerBuilderDefault udp_server(uint16_t local_port);
};

}  // namespace builder
}  // namespace wirestead
