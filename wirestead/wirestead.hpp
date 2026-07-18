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
#include <variant>

#include "wirestead/base/error_codes.hpp"
#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"

// Public API Context and Interface headers
#include "wirestead/wrapper/context.hpp"
#include "wirestead/wrapper/ichannel.hpp"
#include "wirestead/wrapper/iserver.hpp"
#include "wirestead/wrapper/runtime_stats.hpp"

// Wrapper implementations
#include "wirestead/wrapper/serial/serial.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"
#include "wirestead/wrapper/uds_client/uds_client.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

// High-level Builder API includes
#include "wirestead/builder/ibuilder.hpp"
#include "wirestead/builder/serial_builder.hpp"
#include "wirestead/builder/tcp_client_builder.hpp"
#include "wirestead/builder/tcp_server_builder.hpp"
#include "wirestead/builder/udp_builder.hpp"
#include "wirestead/builder/uds_builder.hpp"
#include "wirestead/builder/unified_builder.hpp"

// Configuration Management API includes (optional)
#ifdef WIRESTEAD_ENABLE_CONFIG
#include "wirestead/config/config_factory.hpp"
#include "wirestead/config/config_manager.hpp"
#include "wirestead/config/iconfig_manager.hpp"
#endif

// Error handling and logging system includes
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {

// === Public Facade Aliases ===

// Core communication classes
using TcpClient = wrapper::TcpClient;
using TcpServer = wrapper::TcpServer;
using Serial = wrapper::Serial;
using UdpClient = wrapper::UdpClient;
using UdpServer = wrapper::UdpServer;
using UdsClient = wrapper::UdsClient;
using UdsServer = wrapper::UdsServer;

// Context classes for callbacks
using MessageContext = wrapper::MessageContext;
using ConnectionContext = wrapper::ConnectionContext;
using ErrorContext = wrapper::ErrorContext;
using RuntimeStats = wrapper::RuntimeStats;

// === Public Builder API Convenience Functions ===

/**
 * @brief Create a TCP server builder
 * @param port The port number for the server
 * @return TcpServerBuilderDefault A configured builder for TcpServer
 */
inline builder::TcpServerBuilderDefault tcp_server(uint16_t port) { return builder::TcpServerBuilderDefault(port); }

/**
 * @brief Create a TCP client builder
 * @param host The host address to connect to
 * @param port The port number to connect to
 * @return TcpClientBuilderDefault A configured builder for TcpClient
 */
inline builder::TcpClientBuilderDefault tcp_client(const std::string& host, uint16_t port) {
  return builder::TcpClientBuilderDefault(host, port);
}

/**
 * @brief Create a UDS server builder
 * @param socket_path The path to the Unix Domain Socket file
 * @return UdsServerBuilderDefault A configured builder for UdsServer
 */
inline builder::UdsServerBuilderDefault uds_server(const std::string& socket_path) {
  return builder::UdsServerBuilderDefault(socket_path);
}

/**
 * @brief Create a UDS client builder
 * @param socket_path The path to the Unix Domain Socket file
 * @return UdsClientBuilderDefault A configured builder for UdsClient
 */
inline builder::UdsClientBuilderDefault uds_client(const std::string& socket_path) {
  return builder::UdsClientBuilderDefault(socket_path);
}

/**
 * @brief Create a Serial port builder
 * @param device The serial device path (e.g., "/dev/ttyUSB0")
 * @param baud_rate The baud rate for serial communication
 * @return SerialBuilderDefault A configured builder for Serial
 */
inline builder::SerialBuilderDefault serial(const std::string& device, uint32_t baud_rate) {
  return builder::SerialBuilderDefault(device, baud_rate);
}

/**
 * @brief Create a UDP client builder
 * @param local_port The local port to bind
 * @return UdpClientBuilderDefault A configured builder for UDP Client
 */
inline builder::UdpClientBuilderDefault udp_client(uint16_t local_port) {
  return builder::UdpClientBuilderDefault(local_port);
}

/**
 * @brief Create a UDP server builder (Virtual sessions)
 * @param local_port The local port to bind
 * @return UdpServerBuilderDefault A configured builder for UDP Server
 */
inline builder::UdpServerBuilderDefault udp_server(uint16_t local_port) {
  return builder::UdpServerBuilderDefault(local_port);
}

}  // namespace wirestead
