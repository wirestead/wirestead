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

#include "wirestead/builder/unified_builder.hpp"

namespace wirestead {
namespace builder {

TcpServerBuilderDefault UnifiedBuilder::tcp_server(uint16_t port) { return TcpServerBuilderDefault(port); }

TcpClientBuilderDefault UnifiedBuilder::tcp_client(const std::string& host, uint16_t port) {
  return TcpClientBuilderDefault(host, port);
}

SerialBuilderDefault UnifiedBuilder::serial(const std::string& device, uint32_t baud_rate) {
  return SerialBuilderDefault(device, baud_rate);
}

UdpClientBuilderDefault UnifiedBuilder::udp_client(uint16_t local_port) { return UdpClientBuilderDefault(local_port); }

UdpServerBuilderDefault UnifiedBuilder::udp_server(uint16_t local_port) { return UdpServerBuilderDefault(local_port); }

}  // namespace builder
}  // namespace wirestead
