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

#include <boost/asio/ip/address.hpp>
#include <cstdint>
#include <optional>
#include <string>

#include "wirestead/base/constants.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace config {

// True for an address literal inside the multicast ranges - 224.0.0.0/4 for
// IPv4, ff00::/8 for IPv6. Anything else, including a hostname or a unicast
// address, is rejected: joining a group on a unicast address fails at the
// setsockopt with an error that says nothing about which setting was wrong.
inline bool is_multicast_address(const std::string& address) {
  boost::system::error_code ec;
  const auto parsed = boost::asio::ip::make_address(address, ec);
  if (ec) return false;
  return parsed.is_multicast();
}

struct UdpConfig {
  std::string bind_address = "0.0.0.0";
  uint16_t local_port = 0;
  std::optional<std::string> remote_address;
  std::optional<uint16_t> remote_port;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_broadcast = false;
  bool reuse_address = false;

  // Join this multicast group after binding, so the socket receives datagrams
  // addressed to it. IPv4 groups are 224.0.0.0/4, IPv6 groups are ff00::/8.
  //
  // `multicast_interface` picks which NIC to join on, by its local IPv4
  // address. A robot with a sensor on one interface and a network on another
  // needs it; leave it empty to let the kernel choose, which is only reliable
  // with a single interface. IPv6 always uses the kernel's choice.
  //
  // Receiving is all this covers. Sending to a group already works through
  // remote_address, at the default TTL of 1 - one hop, so the local subnet
  // only, which is where a robot's sensors are.
  std::optional<std::string> multicast_group;
  std::optional<std::string> multicast_interface;
  bool enable_memory_pool = true;
  // Receive callback exceptions request quiet stop when true; false logs and continues receiving.
  bool stop_on_callback_exception = false;
  size_t send_buffer_size = 0;
  size_t receive_buffer_size = 0;

  bool is_valid() const {
    // bind_address/remote_address are passed straight to
    // boost::asio::ip::make_address() (wirestead/transport/udp/udp.cc) -
    // literal IPv4/IPv6 addresses only, no hostname resolution.
    if (!util::InputValidator::is_valid_ipv4(bind_address) && !util::InputValidator::is_valid_ipv6(bind_address)) {
      return false;
    }
    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD ||
        backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      return false;
    }
    if (remote_address.has_value() != remote_port.has_value()) return false;
    if (remote_address && !util::InputValidator::is_valid_ipv4(*remote_address) &&
        !util::InputValidator::is_valid_ipv6(*remote_address)) {
      return false;
    }
    if (remote_port && *remote_port == 0) return false;
    if (multicast_group && !is_multicast_address(*multicast_group)) return false;
    if (multicast_interface && !util::InputValidator::is_valid_ipv4(*multicast_interface)) return false;
    if (send_buffer_size != 0 && (send_buffer_size < base::constants::MIN_SOCKET_BUFFER_SIZE ||
                                  send_buffer_size > base::constants::MAX_SOCKET_BUFFER_SIZE)) {
      return false;
    }
    if (receive_buffer_size != 0 && (receive_buffer_size < base::constants::MIN_SOCKET_BUFFER_SIZE ||
                                     receive_buffer_size > base::constants::MAX_SOCKET_BUFFER_SIZE)) {
      return false;
    }
    return true;
  }

  void validate_and_clamp() {
    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;
    } else if (backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD;
    }

    if (send_buffer_size != 0 && send_buffer_size < base::constants::MIN_SOCKET_BUFFER_SIZE) {
      send_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE;
    } else if (send_buffer_size > base::constants::MAX_SOCKET_BUFFER_SIZE) {
      send_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE;
    }

    if (receive_buffer_size != 0 && receive_buffer_size < base::constants::MIN_SOCKET_BUFFER_SIZE) {
      receive_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE;
    } else if (receive_buffer_size > base::constants::MAX_SOCKET_BUFFER_SIZE) {
      receive_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE;
    }
  }
};

}  // namespace config
}  // namespace wirestead
