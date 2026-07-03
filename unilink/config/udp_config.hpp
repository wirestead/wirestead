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

#include <cstdint>
#include <optional>
#include <string>

#include "unilink/base/constants.hpp"
#include "unilink/util/input_validator.hpp"

namespace unilink {
namespace config {

struct UdpConfig {
  std::string bind_address = "0.0.0.0";
  uint16_t local_port = 0;
  std::optional<std::string> remote_address;
  std::optional<uint16_t> remote_port;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_broadcast = false;
  bool reuse_address = false;
  bool enable_memory_pool = true;
  bool stop_on_callback_exception = false;
  size_t send_buffer_size = 0;
  size_t receive_buffer_size = 0;

  bool is_valid() const {
    // bind_address/remote_address are passed straight to
    // boost::asio::ip::make_address() (unilink/transport/udp/udp.cc) -
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
}  // namespace unilink
