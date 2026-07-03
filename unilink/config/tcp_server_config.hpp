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
#include <string>

#include "unilink/base/constants.hpp"
#include "unilink/util/input_validator.hpp"

namespace unilink {
namespace config {

struct TcpServerConfig {
  std::string bind_address = "0.0.0.0";
  uint16_t port = 9000;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  int max_connections = 0;  // Maximum concurrent connections (0 = unlimited)

  // Port binding retry configuration
  bool enable_port_retry = false;     // Enable port binding retry
  int max_port_retries = 3;           // Maximum number of retry attempts
  int port_retry_interval_ms = 1000;  // Retry interval in milliseconds

  int idle_timeout_ms = 0;  // Idle connection timeout in milliseconds (0 = disabled)
  bool tcp_no_delay = true;
  bool keep_alive = false;
  size_t send_buffer_size = 0;
  size_t receive_buffer_size = 0;

  // Opt into the shared IoContextManager singleton instead of a dedicated
  // io_context + thread (the default since #440). Only meaningful for
  // deliberately trading per-instance parallelism for reduced thread/memory
  // overhead across many instances in one process.
  bool use_shared_context = false;

  // Validation methods
  bool is_valid() const {
    return (util::InputValidator::is_valid_ipv4(bind_address) || util::InputValidator::is_valid_ipv6(bind_address)) &&
           port > 0 && backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD && max_connections >= 0 &&
           idle_timeout_ms >= 0 &&
           (send_buffer_size == 0 || (send_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                      send_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE)) &&
           (receive_buffer_size == 0 || (receive_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                         receive_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE));
  }

  // Apply validation and clamp values to valid ranges
  void validate_and_clamp() {
    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;
    } else if (backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD;
    }

    if (max_connections < 0) {
      max_connections = 0;
    } else if (max_connections > static_cast<int>(base::constants::MAX_MAX_CONNECTIONS)) {
      max_connections = static_cast<int>(base::constants::MAX_MAX_CONNECTIONS);
    }

    if (idle_timeout_ms < 0) {
      idle_timeout_ms = 0;
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
