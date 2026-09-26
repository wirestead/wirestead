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

#include "wirestead/base/constants.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace config {

struct TcpServerConfig {
  std::string bind_address = "0.0.0.0";
  uint16_t port = 9000;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  // #437: 0 (unlimited) used to be the default, risking unbounded memory
  // growth from a large number of slow/malicious clients. 0 still means
  // unlimited for callers who explicitly opt into it.
  int max_connections = static_cast<int>(base::constants::DEFAULT_MAX_CONNECTIONS);

  // Port binding retry configuration
  bool enable_port_retry = false;     // Enable port binding retry
  int max_port_retries = 3;           // Maximum number of retry attempts
  int port_retry_interval_ms = 1000;  // Retry interval in milliseconds

  // TLS. Both must be set for the server to serve TLS; leaving them empty keeps
  // the connection in plaintext, which is the default. Only honoured in a build
  // configured with WIRESTEAD_ENABLE_TLS - a build without it rejects a
  // configured certificate at start() rather than silently serving plaintext.
  std::string tls_certificate_file;
  std::string tls_private_key_file;

  [[nodiscard]] bool tls_enabled() const { return !tls_certificate_file.empty() && !tls_private_key_file.empty(); }

  int idle_timeout_ms = static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS);  // 0 = disabled
  bool tcp_no_delay = base::constants::DEFAULT_TCP_NO_DELAY;
  bool keep_alive = base::constants::DEFAULT_KEEP_ALIVE;
  size_t send_buffer_size = 0;
  size_t receive_buffer_size = 0;
  // Size of the per-connection userspace read buffer that each
  // async_read_some() fills. Distinct from receive_buffer_size, which is the
  // kernel's SO_RCVBUF. Raising this reduces read completions and callback
  // dispatches on bulk transfers, at the cost of that much memory per
  // connection.
  size_t read_buffer_size = base::constants::DEFAULT_READ_BUFFER_SIZE;

  // Opt into the shared IoContextManager singleton instead of a dedicated
  // io_context + thread (the default since #440). Only meaningful for
  // deliberately trading per-instance parallelism for reduced thread/memory
  // overhead across many instances in one process.
  bool use_shared_context = false;

  // Validation methods
  bool is_valid() const {
    return read_buffer_size >= base::constants::MIN_READ_BUFFER_SIZE &&
           read_buffer_size <= base::constants::MAX_READ_BUFFER_SIZE &&
           (util::InputValidator::is_valid_ipv4(bind_address) || util::InputValidator::is_valid_ipv6(bind_address)) &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD && max_connections >= 0 &&
           (idle_timeout_ms == 0 || (idle_timeout_ms >= static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS) &&
                                     idle_timeout_ms <= static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS))) &&
           (send_buffer_size == 0 || (send_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                      send_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE)) &&
           (receive_buffer_size == 0 || (receive_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                         receive_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE));
  }

  // Apply validation and clamp values to valid ranges
  void validate_and_clamp() {
    if (read_buffer_size < base::constants::MIN_READ_BUFFER_SIZE) {
      read_buffer_size = base::constants::MIN_READ_BUFFER_SIZE;
    } else if (read_buffer_size > base::constants::MAX_READ_BUFFER_SIZE) {
      read_buffer_size = base::constants::MAX_READ_BUFFER_SIZE;
    }
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
    } else if (idle_timeout_ms != 0) {
      if (idle_timeout_ms < static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS)) {
        idle_timeout_ms = static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS);
      } else if (idle_timeout_ms > static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS)) {
        idle_timeout_ms = static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS);
      }
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
