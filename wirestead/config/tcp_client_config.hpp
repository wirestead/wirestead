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

struct TcpClientConfig {
  std::string host = "127.0.0.1";
  uint16_t port = 9000;
  unsigned retry_interval_ms = base::constants::DEFAULT_RETRY_INTERVAL_MS;
  unsigned connection_timeout_ms = base::constants::DEFAULT_CONNECTION_TIMEOUT_MS;
  unsigned idle_timeout_ms = base::constants::DEFAULT_IDLE_TIMEOUT_MS;  // 0 = disabled
  IdleTimeoutAction idle_timeout_action = IdleTimeoutAction::Reconnect;
  int max_retries = base::constants::DEFAULT_MAX_RETRIES;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  bool tcp_no_delay = base::constants::DEFAULT_TCP_NO_DELAY;
  bool keep_alive = base::constants::DEFAULT_KEEP_ALIVE;
  size_t send_buffer_size = 0;
  size_t receive_buffer_size = 0;

  TcpClientConfig() = default;
  TcpClientConfig(const TcpClientConfig&) = default;
  TcpClientConfig& operator=(const TcpClientConfig&) = default;
  TcpClientConfig(TcpClientConfig&&) noexcept = default;
  TcpClientConfig& operator=(TcpClientConfig&&) noexcept = default;

  // Validation methods
  bool is_valid() const {
    return util::InputValidator::is_valid_host(host) && port > 0 &&
           retry_interval_ms >= base::constants::MIN_RETRY_INTERVAL_MS &&
           retry_interval_ms <= base::constants::MAX_RETRY_INTERVAL_MS &&
           connection_timeout_ms >= base::constants::MIN_CONNECTION_TIMEOUT_MS &&
           connection_timeout_ms <= base::constants::MAX_CONNECTION_TIMEOUT_MS &&
           (idle_timeout_ms == 0 || (idle_timeout_ms >= base::constants::MIN_IDLE_TIMEOUT_MS &&
                                     idle_timeout_ms <= base::constants::MAX_IDLE_TIMEOUT_MS)) &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD &&
           (send_buffer_size == 0 || (send_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                      send_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE)) &&
           (receive_buffer_size == 0 || (receive_buffer_size >= base::constants::MIN_SOCKET_BUFFER_SIZE &&
                                         receive_buffer_size <= base::constants::MAX_SOCKET_BUFFER_SIZE)) &&
           (max_retries == -1 || (max_retries >= 0 && max_retries <= base::constants::MAX_RETRIES_LIMIT));
  }

  // Apply validation and clamp values to valid ranges
  void validate_and_clamp() {
    if (retry_interval_ms < base::constants::MIN_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
    } else if (retry_interval_ms > base::constants::MAX_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS;
    }

    if (connection_timeout_ms < base::constants::MIN_CONNECTION_TIMEOUT_MS) {
      connection_timeout_ms = base::constants::MIN_CONNECTION_TIMEOUT_MS;
    } else if (connection_timeout_ms > base::constants::MAX_CONNECTION_TIMEOUT_MS) {
      connection_timeout_ms = base::constants::MAX_CONNECTION_TIMEOUT_MS;
    }

    if (idle_timeout_ms != 0) {
      if (idle_timeout_ms < base::constants::MIN_IDLE_TIMEOUT_MS) {
        idle_timeout_ms = base::constants::MIN_IDLE_TIMEOUT_MS;
      } else if (idle_timeout_ms > base::constants::MAX_IDLE_TIMEOUT_MS) {
        idle_timeout_ms = base::constants::MAX_IDLE_TIMEOUT_MS;
      }
    }

    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;
    } else if (backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD;
    }

    if (max_retries != -1 && max_retries > base::constants::MAX_RETRIES_LIMIT) {
      max_retries = base::constants::MAX_RETRIES_LIMIT;
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
