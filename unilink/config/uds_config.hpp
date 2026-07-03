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

/**
 * @brief Configuration for UDS Client
 */
struct UdsClientConfig {
  std::string socket_path = "/tmp/unilink.sock";
  unsigned retry_interval_ms = base::constants::DEFAULT_RETRY_INTERVAL_MS;
  unsigned connection_timeout_ms = base::constants::DEFAULT_CONNECTION_TIMEOUT_MS;
  int max_retries = base::constants::DEFAULT_MAX_RETRIES;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;

  UdsClientConfig() = default;

  bool is_valid() const {
    return util::InputValidator::is_valid_uds_path(socket_path) &&
           retry_interval_ms >= base::constants::MIN_RETRY_INTERVAL_MS &&
           retry_interval_ms <= base::constants::MAX_RETRY_INTERVAL_MS &&
           connection_timeout_ms >= base::constants::MIN_CONNECTION_TIMEOUT_MS &&
           connection_timeout_ms <= base::constants::MAX_CONNECTION_TIMEOUT_MS &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD &&
           (max_retries == -1 || (max_retries >= 0 && max_retries <= base::constants::MAX_RETRIES_LIMIT));
  }

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

    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;
    } else if (backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD;
    }

    if (max_retries != -1 && max_retries > base::constants::MAX_RETRIES_LIMIT) {
      max_retries = base::constants::MAX_RETRIES_LIMIT;
    }
  }
};

/**
 * @brief Configuration for UDS Server
 */
struct UdsServerConfig {
  std::string socket_path = "/tmp/unilink.sock";
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  int max_connections = 0;  // Maximum concurrent connections (0 = unlimited)
  int idle_timeout_ms = 0;  // Idle connection timeout in milliseconds (0 = disabled)

  bool is_valid() const {
    return util::InputValidator::is_valid_uds_path(socket_path) &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD && max_connections >= 0 &&
           (idle_timeout_ms == 0 || (idle_timeout_ms >= static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS) &&
                                     idle_timeout_ms <= static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS)));
  }

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
    } else if (idle_timeout_ms != 0) {
      if (idle_timeout_ms < static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS)) {
        idle_timeout_ms = static_cast<int>(base::constants::MIN_IDLE_TIMEOUT_MS);
      } else if (idle_timeout_ms > static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS)) {
        idle_timeout_ms = static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS);
      }
    }
  }
};

}  // namespace config
}  // namespace unilink
