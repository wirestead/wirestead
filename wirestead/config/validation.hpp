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
#include <cctype>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

#include "wirestead/base/constants.hpp"

namespace wirestead::config::detail {
inline void require(bool valid, const char* message) {
  if (!valid) throw std::invalid_argument(message);
}
template <class T, class L, class H>
void range(T value, L low, H high, const char* name) {
  require(!std::cmp_less(value, low) && !std::cmp_greater(value, high), name);
}
inline void strategy(base::constants::BackpressureStrategy value) {
  require(value == base::constants::BackpressureStrategy::Reliable ||
              value == base::constants::BackpressureStrategy::BestEffort,
          "invalid backpressure strategy");
}
inline void duration(std::chrono::milliseconds value, uint64_t low, uint64_t high, bool zero, const char* name) {
  if (zero && value.count() == 0) return;
  range(value.count(), low, high, name);
}
inline void text_option(std::string_view value, std::initializer_list<std::string_view> choices, const char* name) {
  for (auto choice : choices) {
    if (value.size() != choice.size()) continue;
    bool equal = true;
    for (size_t i = 0; i < value.size(); ++i)
      if (std::tolower(static_cast<unsigned char>(value[i])) != choice[i]) {
        equal = false;
        break;
      }
    if (equal) return;
  }
  require(false, name);
}
template <class Config>
void validate(const Config& config) {
  require(config.is_valid(), "invalid transport configuration");
  strategy(config.backpressure_strategy);
  if constexpr (requires { config.idle_timeout_action; })
    require(config.idle_timeout_action == IdleTimeoutAction::Close ||
                config.idle_timeout_action == IdleTimeoutAction::Reconnect,
            "invalid idle timeout action");
  if constexpr (requires { config.max_connections; })
    range(config.max_connections, 0, base::constants::MAX_MAX_CONNECTIONS, "invalid connection limit");
  if constexpr (requires { config.max_port_retries; }) {
    range(config.max_port_retries, 0, base::constants::MAX_RETRIES_LIMIT, "invalid port retry count");
    range(config.port_retry_interval_ms, base::constants::MIN_RETRY_INTERVAL_MS, base::constants::MAX_RETRY_INTERVAL_MS,
          "invalid port retry interval");
    require(config.tls_certificate_file.empty() == config.tls_private_key_file.empty(),
            "TLS requires certificate and key");
  }
  if constexpr (requires { config.parity; }) {
    using C = std::decay_t<Config>;
    require(config.parity == C::Parity::None || config.parity == C::Parity::Even || config.parity == C::Parity::Odd,
            "invalid parity");
    require(config.flow == C::Flow::None || config.flow == C::Flow::Software || config.flow == C::Flow::Hardware,
            "invalid flow control");
  }
}
}  // namespace wirestead::config::detail
