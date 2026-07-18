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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "wirestead/diagnostics/error_types.hpp"

namespace wirestead {

/**
 * @brief Represents a decision on whether to retry a connection attempt.
 */
struct ReconnectDecision {
  bool retry{false};
  std::chrono::milliseconds delay{0};
};

/**
 * @brief Function type for determining reconnection policy.
 *
 * Accepts the last error information and the current attempt count (0-based).
 * Returns a ReconnectDecision.
 */
using ReconnectPolicy = std::function<ReconnectDecision(const diagnostics::ErrorInfo&, uint32_t)>;

/**
 * @brief Creates a policy that retries with a fixed interval.
 *
 * @param delay The delay between retries.
 * @return A ReconnectPolicy function.
 */
inline ReconnectPolicy FixedInterval(std::chrono::milliseconds delay) {
  return [delay](const diagnostics::ErrorInfo& error_info, uint32_t) -> ReconnectDecision {
    if (!error_info.retryable) {
      return {false, std::chrono::milliseconds(0)};
    }
    return {true, delay};
  };
}

/**
 * @brief Creates a policy that retries with exponential backoff.
 *
 * @param min_delay The initial delay.
 * @param max_delay The maximum delay cap.
 * @param factor The multiplier for each retry (default 2.0).
 * @param jitter Whether to add randomization to the delay (default true).
 * @return A ReconnectPolicy function.
 */
inline ReconnectPolicy ExponentialBackoff(std::chrono::milliseconds min_delay, std::chrono::milliseconds max_delay,
                                          double factor = 2.0, bool jitter = true) {
  struct ProtectedRng {
    std::mt19937 rng;
    std::mutex mtx;

    ProtectedRng() {
      auto seed = static_cast<unsigned int>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
      rng.seed(seed);
    }
  };

  std::shared_ptr<ProtectedRng> shared_rng;
  if (jitter) {
    shared_rng = std::make_shared<ProtectedRng>();
  }

  return [min_delay, max_delay, factor, shared_rng](const diagnostics::ErrorInfo& error_info,
                                                    uint32_t attempt_count) -> ReconnectDecision {
    if (!error_info.retryable) {
      return {false, std::chrono::milliseconds(0)};
    }

    double calculated = static_cast<double>(min_delay.count()) * std::pow(factor, attempt_count);
    double cap = static_cast<double>(max_delay.count());

    // Clamp to max_delay
    double delay_ms = std::min(calculated, cap);

    if (shared_rng) {
      std::lock_guard<std::mutex> lock(shared_rng->mtx);
      // Full Jitter: random between 0 and calculated delay
      std::uniform_real_distribution<> dist(0.0, delay_ms);
      delay_ms = dist(shared_rng->rng);
    }

    return {true, std::chrono::milliseconds(static_cast<long long>(delay_ms))};
  };
}

}  // namespace wirestead
