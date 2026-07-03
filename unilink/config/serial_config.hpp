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

#include <string>

#include "unilink/base/constants.hpp"

namespace unilink {
namespace config {

struct SerialConfig {
#ifdef _WIN32
  std::string device = "COM1";
#else
  std::string device = "/dev/ttyUSB0";
#endif
  unsigned baud_rate = 115200;
  unsigned char_size = 8;  // 5,6,7,8
  enum class Parity { None, Even, Odd } parity = Parity::None;
  unsigned stop_bits = 1;  // 1 or 2
  enum class Flow { None, Software, Hardware } flow = Flow::None;

  size_t read_chunk = base::constants::DEFAULT_READ_BUFFER_SIZE;
  bool reopen_on_error = true;  // Attempt to reopen on device disconnection/error
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  // Controls whether callback exceptions halt the link (true) or trigger the normal retry flow (false)
  bool stop_on_callback_exception = false;

  unsigned retry_interval_ms = base::constants::DEFAULT_RETRY_INTERVAL_MS;
  int max_retries = base::constants::DEFAULT_MAX_RETRIES;

  // Opt into the shared IoContextManager singleton instead of a dedicated
  // io_context + thread (the default since #440). Only meaningful for
  // deliberately trading per-instance parallelism for reduced thread/memory
  // overhead across many instances in one process.
  bool use_shared_context = false;

  // Validation methods
  bool is_valid() const {
    return !device.empty() && baud_rate > 0 && char_size >= 5 && char_size <= 8 && (stop_bits == 1 || stop_bits == 2) &&
           retry_interval_ms >= base::constants::MIN_RETRY_INTERVAL_MS &&
           retry_interval_ms <= base::constants::MAX_RETRY_INTERVAL_MS &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD &&
           (max_retries == -1 || (max_retries >= 0 && max_retries <= base::constants::MAX_RETRIES_LIMIT));
  }

  // Apply validation and clamp values to valid ranges
  void validate_and_clamp() {
    if (char_size < 5)
      char_size = 5;
    else if (char_size > 8)
      char_size = 8;

    if (stop_bits != 1 && stop_bits != 2) stop_bits = 1;

    if (retry_interval_ms < base::constants::MIN_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
    } else if (retry_interval_ms > base::constants::MAX_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS;
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

}  // namespace config
}  // namespace unilink
