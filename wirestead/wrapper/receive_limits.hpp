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

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace wirestead::wrapper {
// Bounds library-retained receive payloads and batch context records, not
// kernel buffers, allocator bookkeeping or copies retained by application code.
struct ReceiveLimits {
  size_t max_bytes = 64 * 1024 * 1024;
  size_t max_frame_bytes = 64 * 1024;
  size_t max_sessions = 1024;

  void validate() const {
    if (!max_bytes || !max_frame_bytes || max_frame_bytes > max_bytes || !max_sessions)
      throw std::invalid_argument("receive limits must be positive and frame bytes must fit the total byte limit");
  }
};

struct ReceiveMemoryStats {
  size_t reserved_bytes = 0;
  size_t peak_reserved_bytes = 0;
  size_t sessions = 0;
  uint64_t overflow_events = 0;
  uint64_t overflow_bytes = 0;
};
}  // namespace wirestead::wrapper
