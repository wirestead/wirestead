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

namespace wirestead {
namespace wrapper {

struct RuntimeStats {
  uint64_t bytes_accepted = 0;
  uint64_t messages_accepted = 0;

  uint64_t bytes_sent = 0;
  uint64_t messages_sent = 0;

  uint64_t bytes_received = 0;
  uint64_t messages_received = 0;

  uint64_t failed_sends = 0;

  uint64_t dropped_messages = 0;
  uint64_t dropped_bytes = 0;

  uint64_t backpressure_events = 0;

  size_t queued_bytes = 0;
  size_t pending_bytes = 0;
  size_t max_queued_bytes = 0;

  bool backpressure_active = false;
};

}  // namespace wrapper
}  // namespace wirestead
