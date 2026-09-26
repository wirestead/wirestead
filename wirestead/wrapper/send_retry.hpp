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

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace wirestead::wrapper::detail {
// A ready capacity snapshot is not a reservation. Concurrent senders can win
// admission first; briefly release the CPU before polling the original pin again.
// No wrapper/session lock may be held here. Callback callers never reach retries.
inline void pause_send_retry(std::condition_variable& cv, std::mutex& mutex) {
  std::unique_lock<std::mutex> lock(mutex);
  cv.wait_for(lock, std::chrono::milliseconds(1));
}
}  // namespace wirestead::wrapper::detail
