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

#include <atomic>

namespace wirestead::transport::detail {
// Internal scheduling seam. Tests can stop immediately before the completion
// signal or observe callers reaching its wait; production leaves this null.
inline std::atomic<void (*)()> g_tcp_io_completion_hook{nullptr};
using StopTestHook = void (*)(const void*, bool);
inline std::atomic<StopTestHook> g_stop_test_hook{nullptr};
inline void stop_test_hook(const void* object, bool completing) {
  if (auto hook = g_stop_test_hook.load(std::memory_order_acquire)) hook(object, completing);
}
}  // namespace wirestead::transport::detail
