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
#include <mutex>

#include "wirestead/base/common.hpp"

namespace wirestead::wrapper::detail {
// Independent of the send-admission lock. A retry is not a terminal error, and a
// terminal failure before any connection must not invent a disconnect.
struct LifecycleEvents {
  std::mutex mutex;
  bool connected = false;
  bool terminal_error = false;
  struct Notification {
    bool disconnect;
    bool error;
  };
  void reset() {
    std::lock_guard<std::mutex> lock(mutex);
    connected = false;
    terminal_error = false;
  }
  Notification observe(base::LinkState state, bool ready) {
    std::lock_guard<std::mutex> lock(mutex);
    const bool lost = connected && !ready;
    connected = ready;
    if (ready) terminal_error = false;
    const bool error = state == base::LinkState::Error && !terminal_error;
    if (error) terminal_error = true;
    return {lost, error};
  }
};
}  // namespace wirestead::wrapper::detail
