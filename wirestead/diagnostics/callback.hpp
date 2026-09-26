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
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "wirestead/diagnostics/logger.hpp"

namespace wirestead::diagnostics {
// Notification exceptions never become another user error notification.
// Even a logging failure must not escape an I/O completion.
template <class Callback, class... Args>
bool invoke_callback(std::string_view component, std::string_view operation, const Callback& callback,
                     Args&&... args) noexcept {
  if (!callback) return true;
  try {
    callback(std::forward<Args>(args)...);
    return true;
  } catch (const std::exception& error) {
    try {
      WIRESTEAD_LOG_ERROR(component, operation, std::string("Exception in callback: ") + error.what());
    } catch (...) {
    }
  } catch (...) {
    try {
      WIRESTEAD_LOG_ERROR(component, operation, "Unknown exception in callback");
    } catch (...) {
    }
  }
  return false;
}
template <class Callback, class... Args>
bool invoke_callback(std::string_view component, std::string_view operation, const std::shared_ptr<Callback>& callback,
                     Args&&... args) noexcept {
  return !callback || invoke_callback(component, operation, *callback, std::forward<Args>(args)...);
}
}  // namespace wirestead::diagnostics
