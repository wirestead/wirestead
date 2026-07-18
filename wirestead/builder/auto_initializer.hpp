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

#include "wirestead/base/visibility.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"

namespace wirestead {
namespace builder {

/**
 * @brief Helper class that automatically initializes IoContextManager in Builder pattern
 *
 * This class automatically starts IoContextManager when using Builder pattern,
 * eliminating the need for manual initialization by users.
 */
class WIRESTEAD_API AutoInitializer {
 public:
  /**
   * @brief Automatically start IoContextManager if not running
   *
   * This method is thread-safe and can be called multiple times safely.
   * If already running, it does nothing.
   */
  static void ensure_io_context_running() {
    if (!concurrency::IoContextManager::instance().is_running()) {
      std::lock_guard<std::mutex> lock(init_mutex());
      // Double-check locking
      if (!concurrency::IoContextManager::instance().is_running()) {
        concurrency::IoContextManager::instance().start();
      }
    }
  }

  /**
   * @brief Check if IoContextManager is running
   */
  static bool io_context_running() { return concurrency::IoContextManager::instance().is_running(); }

 private:
  static std::mutex& init_mutex();
};

}  // namespace builder
}  // namespace wirestead
