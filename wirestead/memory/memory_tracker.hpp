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
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace memory {

/**
 * @brief Memory allocation tracker for debugging and monitoring
 *
 * Tracks memory allocations and deallocations to detect leaks,
 * monitor usage patterns, and provide debugging information.
 */
class WIRESTEAD_API MemoryTracker {
 public:
  struct AllocationInfo {
    void* ptr;
    size_t size;
    const char* file;
    int line;
    std::chrono::steady_clock::time_point timestamp;
    const char* function;
  };

  struct MemoryStats {
    size_t total_allocations{0};
    size_t total_deallocations{0};
    size_t current_allocations{0};
    size_t peak_allocations{0};
    size_t total_bytes_allocated{0};
    size_t total_bytes_deallocated{0};
    size_t current_bytes_allocated{0};
    size_t peak_bytes_allocated{0};
  };

  // Singleton access
  static MemoryTracker& instance();

  // Tracking methods
  void track_allocation(void* ptr, size_t size, const char* file, int line, const char* function);
  void track_deallocation(void* ptr);

  // Statistics
  MemoryStats stats() const;
  std::vector<AllocationInfo> current_allocations() const;
  std::vector<AllocationInfo> leaked_allocations() const;

  // Control
  void enable_tracking(bool enable = true);
  void disable_tracking();
  bool tracking_enabled() const;

  // Cleanup
  void clear_tracking_data();

  // Debugging
  void print_memory_report() const;
  void print_leak_report() const;

  // Logger-based reporting (recommended for production)
  void log_memory_report() const;
  void log_leak_report() const;

 private:
  MemoryTracker() = default;
  ~MemoryTracker() = default;

  MemoryTracker(const MemoryTracker&) = delete;
  MemoryTracker& operator=(const MemoryTracker&) = delete;

  mutable std::mutex allocations_mutex_;
  std::unordered_map<void*, AllocationInfo> allocations_;
  MemoryStats stats_;
  std::atomic<bool> tracking_enabled_{true};
};

/**
 * @brief RAII helper for automatic memory tracking
 */
class WIRESTEAD_API ScopedMemoryTracker {
 public:
  ScopedMemoryTracker(const char* file, int line, const char* function);
  ~ScopedMemoryTracker();

  void track_allocation(void* ptr, size_t size);
  void track_deallocation(void* ptr);

 private:
  const char* file_;
  int line_;
  const char* function_;
};

}  // namespace memory
}  // namespace wirestead

// Convenience macros for automatic tracking
#ifdef WIRESTEAD_ENABLE_MEMORY_TRACKING
#define MEMORY_TRACK_ALLOCATION(ptr, size) \
  wirestead::memory::MemoryTracker::instance().track_allocation(ptr, size, __FILE__, __LINE__, __FUNCTION__)

#define MEMORY_TRACK_DEALLOCATION(ptr) wirestead::memory::MemoryTracker::instance().track_deallocation(ptr)

#define MEMORY_TRACK_SCOPE() wirestead::memory::ScopedMemoryTracker _mem_tracker(__FILE__, __LINE__, __FUNCTION__)
#else
#define MEMORY_TRACK_ALLOCATION(ptr, size) ((void)0)
#define MEMORY_TRACK_DEALLOCATION(ptr) ((void)0)
#define MEMORY_TRACK_SCOPE() ((void)0)
#endif
