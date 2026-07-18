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

#include "wirestead/memory/memory_tracker.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {
namespace memory {

MemoryTracker& MemoryTracker::instance() {
  static MemoryTracker instance;
  return instance;
}

void MemoryTracker::track_allocation(void* ptr, size_t size, const char* file, int line, const char* function) {
  if (!tracking_enabled_.load()) {
    return;
  }

  std::lock_guard<std::mutex> lock(allocations_mutex_);

  AllocationInfo info;
  info.ptr = ptr;
  info.size = size;
  info.file = file ? file : "unknown";
  info.line = line;
  info.function = function ? function : "unknown";
  info.timestamp = std::chrono::steady_clock::now();

  allocations_[ptr] = info;

  // Update statistics
  stats_.total_allocations++;
  stats_.current_allocations++;
  stats_.total_bytes_allocated += size;
  stats_.current_bytes_allocated += size;

  // Update peak values
  if (stats_.current_allocations > stats_.peak_allocations) {
    stats_.peak_allocations = stats_.current_allocations;
  }
  if (stats_.current_bytes_allocated > stats_.peak_bytes_allocated) {
    stats_.peak_bytes_allocated = stats_.current_bytes_allocated;
  }
}

void MemoryTracker::track_deallocation(void* ptr) {
  if (!tracking_enabled_.load()) {
    return;
  }

  std::lock_guard<std::mutex> lock(allocations_mutex_);

  auto it = allocations_.find(ptr);
  if (it != allocations_.end()) {
    size_t size = it->second.size;
    allocations_.erase(it);

    // Update statistics
    stats_.total_deallocations++;
    stats_.current_allocations--;
    stats_.total_bytes_deallocated += size;
    stats_.current_bytes_allocated -= size;
  }
}

MemoryTracker::MemoryStats MemoryTracker::stats() const { return stats_; }

std::vector<MemoryTracker::AllocationInfo> MemoryTracker::current_allocations() const {
  std::lock_guard<std::mutex> lock(allocations_mutex_);

  std::vector<AllocationInfo> result;
  result.reserve(allocations_.size());

  for (const auto& pair : allocations_) {
    result.push_back(pair.second);
  }

  return result;
}

std::vector<MemoryTracker::AllocationInfo> MemoryTracker::leaked_allocations() const {
  return current_allocations();  // Current allocations are potential leaks
}

void MemoryTracker::enable_tracking(bool enable) { tracking_enabled_.store(enable); }

void MemoryTracker::disable_tracking() { tracking_enabled_.store(false); }

bool MemoryTracker::tracking_enabled() const { return tracking_enabled_.load(); }

void MemoryTracker::clear_tracking_data() {
  std::lock_guard<std::mutex> lock(allocations_mutex_);
  allocations_.clear();

  // Reset statistics
  stats_.total_allocations = 0;
  stats_.total_deallocations = 0;
  stats_.current_allocations = 0;
  stats_.peak_allocations = 0;
  stats_.total_bytes_allocated = 0;
  stats_.total_bytes_deallocated = 0;
  stats_.current_bytes_allocated = 0;
  stats_.peak_bytes_allocated = 0;
}

void MemoryTracker::print_memory_report() const {
  auto mem_stats = stats();
  auto allocs = current_allocations();

  std::cout << "\n=== Memory Tracker Report ===" << std::endl;
  std::cout << "Total allocations: " << mem_stats.total_allocations << std::endl;
  std::cout << "Total deallocations: " << mem_stats.total_deallocations << std::endl;
  std::cout << "Current allocations: " << mem_stats.current_allocations << std::endl;
  std::cout << "Peak allocations: " << mem_stats.peak_allocations << std::endl;
  std::cout << "Total bytes allocated: " << mem_stats.total_bytes_allocated << std::endl;
  std::cout << "Total bytes deallocated: " << mem_stats.total_bytes_deallocated << std::endl;
  std::cout << "Current bytes allocated: " << mem_stats.current_bytes_allocated << std::endl;
  std::cout << "Peak bytes allocated: " << mem_stats.peak_bytes_allocated << std::endl;
  std::cout << "Current active allocations: " << allocs.size() << std::endl;

  if (!allocs.empty()) {
    std::cout << "\n=== Current Allocations ===" << std::endl;
    for (const auto& alloc : allocs) {
      std::cout << "Ptr: " << alloc.ptr << ", Size: " << alloc.size << ", File: " << alloc.file << ":" << alloc.line
                << ", Function: " << alloc.function << std::endl;
    }
  }
}

void MemoryTracker::print_leak_report() const {
  auto leaks = leaked_allocations();

  if (leaks.empty()) {
    std::cout << "\n=== No Memory Leaks Detected ===" << std::endl;
    return;
  }

  std::cout << "\n=== Memory Leak Report ===" << std::endl;
  std::cout << "Found " << leaks.size() << " potential memory leaks:" << std::endl;

  size_t total_leaked_bytes = 0;
  for (const auto& alloc : leaks) {
    std::cout << "Leaked: " << alloc.size << " bytes at " << alloc.ptr << " allocated in " << alloc.file << ":"
              << alloc.line << " (" << alloc.function << ")" << std::endl;
    total_leaked_bytes += alloc.size;
  }

  std::cout << "Total leaked bytes: " << total_leaked_bytes << std::endl;
}

void MemoryTracker::log_memory_report() const {
  auto mem_stats = stats();
  auto allocs = current_allocations();

  std::ostringstream oss;
  oss << "\n=== Memory Tracker Report ===\n";
  oss << "Total allocations: " << mem_stats.total_allocations << "\n";
  oss << "Total deallocations: " << mem_stats.total_deallocations << "\n";
  oss << "Current allocations: " << mem_stats.current_allocations << "\n";
  oss << "Peak allocations: " << mem_stats.peak_allocations << "\n";
  oss << "Total bytes allocated: " << mem_stats.total_bytes_allocated << "\n";
  oss << "Total bytes deallocated: " << mem_stats.total_bytes_deallocated << "\n";
  oss << "Current bytes allocated: " << mem_stats.current_bytes_allocated << "\n";
  oss << "Peak bytes allocated: " << mem_stats.peak_bytes_allocated << "\n";
  oss << "Current active allocations: " << allocs.size();

  WIRESTEAD_LOG_INFO("memory_tracker", "report", oss.str());

  if (!allocs.empty()) {
    std::ostringstream alloc_oss;
    alloc_oss << "\n=== Current Allocations ===\n";
    for (const auto& alloc : allocs) {
      alloc_oss << "Ptr: " << alloc.ptr << ", Size: " << alloc.size << ", File: " << alloc.file << ":" << alloc.line
                << ", Function: " << alloc.function << "\n";
    }
    WIRESTEAD_LOG_INFO("memory_tracker", "allocations", alloc_oss.str());
  }
}

void MemoryTracker::log_leak_report() const {
  auto leaks = leaked_allocations();

  if (leaks.empty()) {
    WIRESTEAD_LOG_INFO("memory_tracker", "leak_check", "No Memory Leaks Detected");
    return;
  }

  std::ostringstream oss;
  oss << "\n=== Memory Leak Report ===\n";
  oss << "Found " << leaks.size() << " potential memory leaks:\n";

  size_t total_leaked_bytes = 0;
  for (const auto& alloc : leaks) {
    oss << "Leaked: " << alloc.size << " bytes at " << alloc.ptr << " allocated in " << alloc.file << ":" << alloc.line
        << " (" << alloc.function << ")\n";
    total_leaked_bytes += alloc.size;
  }

  oss << "Total leaked bytes: " << total_leaked_bytes;
  WIRESTEAD_LOG_ERROR("memory_tracker", "leak_check", oss.str());
}

// ScopedMemoryTracker implementation
ScopedMemoryTracker::ScopedMemoryTracker(const char* file, int line, const char* function)
    : file_(file), line_(line), function_(function) {}

ScopedMemoryTracker::~ScopedMemoryTracker() {
  // Destructor can be used for cleanup if needed
}

void ScopedMemoryTracker::track_allocation(void* ptr, size_t size) {
  MemoryTracker::instance().track_allocation(ptr, size, file_, line_, function_);
}

void ScopedMemoryTracker::track_deallocation(void* ptr) { MemoryTracker::instance().track_deallocation(ptr); }

}  // namespace memory
}  // namespace wirestead
