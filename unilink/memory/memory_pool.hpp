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

#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "unilink/base/visibility.hpp"

namespace unilink {
namespace memory {

/**
 * @brief Selective simplified memory pool with optimized performance
 *
 * Core design principles:
 * - Small pools: Lock-based (fast allocation, low overhead)
 * - Large pools: Lock-free (high concurrency)
 * - Memory alignment: 64-byte alignment for buffers >= 4KB
 * - Minimal statistics: Basic stats only to minimize overhead
 */
class UNILINK_API MemoryPool {
 public:
  // Basic statistics
  struct PoolStats {
    size_t total_allocations{0};
    size_t pool_hits{0};
  };

  // Basic health metrics
  struct HealthMetrics {
    double hit_rate{0.0};
  };

  // Predefined buffer sizes for common use cases
  enum class BufferSize : size_t {
    SMALL = 1024,   // 1KB - small messages
    MEDIUM = 4096,  // 4KB - typical network packets
    LARGE = 16384,  // 16KB - large data transfers
    XLARGE = 65536  // 64KB - bulk operations
  };

  explicit MemoryPool(size_t initial_pool_size = 400, size_t max_pool_size = 2000);
  ~MemoryPool() = default;

  // Non-copyable, non-movable
  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;
  MemoryPool(MemoryPool&&) = delete;
  MemoryPool& operator=(MemoryPool&&) = delete;

  std::unique_ptr<uint8_t[]> acquire(size_t size);
  std::unique_ptr<uint8_t[]> acquire(BufferSize buffer_size);
  void release(std::unique_ptr<uint8_t[]> buffer, size_t size);
  PoolStats stats() const;
  double hit_rate() const;
  // Returns (bytes currently checked out, peak bytes ever checked out
  // concurrently). Unlike a monotonically-increasing allocation counter,
  // both reflect actual acquire()/release() traffic - the first value goes
  // down as buffers are released (#451).
  std::pair<size_t, size_t> memory_usage() const;
  HealthMetrics health_metrics() const;

 private:
  // Simple pool bucket
  struct PoolBucket {
    std::vector<std::unique_ptr<uint8_t[]>> buffers_;
    mutable std::mutex mutex_;
    size_t size_;
    size_t capacity_;

    PoolBucket() : size_{0}, capacity_{0} {}
    PoolBucket(PoolBucket&& other) noexcept;
    PoolBucket& operator=(PoolBucket&& other) noexcept;
    PoolBucket(const PoolBucket&) = delete;
    PoolBucket& operator=(const PoolBucket&) = delete;
  };

  std::array<PoolBucket, 4> buckets_;  // For SMALL, MEDIUM, LARGE, XLARGE

  // Internal statistics (atomic for thread safety)
  std::atomic<size_t> total_allocations_{0};
  std::atomic<size_t> pool_hits_{0};
  // #451: real, fluctuating usage tracking for memory_usage() - incremented
  // in acquire(), decremented in release(), unlike total_allocations_ above
  // which only ever grows.
  std::atomic<size_t> outstanding_bytes_{0};
  std::atomic<size_t> peak_bytes_{0};

  // Helper functions
  PoolBucket& bucket(size_t size);
  size_t bucket_index(size_t size) const;
  void track_acquire(size_t size);
  void track_release(size_t size);

  // Allocation functions
  std::unique_ptr<uint8_t[]> acquire_from_bucket(PoolBucket& bucket);
  std::unique_ptr<uint8_t[]> create_buffer(size_t size);

  // Release functions
  void release_to_bucket(PoolBucket& bucket, std::unique_ptr<uint8_t[]> buffer);

  // Utility functions
  void validate_size(size_t size) const;
};

/**
 * @brief Global memory pool instance
 */
class UNILINK_API GlobalMemoryPool {
 public:
  static MemoryPool& instance();

  // Factory method to create optimized memory pool
  static std::unique_ptr<MemoryPool> create_optimized() {
    return std::make_unique<MemoryPool>(800, 4000);  // Optimized default sizes
  }

  // Factory method to create size-optimized memory pool
  static std::unique_ptr<MemoryPool> create_size_optimized() {
    return std::make_unique<MemoryPool>(1200, 6000);  // Even larger for better concurrency
  }

  // Non-copyable, non-movable
  GlobalMemoryPool() = delete;
  GlobalMemoryPool(const GlobalMemoryPool&) = delete;
  GlobalMemoryPool& operator=(const GlobalMemoryPool&) = delete;
};

/**
 * @brief RAII wrapper for memory pool buffers with enhanced safety
 */
class UNILINK_API PooledBuffer {
 public:
  explicit PooledBuffer(size_t size);
  explicit PooledBuffer(MemoryPool::BufferSize buffer_size);
  // #443: draw from a specific pool (e.g. a per-channel instance) instead of
  // the process-wide GlobalMemoryPool singleton, to avoid cross-channel
  // contention on the singleton's bucket mutexes.
  PooledBuffer(size_t size, MemoryPool& pool);
  PooledBuffer(MemoryPool::BufferSize buffer_size, MemoryPool& pool);
  ~PooledBuffer();

  // Non-copyable, movable
  PooledBuffer(const PooledBuffer&) = delete;
  PooledBuffer& operator=(const PooledBuffer&) = delete;
  PooledBuffer(PooledBuffer&& other) noexcept;
  PooledBuffer& operator=(PooledBuffer&& other) noexcept;

  // Safe access methods
  uint8_t* data() const;
  size_t size() const;
  bool valid() const;

  // Safe array access with bounds checking
  uint8_t& operator[](size_t index);
  const uint8_t& operator[](size_t index) const;

  // Safe array access with bounds checking
  uint8_t& at(size_t index);
  const uint8_t& at(size_t index) const;

  // Explicit conversion methods (no implicit conversion)
  uint8_t* get() const { return data(); }
  explicit operator bool() const { return valid(); }

 private:
  std::unique_ptr<uint8_t[]> buffer_;
  size_t size_;
  MemoryPool* pool_;

  // Helper for bounds checking
  void check_bounds(size_t index) const;
};

}  // namespace memory
}  // namespace unilink
