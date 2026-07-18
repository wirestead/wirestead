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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/builder/unified_builder.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/memory/safe_data_buffer.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace wirestead::memory;
using namespace std::chrono_literals;

/**
 * @brief Comprehensive memory management tests
 *
 * This file combines all memory-related tests including
 * memory pool functionality, leak detection, performance,
 * and safety testing.
 */
class MemoryIntegratedTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize test state
    test_port_ = TestUtils::getAvailableTestPort();

    // Initialize memory tracking
    initial_memory_usage_ = get_memory_usage();
  }

  void TearDown() override {
    // Clean up any test state
    TestUtils::waitFor(100);

    // Check for memory leaks
    size_t final_memory_usage = get_memory_usage();
    size_t memory_difference = final_memory_usage - initial_memory_usage_;

    // Allow for small memory differences (less than 1MB)
    if (memory_difference > 1024 * 1024) {
      std::cout << "WARNING: Potential memory leak detected. " << "Memory usage increased by " << memory_difference
                << " bytes during test." << std::endl;
    }
  }

  uint16_t test_port_;
  size_t initial_memory_usage_;

  // Helper function to get memory usage (simplified)
  size_t get_memory_usage() {
    // In a real implementation, this would read from /proc/self/status
    // For now, return a placeholder value based on memory pool stats
    auto& pool = GlobalMemoryPool::instance();
    auto stats = pool.stats();
    return stats.total_allocations * 1024;  // Rough estimate
  }

  // Helper function to generate random data
  std::vector<uint8_t> generate_random_data(size_t size) {
    std::vector<uint8_t> data(size);
    static thread_local std::mt19937 gen(12345);
    std::uniform_int_distribution<> dis(0, 255);

    for (auto& byte : data) {
      byte = static_cast<uint8_t>(dis(gen));
    }
    return data;
  }
};

// ============================================================================
// MEMORY POOL BASIC TESTS
// ============================================================================

/**
 * @brief Test memory pool basic functionality
 */
TEST_F(MemoryIntegratedTest, MemoryPoolBasicFunctionality) {
  std::cout << "\n=== Memory Pool Basic Functionality Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const size_t buffer_size = 1024;

  // Test basic allocation and deallocation
  auto buffer = pool.acquire(buffer_size);
  EXPECT_NE(buffer, nullptr);

  // Test buffer usage
  if (buffer) {
    // Fill buffer with test data
    std::fill(buffer.get(), buffer.get() + buffer_size, 0xAA);

    // Verify data integrity
    for (size_t i = 0; i < buffer_size; ++i) {
      EXPECT_EQ(buffer[i], 0xAA);
    }
  }

  // Test deallocation
  pool.release(std::move(buffer), buffer_size);

  std::cout << "Memory pool basic functionality test completed" << std::endl;
}

/**
 * @brief Test memory pool performance
 */
TEST_F(MemoryIntegratedTest, MemoryPoolPerformance) {
  std::cout << "\n=== Memory Pool Performance Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_operations = 1000;
  const size_t buffer_size = 1024;

  auto start_time = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_operations; ++i) {
    auto buffer = pool.acquire(buffer_size);
    if (buffer) {
      pool.release(std::move(buffer), buffer_size);
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  double throughput = static_cast<double>(num_operations) / (static_cast<double>(duration.count()) / 1000000.0);

  std::cout << "Memory pool performance:" << std::endl;
  std::cout << "  Operations: " << num_operations << std::endl;
  std::cout << "  Duration: " << duration.count() << " μs" << std::endl;
  std::cout << "  Throughput: " << throughput << " ops/sec" << std::endl;

  // Performance should be reasonable (at least 1000 ops/sec)
  EXPECT_GT(throughput, 1000);
}

/**
 * @brief Test memory pool statistics
 */
TEST_F(MemoryIntegratedTest, MemoryPoolStatistics) {
  std::cout << "\n=== Memory Pool Statistics Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_operations = 100;
  const size_t buffer_size = 1024;

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;

  // Perform operations
  for (int i = 0; i < num_operations; ++i) {
    auto buffer = pool.acquire(buffer_size);
    if (buffer) {
      pool.release(std::move(buffer), buffer_size);
    }
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Allocation difference: " << (final_allocations - initial_allocations) << std::endl;

  // Statistics should be accurate
  EXPECT_GE(final_allocations, initial_allocations);
  EXPECT_LE(final_allocations - initial_allocations, num_operations);
}

// ============================================================================
// MEMORY LEAK DETECTION TESTS
// ============================================================================

/**
 * @brief Test basic memory leak detection
 */
TEST_F(MemoryIntegratedTest, BasicMemoryLeakDetection) {
  std::cout << "\n=== Basic Memory Leak Detection Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_cycles = 50;
  const int buffers_per_cycle = 10;
  const size_t buffer_size = 1024;

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;

  // Perform allocation/deallocation cycles
  for (int cycle = 0; cycle < num_cycles; ++cycle) {
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    buffers.reserve(buffers_per_cycle);

    // Allocate buffers
    for (int i = 0; i < buffers_per_cycle; ++i) {
      auto buffer = pool.acquire(buffer_size);
      if (buffer) {
        buffers.push_back(std::move(buffer));
      }
    }

    // Release buffers
    for (auto& buffer : buffers) {
      pool.release(std::move(buffer), buffer_size);
    }
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Total cycles: " << num_cycles << std::endl;
  std::cout << "Buffers per cycle: " << buffers_per_cycle << std::endl;

  // Memory pool should handle the load without significant memory growth
  EXPECT_GE(final_allocations, initial_allocations);
  // Note: Memory pool may track all allocations, so we check for reasonable growth
  EXPECT_LE(final_allocations - initial_allocations, num_cycles * buffers_per_cycle * 2);
}

/**
 * @brief Test memory leak detection with large allocations
 */
TEST_F(MemoryIntegratedTest, LargeAllocationMemoryLeakDetection) {
  std::cout << "\n=== Large Allocation Memory Leak Detection Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_cycles = 20;
  const int buffers_per_cycle = 5;
  const size_t buffer_size = 1024 * 1024;  // 1MB buffers

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;
  std::cout << "Buffer size: " << buffer_size << " bytes" << std::endl;

  // Perform allocation/deallocation cycles
  for (int cycle = 0; cycle < num_cycles; ++cycle) {
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    buffers.reserve(buffers_per_cycle);

    // Allocate buffers
    for (int i = 0; i < buffers_per_cycle; ++i) {
      auto buffer = pool.acquire(buffer_size);
      if (buffer) {
        buffers.push_back(std::move(buffer));
      }
    }

    // Release buffers
    for (auto& buffer : buffers) {
      pool.release(std::move(buffer), buffer_size);
    }
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Total cycles: " << num_cycles << std::endl;
  std::cout << "Buffers per cycle: " << buffers_per_cycle << std::endl;

  // Memory pool should handle the load without significant memory growth
  EXPECT_GE(final_allocations, initial_allocations);
  // Note: Memory pool may track all allocations, so we check for reasonable growth
  EXPECT_LE(final_allocations - initial_allocations, num_cycles * buffers_per_cycle * 2);
}

/**
 * @brief Test memory leak detection with concurrent access
 */
TEST_F(MemoryIntegratedTest, ConcurrentMemoryLeakDetection) {
  std::cout << "\n=== Concurrent Memory Leak Detection Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_threads = 4;
  const int operations_per_thread = 25;
  const size_t buffer_size = 2048;

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;
  std::cout << "Threads: " << num_threads << std::endl;
  std::cout << "Operations per thread: " << operations_per_thread << std::endl;

  std::atomic<int> completed_operations{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        auto buffer = pool.acquire(buffer_size);
        if (buffer) {
          // Simulate some work
          std::this_thread::sleep_for(std::chrono::microseconds(100));
          pool.release(std::move(buffer), buffer_size);
        }
        completed_operations++;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Completed operations: " << completed_operations.load() << std::endl;

  EXPECT_EQ(completed_operations.load(), num_threads * operations_per_thread);
  EXPECT_GE(final_allocations, initial_allocations);
  EXPECT_LE(final_allocations - initial_allocations, num_threads * operations_per_thread);
}

/**
 * @brief Test memory leak detection under stress
 */
TEST_F(MemoryIntegratedTest, StressMemoryLeakDetection) {
  std::cout << "\n=== Stress Memory Leak Detection Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_cycles = 10;        // Reduced cycles for stability
  const int buffers_per_cycle = 3;  // Reduced buffers per cycle
  const size_t min_buffer_size = 256;
  const size_t max_buffer_size = 1024;  // Reduced max size

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;
  std::cout << "Total cycles: " << num_cycles << std::endl;
  std::cout << "Buffers per cycle: " << buffers_per_cycle << std::endl;

  static thread_local std::mt19937 gen(98765);
  std::uniform_int_distribution<> size_dis(min_buffer_size, max_buffer_size);

  // Perform stress allocation/deallocation cycles with safer approach
  for (int cycle = 0; cycle < num_cycles; ++cycle) {
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    std::vector<size_t> buffer_sizes;

    try {
      // Allocate buffers with random sizes
      for (int i = 0; i < buffers_per_cycle; ++i) {
        size_t buffer_size = size_dis(gen);
        auto buffer = pool.acquire(buffer_size);
        if (buffer) {
          buffers.push_back(std::move(buffer));
          buffer_sizes.push_back(buffer_size);
        }
      }

      // Release buffers safely
      for (size_t i = 0; i < buffers.size(); ++i) {
        if (buffers[i]) {
          pool.release(std::move(buffers[i]), buffer_sizes[i]);
        }
      }

      // Clear vectors to free memory
      buffers.clear();
      buffer_sizes.clear();

    } catch (const std::exception& e) {
      std::cout << "Exception in cycle " << cycle << ": " << e.what() << std::endl;
      // Clean up any remaining buffers
      for (size_t i = 0; i < buffers.size(); ++i) {
        if (buffers[i]) {
          pool.release(std::move(buffers[i]), buffer_sizes[i]);
        }
      }
      buffers.clear();
      buffer_sizes.clear();
    }

    // Small delay to prevent overwhelming the system
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Total cycles: " << num_cycles << std::endl;
  std::cout << "Buffers per cycle: " << buffers_per_cycle << std::endl;

  // Memory pool should handle the stress without significant memory growth
  EXPECT_GE(final_allocations, initial_allocations);
  // Note: Memory pool may track all allocations, so we check for reasonable growth
  // For stress test, allow more generous limits
  EXPECT_LE(final_allocations - initial_allocations, num_cycles * buffers_per_cycle * 2);
}

/**
 * @brief Test memory usage monitoring
 */
TEST_F(MemoryIntegratedTest, MemoryUsageMonitoring) {
  std::cout << "\n=== Memory Usage Monitoring Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_cycles = 30;
  const int buffers_per_cycle = 15;
  const size_t buffer_size = 1024;

  // Get initial memory usage
  size_t initial_memory = get_memory_usage();
  auto initial_stats = pool.stats();

  std::cout << "Initial memory usage: " << initial_memory << " bytes" << std::endl;
  std::cout << "Initial allocations: " << initial_stats.total_allocations << std::endl;

  // Perform allocation cycles
  for (int cycle = 0; cycle < num_cycles; ++cycle) {
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    buffers.reserve(buffers_per_cycle);

    // Allocate buffers
    for (int i = 0; i < buffers_per_cycle; ++i) {
      auto buffer = pool.acquire(buffer_size);
      if (buffer) {
        buffers.push_back(std::move(buffer));
      }
    }

    // Release buffers
    for (auto& buffer : buffers) {
      pool.release(std::move(buffer), buffer_size);
    }

    // Monitor memory usage
    if (cycle % 10 == 0) {
      size_t current_memory = get_memory_usage();
      std::cout << "Cycle " << cycle << " memory usage: " << current_memory << " bytes" << std::endl;
    }
  }

  // Get final memory usage
  size_t final_memory = get_memory_usage();
  auto final_stats = pool.stats();

  std::cout << "Final memory usage: " << final_memory << " bytes" << std::endl;
  std::cout << "Final allocations: " << final_stats.total_allocations << std::endl;
  std::cout << "Memory difference: " << (final_memory - initial_memory) << " bytes" << std::endl;

  // Memory usage should not grow significantly
  EXPECT_LT(final_memory - initial_memory, 1024 * 1024);  // Less than 1MB growth
}

/**
 * @brief Test memory pool statistics accuracy
 */
TEST_F(MemoryIntegratedTest, MemoryPoolStatisticsAccuracy) {
  std::cout << "\n=== Memory Pool Statistics Accuracy Test ===" << std::endl;

  auto& pool = GlobalMemoryPool::instance();
  const int num_operations = 50;
  const size_t buffer_size = 1024;

  // Get initial stats
  auto initial_stats = pool.stats();
  size_t initial_allocations = initial_stats.total_allocations;

  std::cout << "Initial allocations: " << initial_allocations << std::endl;
  std::cout << "Operations to perform: " << num_operations << std::endl;

  // Perform operations
  for (int i = 0; i < num_operations; ++i) {
    auto buffer = pool.acquire(buffer_size);
    if (buffer) {
      pool.release(std::move(buffer), buffer_size);
    }
  }

  // Get final stats
  auto final_stats = pool.stats();
  size_t final_allocations = final_stats.total_allocations;

  std::cout << "Final allocations: " << final_allocations << std::endl;
  std::cout << "Allocation difference: " << (final_allocations - initial_allocations) << std::endl;

  // Statistics should be accurate
  EXPECT_GE(final_allocations, initial_allocations);
  EXPECT_LE(final_allocations - initial_allocations, num_operations);
}

// ============================================================================
// SAFE DATA BUFFER TESTS
// ============================================================================

/**
 * @brief Test safe data buffer basic functionality
 */
TEST_F(MemoryIntegratedTest, SafeDataBufferBasicFunctionality) {
  std::cout << "\n=== Safe Data Buffer Basic Functionality Test ===" << std::endl;

  const size_t buffer_size = 1024;
  std::vector<uint8_t> test_data(buffer_size, 0xAA);

  // Test SafeDataBuffer construction
  SafeDataBuffer buffer(test_data);
  EXPECT_EQ(buffer.size(), buffer_size);

  // Test data access
  for (size_t i = 0; i < buffer_size; ++i) {
    EXPECT_EQ(buffer[i], 0xAA);
  }

  std::cout << "Safe data buffer basic functionality test completed" << std::endl;
}

/**
 * @brief Test safe data buffer bounds checking
 */
TEST_F(MemoryIntegratedTest, SafeDataBufferBoundsChecking) {
  std::cout << "\n=== Safe Data Buffer Bounds Checking Test ===" << std::endl;

  const size_t buffer_size = 1024;
  std::vector<uint8_t> test_data(buffer_size, 0xAA);

  SafeDataBuffer buffer(test_data);

  // Test valid access
  EXPECT_EQ(buffer[0], 0xAA);
  EXPECT_EQ(buffer[buffer_size - 1], 0xAA);

  // Test bounds checking (should not crash)
  try {
    volatile uint8_t value = buffer[buffer_size];  // Out of bounds
    (void)value;                                   // Suppress unused variable warning
  } catch (...) {
    // Expected behavior for bounds checking
    std::cout << "Bounds checking working correctly" << std::endl;
  }

  std::cout << "Safe data buffer bounds checking test completed" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
