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

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "wirestead/memory/memory_pool.hpp"

using namespace wirestead::memory;

class MemoryPoolStressTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Basic setup if needed
  }

  void TearDown() override {
    // Basic teardown if needed
  }
};

TEST_F(MemoryPoolStressTest, ExhaustionAndRecovery) {
  // Create a pool with small capacity
  // 4 buckets, if max_pool_size is 20, capacity per bucket is 5.
  MemoryPool pool(5, 20);

  std::vector<std::unique_ptr<uint8_t[]>> allocations;
  size_t alloc_size = 1024;  // Should fall into SMALL bucket (size 1024)

  // Allocate 6 items (capacity is 5)
  for (int i = 0; i < 6; ++i) {
    auto ptr = pool.acquire(alloc_size);
    EXPECT_NE(ptr, nullptr);
    allocations.push_back(std::move(ptr));
  }

  auto stats = pool.stats();
  EXPECT_EQ(stats.total_allocations, 6);
  // Hits might be 0 because pool starts empty and fills on release?
  // No, pool starts with empty vector (reserve only).
  // So all 6 are new allocations (misses in terms of "getting from pool", but counted as total_allocations).
  // Implementation: acquire -> if !empty pop (hit) else create (total++).
  // So all 6 are created. pool_hits should be 0.
  EXPECT_EQ(stats.pool_hits, 0);

  // Release all
  for (auto& ptr : allocations) {
    pool.release(std::move(ptr), alloc_size);
  }
  allocations.clear();

  // Now bucket should have 5 items (capacity limit). 1 was discarded.

  // Allocate 6 again
  for (int i = 0; i < 6; ++i) {
    auto ptr = pool.acquire(alloc_size);
    EXPECT_NE(ptr, nullptr);
    allocations.push_back(std::move(ptr));
  }

  stats = pool.stats();
  // Total allocations: 6 (initial) + 6 (second round) = 12?
  // Wait, acquire_from_bucket:
  // if from stack: hits++, total++
  // if create: total++
  // So total should be 12.
  // First 5 should be hits.
  // 6th should be create (miss).
  EXPECT_EQ(stats.total_allocations, 12);
  EXPECT_EQ(stats.pool_hits, 5);
}

TEST_F(MemoryPoolStressTest, ReuseAddress) {
  MemoryPool pool(5, 20);
  size_t size = 1024;

  auto ptr1 = pool.acquire(size);
  void* addr1 = ptr1.get();
  pool.release(std::move(ptr1), size);

  auto ptr2 = pool.acquire(size);
  void* addr2 = ptr2.get();

  // Should reuse the same address
  EXPECT_EQ(addr1, addr2);
}

TEST_F(MemoryPoolStressTest, PooledBufferLifecycle) {
  MemoryPool pool(5, 20);

  {
    PooledBuffer buf(MemoryPool::BufferSize::SMALL);
    EXPECT_TRUE(buf.valid());
    EXPECT_EQ(buf.size(), 1024);
    // Writes should work
    buf[0] = 0xAA;
    EXPECT_EQ(buf[0], 0xAA);
  }
  // Released automatically

  // Verify stats via global pool?
  // PooledBuffer uses GlobalMemoryPool::instance().
  // We cannot easily isolate tests on GlobalMemoryPool if other tests run in parallel or sequence.
  // But we can check if it works without crashing.
}

TEST_F(MemoryPoolStressTest, PooledBufferMove) {
  PooledBuffer buf1(MemoryPool::BufferSize::SMALL);
  buf1[0] = 0xBB;

  PooledBuffer buf2 = std::move(buf1);
  EXPECT_FALSE(buf1.valid());
  EXPECT_TRUE(buf2.valid());
  EXPECT_EQ(buf2[0], 0xBB);
}

TEST_F(MemoryPoolStressTest, ZeroSizeAllocation) {
  MemoryPool pool;
  EXPECT_THROW(pool.acquire(0), std::invalid_argument);
}
