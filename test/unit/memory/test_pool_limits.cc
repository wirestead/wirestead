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

#include <gtest/gtest.h>

#include <vector>

#include "wirestead/memory/memory_pool.hpp"

using namespace wirestead::memory;

namespace {

class MemoryPoolLimitsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // max_pool_size = 4 implies capacity = 1 for each of the 4 buckets
    pool_ = std::make_unique<MemoryPool>(1, 4);
  }

  std::unique_ptr<MemoryPool> pool_;
};

TEST_F(MemoryPoolLimitsTest, ReuseLogic) {
  // 1. Allocate one item
  auto buf1 = pool_->acquire(MemoryPool::BufferSize::SMALL);
  uint8_t* addr1 = buf1.get();
  EXPECT_NE(addr1, nullptr);

  // 2. Release it (should go back to pool)
  pool_->release(std::move(buf1), static_cast<size_t>(MemoryPool::BufferSize::SMALL));

  // 3. Allocate again
  auto buf2 = pool_->acquire(MemoryPool::BufferSize::SMALL);
  uint8_t* addr2 = buf2.get();

  // 4. Verify address reuse
  EXPECT_EQ(addr1, addr2) << "Memory address should be reused from the pool";
}

TEST_F(MemoryPoolLimitsTest, ExpansionAndOverflow) {
  // Capacity is 1 per bucket.

  // 1. Allocate first item (from heap, or pool if pre-filled?)
  // Implementation doesn't pre-fill.
  auto buf1 = pool_->acquire(MemoryPool::BufferSize::SMALL);

  // 2. Allocate second item (should trigger expansion/new allocation)
  auto buf2 = pool_->acquire(MemoryPool::BufferSize::SMALL);

  EXPECT_NE(buf1.get(), nullptr);
  EXPECT_NE(buf2.get(), nullptr);
  EXPECT_NE(buf1.get(), buf2.get());

  // 3. Release both.
  // First release should fit in pool.
  // Second release should overflow and be deleted.
  uint8_t* addr1 = buf1.get();
  // uint8_t* addr2 = buf2.get();

  pool_->release(std::move(buf1), static_cast<size_t>(MemoryPool::BufferSize::SMALL));
  // Pool now has 1 item (addr1).

  pool_->release(std::move(buf2), static_cast<size_t>(MemoryPool::BufferSize::SMALL));
  // Pool still has 1 item (addr1). buf2 was dropped.

  // 4. Acquire again. Should get addr1.
  auto buf3 = pool_->acquire(MemoryPool::BufferSize::SMALL);
  EXPECT_EQ(buf3.get(), addr1) << "Should get the pooled item";

  // 5. Acquire again. Should get new address (not addr2, unless allocator
  // reused it immediately, which is possible but unlikely to be same object
  // logic) Actually system allocator might reuse addr2, so strict equality
  // check for != addr2 might be flaky. But we can check stats if available.

  MemoryPool::PoolStats stats = pool_->stats();
  EXPECT_GT(stats.total_allocations, 0);
}

TEST_F(MemoryPoolLimitsTest, ValidatesSize) {
  EXPECT_THROW(pool_->acquire(0), std::invalid_argument);
  EXPECT_THROW(pool_->acquire(100 * 1024 * 1024),
               std::invalid_argument);  // > 64MB
}

TEST_F(MemoryPoolLimitsTest, LargeAllocation) {
  // Allocate a buffer larger than the largest bucket (64KB)
  size_t large_size = 100000;
  auto buf = pool_->acquire(large_size);
  EXPECT_NE(buf.get(), nullptr);

  // Write to the end to ensure we own the memory
  // If the buffer was truncated to 64KB, this write is out of bounds
  // and might cause a crash or be detected by tools.
  // Note: Depending on page alignment, this might not crash immediately
  // if the next page is mapped, but it's invalid access.
  buf[large_size - 1] = 0xAA;
  EXPECT_EQ(buf[large_size - 1], 0xAA);

  // Release it
  pool_->release(std::move(buf), large_size);
}

// #443: PooledBuffer(size, pool) must draw from and release back to the
// specific pool instance passed in, not GlobalMemoryPool::instance() -
// this is what makes per-channel pools (one MemoryPool member per
// transport instance) actually isolated from each other.
TEST(PooledBufferPerPoolTest, DrawsFromAndReleasesToTheGivenPoolNotGlobal) {
  MemoryPool pool_a(4, 16);
  MemoryPool pool_b(4, 16);

  {
    PooledBuffer buf(MemoryPool::BufferSize::SMALL, pool_a);
    EXPECT_TRUE(buf.valid());
  }
  // The buffer was acquired from and released back to pool_a - pool_a should
  // show a recorded allocation, pool_b must remain completely untouched.
  EXPECT_EQ(pool_a.stats().total_allocations, 1U);
  EXPECT_EQ(pool_b.stats().total_allocations, 0U);

  {
    PooledBuffer buf(MemoryPool::BufferSize::SMALL, pool_a);
    EXPECT_TRUE(buf.valid());
  }
  // Second acquire should hit the bucket populated by the first release.
  EXPECT_EQ(pool_a.stats().pool_hits, 1U);
  EXPECT_EQ(pool_b.stats().total_allocations, 0U);
  EXPECT_EQ(pool_b.stats().pool_hits, 0U);
}

}  // namespace
