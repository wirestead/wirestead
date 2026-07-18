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

#include <chrono>

#include "wirestead/memory/memory_pool.hpp"

using namespace wirestead::memory;

namespace wirestead {
namespace test {

TEST(MemoryPoolEdgeCaseTest, LargeAllocationBypass) {
  MemoryPool pool;
  // XLARGE is 64KB. Request 128KB.
  size_t large_size = 128 * 1024;
  auto buf = pool.acquire(large_size);
  ASSERT_NE(buf, nullptr);

  // stats check - hits should be 0, total_allocations should be 1
  auto s = pool.stats();
  EXPECT_EQ(s.pool_hits, 0);
  EXPECT_EQ(s.total_allocations, 1);

  pool.release(std::move(buf), large_size);
}

TEST(MemoryPoolEdgeCaseTest, PoolCapacityLimit) {
  // Initial pool size is dummy, max_pool_size is 4096.
  // buckets_.size() is 4. Each bucket capacity = 4096 / 4 = 1024 bytes.
  // For SMALL bucket (1024 bytes), capacity = 1024 / 1024 = 1 buffer.
  MemoryPool pool(1024, 4096);

  auto buf1 = pool.acquire(MemoryPool::BufferSize::SMALL);
  auto buf2 = pool.acquire(MemoryPool::BufferSize::SMALL);

  // Release both. One should be kept, one should be discarded.
  pool.release(std::move(buf1), static_cast<size_t>(MemoryPool::BufferSize::SMALL));
  pool.release(std::move(buf2), static_cast<size_t>(MemoryPool::BufferSize::SMALL));

  // Re-acquire. Should get at least 1 hit.
  auto buf3 = pool.acquire(MemoryPool::BufferSize::SMALL);
  auto buf4 = pool.acquire(MemoryPool::BufferSize::SMALL);

  auto s = pool.stats();
  EXPECT_GE(s.pool_hits, 1);
}

TEST(MemoryPoolEdgeCaseTest, InvalidBufferSize) {
  MemoryPool pool;
  EXPECT_THROW(pool.acquire(0), std::invalid_argument);
  EXPECT_THROW(pool.acquire(100 * 1024 * 1024), std::invalid_argument);
}

TEST(MemoryPoolEdgeCaseTest, PooledBufferBounds) {
  PooledBuffer buf(10);
  EXPECT_THROW(buf.at(10), std::out_of_range);

  const PooledBuffer& const_buf = buf;
  EXPECT_THROW(const_buf.at(10), std::out_of_range);
}

TEST(MemoryPoolEdgeCaseTest, StatsAndMetrics) {
  MemoryPool pool;
  EXPECT_EQ(pool.hit_rate(), 0.0);

  pool.acquire(MemoryPool::BufferSize::SMALL);
  EXPECT_EQ(pool.hit_rate(), 0.0);  // 0 hits / 1 alloc

  auto usage = pool.memory_usage();
  EXPECT_GT(usage.first, 0);

  auto health = pool.health_metrics();
  EXPECT_EQ(health.hit_rate, 0.0);
}

TEST(MemoryPoolEdgeCaseTest, NullReleaseIsSafe) {
  MemoryPool pool;

  std::unique_ptr<uint8_t[]> empty;
  EXPECT_NO_THROW(pool.release(std::move(empty), static_cast<size_t>(MemoryPool::BufferSize::SMALL)));

  auto before = pool.memory_usage();
  auto health = pool.health_metrics();
  EXPECT_EQ(before.first, before.second);  // nothing acquired yet - both 0
  EXPECT_EQ(health.hit_rate, pool.hit_rate());
}

// #451: memory_usage() must reflect actual current usage - going down when
// buffers are released - not a monotonically-increasing allocation count.
// The peak (second) value must stay at the high-water mark rather than
// following the current value back down.
TEST(MemoryPoolEdgeCaseTest, MemoryUsageReflectsReleases) {
  MemoryPool pool;
  constexpr size_t kSize = static_cast<size_t>(MemoryPool::BufferSize::SMALL);

  auto buf1 = pool.acquire(kSize);
  auto buf2 = pool.acquire(kSize);
  auto usage_after_two = pool.memory_usage();
  EXPECT_EQ(usage_after_two.first, 2 * kSize);
  EXPECT_EQ(usage_after_two.second, 2 * kSize);

  pool.release(std::move(buf1), kSize);
  auto usage_after_release = pool.memory_usage();
  EXPECT_EQ(usage_after_release.first, kSize) << "current usage must decrease after release()";
  EXPECT_EQ(usage_after_release.second, 2 * kSize) << "peak must remain at the prior high-water mark";

  pool.release(std::move(buf2), kSize);
  auto usage_after_both_released = pool.memory_usage();
  EXPECT_EQ(usage_after_both_released.first, 0u);
  EXPECT_EQ(usage_after_both_released.second, 2 * kSize);
}

TEST(MemoryPoolEdgeCaseTest, StandardBucketsReuseAcrossSizes) {
  MemoryPool pool(4, 16);

  auto medium = pool.acquire(MemoryPool::BufferSize::MEDIUM);
  auto large = pool.acquire(MemoryPool::BufferSize::LARGE);
  auto xlarge = pool.acquire(MemoryPool::BufferSize::XLARGE);
  auto* medium_addr = medium.get();
  auto* large_addr = large.get();
  auto* xlarge_addr = xlarge.get();

  pool.release(std::move(medium), static_cast<size_t>(MemoryPool::BufferSize::MEDIUM));
  pool.release(std::move(large), static_cast<size_t>(MemoryPool::BufferSize::LARGE));
  pool.release(std::move(xlarge), static_cast<size_t>(MemoryPool::BufferSize::XLARGE));

  auto medium_again = pool.acquire(MemoryPool::BufferSize::MEDIUM);
  auto large_again = pool.acquire(MemoryPool::BufferSize::LARGE);
  auto xlarge_again = pool.acquire(MemoryPool::BufferSize::XLARGE);

  EXPECT_EQ(medium_again.get(), medium_addr);
  EXPECT_EQ(large_again.get(), large_addr);
  EXPECT_EQ(xlarge_again.get(), xlarge_addr);
  EXPECT_GE(pool.stats().pool_hits, 3u);
}

}  // namespace test
}  // namespace wirestead
