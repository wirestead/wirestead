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

#include <stdexcept>
#include <utility>
#include <vector>

#include "wirestead/memory/memory_pool.hpp"

using namespace wirestead::memory;

class PooledBufferTest : public ::testing::Test {};

TEST_F(PooledBufferTest, ConstructionAndValidity) {
  PooledBuffer buffer(1024);
  EXPECT_TRUE(buffer.valid());
  EXPECT_EQ(buffer.size(), 1024);
  EXPECT_NE(buffer.data(), nullptr);
}

TEST_F(PooledBufferTest, OperatorSquareBrackets) {
  const size_t size = 100;
  PooledBuffer buffer(size);
  ASSERT_TRUE(buffer.valid());

  // Write
  for (size_t i = 0; i < size; ++i) {
    buffer[i] = static_cast<uint8_t>(i);
  }

  // Read
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(buffer[i], static_cast<uint8_t>(i));
  }

  // Const access
  const PooledBuffer& const_buffer = buffer;
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(const_buffer[i], static_cast<uint8_t>(i));
  }
}

TEST_F(PooledBufferTest, AtMethodValidAccess) {
  const size_t size = 100;
  PooledBuffer buffer(size);
  ASSERT_TRUE(buffer.valid());

  // Write via []
  for (size_t i = 0; i < size; ++i) {
    buffer[i] = static_cast<uint8_t>(i);
  }

  // Read via at()
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(buffer.at(i), static_cast<uint8_t>(i));
  }

  // Const access via at()
  const PooledBuffer& const_buffer = buffer;
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(const_buffer.at(i), static_cast<uint8_t>(i));
  }
}

TEST_F(PooledBufferTest, AtMethodOutOfBounds) {
  const size_t size = 100;
  PooledBuffer buffer(size);
  ASSERT_TRUE(buffer.valid());

  EXPECT_THROW(buffer.at(size), std::out_of_range);
  EXPECT_THROW(buffer.at(size + 1), std::out_of_range);
}

TEST_F(PooledBufferTest, MoveAssignmentTransfersOwnershipAndHandlesSelfMove) {
  PooledBuffer source(MemoryPool::BufferSize::SMALL);
  PooledBuffer target(MemoryPool::BufferSize::MEDIUM);
  ASSERT_TRUE(source.valid());
  ASSERT_TRUE(target.valid());

  source[0] = 0x5A;
  target = std::move(source);

  EXPECT_TRUE(target.valid());
  EXPECT_EQ(target.size(), static_cast<size_t>(MemoryPool::BufferSize::SMALL));
  EXPECT_EQ(target[0], 0x5A);
  EXPECT_FALSE(source.valid());
  EXPECT_EQ(source.size(), 0u);

  auto* same_target = &target;
  target = std::move(*same_target);
  EXPECT_TRUE(target.valid());
  EXPECT_EQ(target.size(), static_cast<size_t>(MemoryPool::BufferSize::SMALL));
  EXPECT_EQ(target[0], 0x5A);
}
