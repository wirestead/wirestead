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

#include <algorithm>
#include <cstdlib>
#include <new>
#include <vector>

#include "wirestead/wrapper/bounded_receive.hpp"

// This file must stay its own executable: it replaces global operator new for
// the whole binary to measure the bytes prepare_receive() allocates.

namespace {
thread_local bool t_counting = false;
thread_local size_t t_bytes = 0;
}  // namespace

void* operator new(std::size_t n) {
  if (t_counting) t_bytes += n;
  void* p = std::malloc(n ? n : 1);
  if (!p) throw std::bad_alloc();
  return p;
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
  if (t_counting) t_bytes += n;
  return std::malloc(n ? n : 1);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept { return ::operator new(n, std::nothrow); }

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }

using namespace wirestead;
using namespace wirestead::wrapper;
using namespace wirestead::wrapper::detail;

// prepare_receive() feeds its staging framer in 4 KiB steps. Growing the
// staging buffer to exactly the bytes needed reallocated and recopied a partial
// frame on every step: a 1 MiB frame arriving in 64 KiB reads allocated about
// 139 MiB. Geometric growth brings that to about 27 MiB; what remains is the
// per-input copy that keeps a rejected input from altering the framer.
TEST(BoundedReceiveGrowthTest, LargeFrameIsNotRecopiedOnEveryStagingStep) {
  auto budget = std::make_shared<ReceiveBudget>(ReceiveLimits{});
  ReceiveState state(budget->open_scope());
  auto f = std::make_shared<framer::LineFramer>("\n", false, 2 * 1024 * 1024);
  size_t frame_size = 0;
  f->on_message([&](memory::ConstByteSpan m) { frame_size = m.size(); });

  constexpr size_t kFrame = 1024 * 1024 - 1;  // fits the default 1 MiB frame limit
  constexpr size_t kRead = 64 * 1024;
  std::vector<uint8_t> stream(kFrame, 'x');
  stream.push_back('\n');

  t_bytes = 0;
  t_counting = true;
  for (size_t off = 0; off < stream.size(); off += kRead) {
    const size_t n = std::min(kRead, stream.size() - off);
    prepare_receive(state, f, 1, memory::ConstByteSpan(stream.data() + off, n), false).deliver();
  }
  t_counting = false;

  EXPECT_EQ(frame_size, kFrame);
  EXPECT_LT(t_bytes, size_t{64} * 1024 * 1024) << "staging allocated " << t_bytes << " bytes for a 1 MiB frame";
}

TEST(BoundedReceiveGrowthTest, GrowthFallsBackToExactSizeWhenBudgetIsTight) {
  ReceiveLimits limits;
  limits.max_bytes = 1024 * 1024;
  limits.max_frame_bytes = 64 * 1024;
  auto budget = std::make_shared<ReceiveBudget>(limits);
  // Another session holds all but 22 KiB. Staging a 12 KiB partial frame grows
  // 4 -> 8 KiB, then needs 12 KiB: doubling to 16 KiB while the 8 KiB buffer is
  // still charged needs 24 KiB, but the exact 12 KiB step needs only 20 KiB.
  auto other = budget->open_scope();
  auto held = other->reserve(limits.max_bytes - 22 * 1024);
  ASSERT_TRUE(held);
  ReceiveState state(budget->open_scope());
  auto f = std::make_shared<framer::LineFramer>("\n", false, 64 * 1024);

  std::vector<uint8_t> partial(12 * 1024, 'x');
  EXPECT_NO_THROW(prepare_receive(state, f, 1, memory::ConstByteSpan(partial), false));
  EXPECT_EQ(ReceiveFramerAccess::size(f.get()), 12u * 1024);
  EXPECT_EQ(ReceiveFramerAccess::capacity(f.get()), 12u * 1024);
}
