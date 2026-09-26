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

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include "wirestead/wrapper/receive_budget.hpp"
using wirestead::wrapper::ReceiveLimits;
using wirestead::wrapper::detail::ReceiveBudget;
namespace {
std::shared_ptr<ReceiveBudget> budget(size_t bytes = 100, size_t sessions = 4) {
  return std::make_shared<ReceiveBudget>(ReceiveLimits{bytes, bytes, sessions});
}
TEST(ReceiveBudgetTest, RejectsInvalidLimitsBeforeUse) {
  EXPECT_THROW(ReceiveBudget(ReceiveLimits{0, 1, 1}), std::invalid_argument);
  EXPECT_THROW(ReceiveBudget(ReceiveLimits{1, 0, 1}), std::invalid_argument);
  EXPECT_THROW(ReceiveBudget(ReceiveLimits{1, 2, 1}), std::invalid_argument);
  EXPECT_THROW(ReceiveBudget(ReceiveLimits{1, 1, 0}), std::invalid_argument);
}
TEST(ReceiveBudgetTest, AggregateAdmissionPreservesOtherSessions) {
  auto b = budget();
  auto first = b->open_scope();
  auto second = b->open_scope();
  auto a = first->reserve(70);
  auto c = second->reserve(30);
  ASSERT_TRUE(a);
  ASSERT_TRUE(c);
  EXPECT_FALSE(first->reserve(1));
  EXPECT_EQ(b->stats().reserved_bytes, 100u);
  EXPECT_EQ(second->stats().reserved_bytes, 30u);
  a.reset();
  EXPECT_TRUE(first->reserve(70));
  EXPECT_EQ(second->stats().reserved_bytes, 30u);
  EXPECT_EQ(b->stats().peak_reserved_bytes, 100u);
}
TEST(ReceiveBudgetTest, RetiredScopeStaysChargedUntilLastStorageRelease) {
  auto b = budget(100, 1);
  auto scope = b->open_scope();
  auto charge = scope->reserve(80);
  scope.reset();
  EXPECT_EQ(b->stats().sessions, 1u);
  EXPECT_EQ(b->stats().reserved_bytes, 80u);
  EXPECT_FALSE(b->open_scope());
  charge.reset();
  EXPECT_EQ(b->stats().sessions, 0u);
  EXPECT_EQ(b->stats().reserved_bytes, 0u);
  EXPECT_TRUE(b->open_scope());
}
TEST(ReceiveBudgetTest, ShrinkAndMergePreserveAccounting) {
  auto b = budget();
  auto scope = b->open_scope();
  auto a = scope->reserve(40);
  auto c = scope->reserve(30);
  a->merge(*c);
  EXPECT_EQ(c->size(), 0u);
  c.reset();
  EXPECT_EQ(b->stats().reserved_bytes, 70u);
  a->shrink(15);
  EXPECT_EQ(scope->stats().reserved_bytes, 15u);
  EXPECT_THROW(a->shrink(16), std::logic_error);
  a->merge(*a);
  EXPECT_EQ(a->size(), 15u);
  a.reset();
  EXPECT_EQ(b->stats().reserved_bytes, 0u);
}
TEST(ReceiveBudgetTest, DifferentScopesCannotMerge) {
  auto b = budget();
  auto a = b->open_scope()->reserve(20);
  auto c = b->open_scope()->reserve(30);
  EXPECT_THROW(a->merge(*c), std::logic_error);
  EXPECT_EQ(a->size(), 20u);
  EXPECT_EQ(c->size(), 30u);
  EXPECT_EQ(b->stats().reserved_bytes, 50u);
}
TEST(ReceiveBudgetTest, RejectsSizeOverflowWithoutChangingReservation) {
  const auto max = std::numeric_limits<size_t>::max();
  auto b = budget(max);
  auto scope = b->open_scope();
  auto a = scope->reserve(max - 1);
  ASSERT_TRUE(a);
  EXPECT_FALSE(scope->reserve(2));
  auto c = scope->reserve(1);
  ASSERT_TRUE(c);
  EXPECT_EQ(b->stats().reserved_bytes, max);
  a.reset();
  c.reset();
  EXPECT_EQ(b->stats().reserved_bytes, 0u);
}
TEST(ReceiveBudgetTest, ResetCountersKeepsOutstandingCharges) {
  auto b = budget();
  auto scope = b->open_scope();
  auto a = scope->reserve(80);
  a->shrink(20);
  scope->overflow(15);
  EXPECT_EQ(b->stats().overflow_events, 1u);
  EXPECT_EQ(scope->stats().overflow_bytes, 15u);
  b->reset_stats();
  EXPECT_EQ(b->stats().reserved_bytes, 20u);
  EXPECT_EQ(b->stats().peak_reserved_bytes, 20u);
  EXPECT_EQ(b->stats().overflow_events, 0u);
  EXPECT_EQ(b->stats().overflow_bytes, 0u);
  a.reset();
  EXPECT_EQ(b->stats().reserved_bytes, 0u);
}
TEST(ReceiveBudgetTest, ConcurrentScopesNeverExceedAggregateCapacity) {
  auto b = budget(103, 8);
  std::atomic<bool> invalid{false};
  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&] {
      auto scope = b->open_scope();
      if (!scope) {
        invalid = true;
        return;
      }
      for (int j = 0; j < 2000; ++j) {
        auto charge = scope->reserve(17);
        if (b->stats().reserved_bytes > 103) invalid = true;
        if (charge) std::this_thread::yield();
      }
    });
  }
  for (auto& worker : workers) worker.join();
  EXPECT_FALSE(invalid);
  EXPECT_EQ(b->stats().reserved_bytes, 0u);
  EXPECT_EQ(b->stats().sessions, 0u);
  EXPECT_LE(b->stats().peak_reserved_bytes, 103u);
}
}  // namespace
