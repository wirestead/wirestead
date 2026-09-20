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

// The gate D-1 relies on: admission, the running count and the closed flag as
// one decision, and a generation that keeps one run's callbacks out of the
// next. The wrapper's use of it is covered separately, in
// test/integration/wrapper/test_tcp_stop_admission.cc.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead::wrapper::detail;
using namespace std::chrono_literals;

namespace {

TEST(CallbackGateTest, AdmitsForTheCurrentGenerationOnly) {
  CallbackGate gate;
  const uint64_t first = gate.open_new_generation();
  EXPECT_TRUE(gate.enter(first).admitted());

  const uint64_t second = gate.open_new_generation();
  EXPECT_NE(first, second);
  EXPECT_FALSE(gate.enter(first).admitted()) << "a callback of the previous run was admitted";
  EXPECT_TRUE(gate.enter(second).admitted());
}

TEST(CallbackGateTest, RefusesAfterClose) {
  CallbackGate gate;
  const uint64_t generation = gate.open_new_generation();
  EXPECT_TRUE(gate.enter(generation).admitted());

  gate.close();
  EXPECT_FALSE(gate.enter(generation).admitted());

  // A new run admits again, under its own generation.
  const uint64_t next = gate.open_new_generation();
  EXPECT_TRUE(gate.enter(next).admitted());
}

TEST(CallbackGateTest, WaitUntilIdleReturnsOnlyWhenTheLastLeaseIsReleased) {
  CallbackGate gate;
  const uint64_t generation = gate.open_new_generation();

  auto lease = gate.enter(generation);
  ASSERT_TRUE(lease.admitted());

  std::atomic<bool> waited{false};
  std::thread waiter([&] {
    gate.close();
    gate.wait_until_idle();
    waited = true;
  });

  std::this_thread::sleep_for(200ms);
  EXPECT_FALSE(waited.load()) << "wait_until_idle() returned while a lease was held";

  { auto released = std::move(lease); }  // release it here
  waiter.join();
  EXPECT_TRUE(waited.load());
}

TEST(CallbackGateTest, ReportsWhetherThisThreadHoldsALease) {
  CallbackGate gate;
  const uint64_t generation = gate.open_new_generation();
  EXPECT_FALSE(gate.active_on_this_thread());

  {
    auto lease = gate.enter(generation);
    ASSERT_TRUE(lease.admitted());
    EXPECT_TRUE(gate.active_on_this_thread());

    std::thread other([&] { EXPECT_FALSE(gate.active_on_this_thread()); });
    other.join();
  }

  EXPECT_FALSE(gate.active_on_this_thread());
}

}  // namespace
