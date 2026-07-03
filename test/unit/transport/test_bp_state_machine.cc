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
#include <deque>
#include <variant>
#include <vector>

#include "unilink/transport/base/bp_state_machine.hpp"

using namespace unilink::transport::queue_util;
using unilink::base::constants::BackpressureStrategy;
using unilink::interface::Channel;

namespace {

// maybe_flush_for_keep_latest() (reused by decide_enqueue() for BestEffort
// trimming) visits each deque element via std::visit, so elements must
// actually be a std::variant - a single-alternative one is enough here,
// matching how TCP/UDS/Serial transports store tx_ as deque<BufferVariant>
// directly (UDP's deque<TxItem{BufferVariant, destination}> is the one
// exception, handled separately when UDP migrates to this header).
using TestBuffer = std::variant<std::vector<uint8_t>>;

// Isolated fixture: no sockets, no io_context.
struct FakeQueues {
  std::deque<TestBuffer> tx;
  std::deque<TestBuffer> pending;
  std::atomic<size_t> queue_bytes{0};
  std::atomic<size_t> pending_bytes{0};
  std::atomic<bool> backpressure_active{false};
  unilink::diagnostics::RuntimeStatsCounters stats;

  BackpressureFields fields(BackpressureStrategy strategy, size_t bp_high = 100, size_t bp_low = 50,
                            size_t bp_limit = 400) {
    return BackpressureFields{queue_bytes, pending_bytes, backpressure_active, bp_high, bp_low, bp_limit, strategy};
  }
};

}  // namespace

TEST(BpStateMachineTest, ReliableRoutesToPendingOnceBackpressureActive) {
  FakeQueues q;
  q.backpressure_active.store(true);
  auto f = q.fields(BackpressureStrategy::Reliable);
  DropAccounting dropped;

  auto decision = decide_enqueue(f, 10, q.tx, dropped);

  EXPECT_EQ(decision, EnqueueDecision::Pending);
  EXPECT_FALSE(dropped.any());
}

TEST(BpStateMachineTest, ReliableRejectsWhenPendingWouldExceedLimit) {
  FakeQueues q;
  q.backpressure_active.store(true);
  q.queue_bytes.store(300);
  q.pending_bytes.store(90);
  auto f = q.fields(BackpressureStrategy::Reliable);
  DropAccounting dropped;

  // queue_bytes(300) + pending_bytes(90) + added(20) = 410 > bp_limit(400)
  auto decision = decide_enqueue(f, 20, q.tx, dropped);

  EXPECT_EQ(decision, EnqueueDecision::Rejected);
}

TEST(BpStateMachineTest, ReliableIsImmediateWhenBackpressureNotActive) {
  FakeQueues q;
  auto f = q.fields(BackpressureStrategy::Reliable);
  DropAccounting dropped;

  auto decision = decide_enqueue(f, 10, q.tx, dropped);

  EXPECT_EQ(decision, EnqueueDecision::Immediate);
  EXPECT_FALSE(dropped.any());
}

TEST(BpStateMachineTest, BestEffortTrimsOldestEntriesToFitNewBuffer) {
  FakeQueues q;
  q.tx.push_back(std::vector<uint8_t>(40, 0));
  q.tx.push_back(std::vector<uint8_t>(40, 0));
  q.queue_bytes.store(80);
  auto f = q.fields(BackpressureStrategy::BestEffort);  // bp_high = 100
  DropAccounting dropped;

  // queue_bytes(80) + added(30) = 110 > bp_high(100): must trim.
  auto decision = decide_enqueue(f, 30, q.tx, dropped);

  EXPECT_EQ(decision, EnqueueDecision::Immediate);
  EXPECT_TRUE(dropped.any());
  EXPECT_EQ(dropped.messages, 1u);  // dropping the oldest 40-byte entry brings us to 40+30=70 <= 100
  EXPECT_EQ(q.tx.size(), 1u);
}

TEST(BpStateMachineTest, BestEffortDropsEverythingWhenNewBufferAloneExceedsHigh) {
  FakeQueues q;
  q.tx.push_back(std::vector<uint8_t>(10, 0));
  q.queue_bytes.store(10);
  auto f = q.fields(BackpressureStrategy::BestEffort);  // bp_high = 100
  DropAccounting dropped;

  auto decision = decide_enqueue(f, 150, q.tx, dropped);  // added alone >= bp_high

  EXPECT_EQ(decision, EnqueueDecision::Immediate);
  EXPECT_EQ(dropped.messages, 1u);
  EXPECT_TRUE(q.tx.empty());
  EXPECT_EQ(q.queue_bytes.load(), 0u);
}

TEST(BpStateMachineTest, ReportBackpressureFiresOnWhenCrossingHighWatermark) {
  FakeQueues q;
  auto f = q.fields(BackpressureStrategy::Reliable);
  Channel::OnBackpressure on_bp;
  int fired_with = -1;
  on_bp = [&](size_t queued) { fired_with = static_cast<int>(queued); };
  bool kicked = false;

  report_backpressure(f, /*queued_bytes=*/120, on_bp, q.stats, [&]() -> size_t { return 0; }, [&]() { kicked = true; });

  EXPECT_TRUE(q.backpressure_active.load());
  EXPECT_EQ(fired_with, 120);
  EXPECT_FALSE(kicked);  // ON transition never kicks a write
}

TEST(BpStateMachineTest, ReportBackpressureDoesNothingBetweenLowAndHigh) {
  FakeQueues q;
  auto f = q.fields(BackpressureStrategy::Reliable);
  Channel::OnBackpressure on_bp;
  bool fired = false;
  on_bp = [&](size_t) { fired = true; };

  report_backpressure(f, /*queued_bytes=*/75, on_bp, q.stats, [&]() -> size_t { return 0; }, [&]() {});

  EXPECT_FALSE(q.backpressure_active.load());
  EXPECT_FALSE(fired);
}

TEST(BpStateMachineTest, ReportBackpressureFlushesPendingAndKicksWriteOnOffTransition) {
  FakeQueues q;
  q.backpressure_active.store(true);
  q.queue_bytes.store(40);
  auto f = q.fields(BackpressureStrategy::Reliable);
  Channel::OnBackpressure on_bp;
  std::vector<int> fired;
  on_bp = [&](size_t queued) { fired.push_back(static_cast<int>(queued)); };
  bool kicked = false;
  bool flushed = false;

  report_backpressure(
      f, /*queued_bytes=*/40, on_bp, q.stats,
      [&]() -> size_t {
        flushed = true;
        return 15;  // pretend 15 bytes moved from pending_ into tx_
      },
      [&]() { kicked = true; });

  EXPECT_TRUE(flushed);
  EXPECT_FALSE(q.backpressure_active.load());  // stays off: 40 + 15 = 55 < bp_high(100)
  EXPECT_EQ(q.queue_bytes.load(), 55u);        // fetch_add applied the flushed bytes onto the existing 40
  ASSERT_EQ(fired.size(), 1u);
  EXPECT_EQ(fired[0], 40);  // OFF fired with the pre-flush queued_bytes, not the post-flush total
  EXPECT_TRUE(kicked);
}

TEST(BpStateMachineTest, ReportBackpressureReArmsImmediatelyIfPostFlushStillHigh) {
  FakeQueues q;
  q.backpressure_active.store(true);
  q.queue_bytes.store(40);
  auto f = q.fields(BackpressureStrategy::Reliable);  // bp_high = 100
  Channel::OnBackpressure on_bp;
  std::vector<int> fired;
  on_bp = [&](size_t queued) { fired.push_back(static_cast<int>(queued)); };

  report_backpressure(
      f, /*queued_bytes=*/40, on_bp, q.stats, [&]() -> size_t { return 90; },  // 40 + 90 = 130 >= bp_high(100)
      [&]() {});

  EXPECT_TRUE(q.backpressure_active.load());  // re-armed
  ASSERT_EQ(fired.size(), 2u);
  EXPECT_EQ(fired[0], 40);   // OFF with pre-flush size
  EXPECT_EQ(fired[1], 130);  // immediately re-fired ON with the post-flush size
}

TEST(BpStateMachineTest, DrainClearsQueuesAndFiresOnceIfBackpressureWasActive) {
  FakeQueues q;
  q.backpressure_active.store(true);
  auto f = q.fields(BackpressureStrategy::Reliable);
  Channel::OnBackpressure on_bp;
  int fire_count = 0;
  int last_value = -1;
  on_bp = [&](size_t queued) {
    ++fire_count;
    last_value = static_cast<int>(queued);
  };
  bool cleared = false;

  drain_and_clear_backpressure(f, on_bp, [&]() { cleared = true; });

  EXPECT_TRUE(cleared);
  EXPECT_FALSE(q.backpressure_active.load());
  EXPECT_EQ(fire_count, 1);
  EXPECT_EQ(last_value, 0);
}

TEST(BpStateMachineTest, DrainIsNoOpIfBackpressureWasNotActive) {
  FakeQueues q;
  auto f = q.fields(BackpressureStrategy::Reliable);
  Channel::OnBackpressure on_bp;
  bool fired = false;
  on_bp = [&](size_t) { fired = true; };
  bool cleared = false;

  drain_and_clear_backpressure(f, on_bp, [&]() { cleared = true; });

  EXPECT_TRUE(cleared);  // clear_queues always runs regardless of whether bp was active
  EXPECT_FALSE(fired);   // but on_bp only fires if there was something to clear
}
