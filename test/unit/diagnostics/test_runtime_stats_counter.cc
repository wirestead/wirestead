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

#include "wirestead/diagnostics/runtime_stats_counter.hpp"

namespace {

using wirestead::diagnostics::RuntimeStatsCounters;

TEST(RuntimeStatsCounterTest, DefaultSnapshotIsZero) {
  RuntimeStatsCounters counters;

  const auto stats = counters.snapshot(0, 0, false);

  EXPECT_EQ(stats.messages_accepted, 0u);
  EXPECT_EQ(stats.bytes_accepted, 0u);
  EXPECT_EQ(stats.messages_sent, 0u);
  EXPECT_EQ(stats.bytes_sent, 0u);
  EXPECT_EQ(stats.messages_received, 0u);
  EXPECT_EQ(stats.bytes_received, 0u);
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
  EXPECT_EQ(stats.backpressure_events, 0u);
  EXPECT_EQ(stats.queued_bytes, 0u);
  EXPECT_EQ(stats.pending_bytes, 0u);
  EXPECT_EQ(stats.max_queued_bytes, 0u);
  EXPECT_FALSE(stats.backpressure_active);
}

TEST(RuntimeStatsCounterTest, RecordsCountersAndSnapshotGauges) {
  RuntimeStatsCounters counters;

  counters.record_accepted(11);
  counters.record_accepted(13);
  counters.record_sent(7);
  counters.record_received(5);
  counters.record_failed_send();
  counters.record_dropped(3, 17);
  counters.record_backpressure_event();
  counters.record_backpressure_event();
  counters.observe_queue(12);
  counters.observe_queue(8);

  const auto stats = counters.snapshot(4, 2, true);

  EXPECT_EQ(stats.messages_accepted, 2u);
  EXPECT_EQ(stats.bytes_accepted, 24u);
  EXPECT_EQ(stats.messages_sent, 1u);
  EXPECT_EQ(stats.bytes_sent, 7u);
  EXPECT_EQ(stats.messages_received, 1u);
  EXPECT_EQ(stats.bytes_received, 5u);
  EXPECT_EQ(stats.failed_sends, 1u);
  EXPECT_EQ(stats.dropped_messages, 3u);
  EXPECT_EQ(stats.dropped_bytes, 17u);
  EXPECT_EQ(stats.backpressure_events, 2u);
  EXPECT_EQ(stats.queued_bytes, 4u);
  EXPECT_EQ(stats.pending_bytes, 2u);
  EXPECT_EQ(stats.max_queued_bytes, 12u);
  EXPECT_TRUE(stats.backpressure_active);
}

TEST(RuntimeStatsCounterTest, ResetClearsCumulativeCountersAndSetsMaxQueueBaseline) {
  RuntimeStatsCounters counters;

  counters.record_accepted(11);
  counters.record_sent(7);
  counters.record_received(5);
  counters.record_failed_send();
  counters.record_dropped(2, 19);
  counters.record_backpressure_event();
  counters.observe_queue(40);

  counters.reset(6);
  const auto stats = counters.snapshot(4, 2, true);

  EXPECT_EQ(stats.messages_accepted, 0u);
  EXPECT_EQ(stats.bytes_accepted, 0u);
  EXPECT_EQ(stats.messages_sent, 0u);
  EXPECT_EQ(stats.bytes_sent, 0u);
  EXPECT_EQ(stats.messages_received, 0u);
  EXPECT_EQ(stats.bytes_received, 0u);
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
  EXPECT_EQ(stats.backpressure_events, 0u);
  EXPECT_EQ(stats.queued_bytes, 4u);
  EXPECT_EQ(stats.pending_bytes, 2u);
  EXPECT_EQ(stats.max_queued_bytes, 6u);
  EXPECT_TRUE(stats.backpressure_active);
}

}  // namespace
