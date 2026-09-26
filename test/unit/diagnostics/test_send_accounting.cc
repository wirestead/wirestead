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

#include <thread>

#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/diagnostics/send_accounting.hpp"

namespace {
using Ledger = wirestead::diagnostics::SendAccountingLedger;
using Cause = Ledger::Cause;

void expect_conservation(const wirestead::wrapper::SendAccounting& s) {
  EXPECT_EQ(s.accepted.requests,
            s.written.requests + s.outstanding.requests + s.explicit_stop.discarded_before_write.requests +
                s.explicit_stop.aborted_during_write.requests + s.connection_loss.discarded_before_write.requests +
                s.connection_loss.aborted_during_write.requests + s.queue_pressure.discarded_before_write.requests +
                s.queue_pressure.aborted_during_write.requests + s.session_expiry.discarded_before_write.requests +
                s.session_expiry.aborted_during_write.requests);
  EXPECT_EQ(s.accepted.bytes,
            s.written.bytes + s.outstanding.bytes + s.explicit_stop.discarded_before_write.bytes +
                s.explicit_stop.aborted_during_write.bytes + s.connection_loss.discarded_before_write.bytes +
                s.connection_loss.aborted_during_write.bytes + s.queue_pressure.discarded_before_write.bytes +
                s.queue_pressure.aborted_during_write.bytes + s.session_expiry.discarded_before_write.bytes +
                s.session_expiry.aborted_during_write.bytes);
}

TEST(SendAccountingTest, FailedSubmissionRollsBackBeforeReturningAcceptance) {
  Ledger ledger;
  {
    Ledger::Admission pending(ledger, 11);
    EXPECT_EQ(ledger.snapshot().outstanding.requests, 1u);
  }
  EXPECT_EQ(ledger.snapshot().accepted.requests, 0u);
  EXPECT_EQ(ledger.snapshot().outstanding.requests, 0u);
  {
    Ledger::Admission submitted(ledger, 13);
    submitted.commit();
  }
  ledger.end(Cause::ExplicitStop);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.accepted.requests, 1u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.bytes, 13u);
  expect_conservation(s);
}

TEST(SendAccountingTest, UnsupportedStatsAreNotReportedAsZeroLoss) {
  wirestead::diagnostics::RuntimeStatsCounters legacy;
  EXPECT_FALSE(legacy.snapshot(0, 0, false).send_accounting.has_value());
}
TEST(SendAccountingTest, QueuedAndActiveLossRemainDistinctAndFirstCauseWins) {
  Ledger ledger;
  const auto queued = ledger.admit(11), active = ledger.admit(23);
  ASSERT_TRUE(ledger.begin(active));
  ledger.end(Cause::ConnectionLoss);
  ledger.end(Cause::ExplicitStop);
  ledger.complete(active, 23);
  ledger.discard(queued, Cause::QueuePressure);
  EXPECT_FALSE(ledger.begin(queued));
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.connection_loss.discarded_before_write.requests, 1u);
  EXPECT_EQ(s.connection_loss.discarded_before_write.bytes, 11u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 23u);
  EXPECT_EQ(s.explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(s.confirmed_written_bytes, 0u);
  expect_conservation(s);
}
TEST(SendAccountingTest, PartialGatherPrefixCountsLogicalRequestsAndConfirmedBytes) {
  Ledger ledger;
  const auto a = ledger.admit(4), b = ledger.admit(7), c = ledger.admit(9);
  ledger.begin(a);
  ledger.begin(b);
  ledger.begin(c);
  ledger.complete(a, 4);
  ledger.complete(b, 2);
  ledger.complete(c, 0);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.written.requests, 1u);
  EXPECT_EQ(s.written.bytes, 4u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.requests, 2u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 16u);
  EXPECT_EQ(s.confirmed_written_bytes, 6u);
  expect_conservation(s);
}
TEST(SendAccountingTest, ResetExcludesOldRequestsWithoutReusingIds) {
  Ledger ledger;
  const auto active = ledger.admit(10), pending = ledger.admit(20);
  ledger.begin(active);
  ledger.reset();
  const auto fresh = ledger.admit(30);
  EXPECT_NE(active, fresh);
  EXPECT_NE(pending, fresh);
  ledger.complete(active, 10);
  ledger.begin(pending);
  ledger.complete(pending, 20);
  ledger.begin(fresh);
  ledger.complete(fresh, 30);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.accepted.requests, 1u);
  EXPECT_EQ(s.written.bytes, 30u);
  EXPECT_EQ(s.confirmed_written_bytes, 30u);
  expect_conservation(s);
}
TEST(SendAccountingTest, OldCompletionCannotTerminateReplacementConnectionRequest) {
  Ledger ledger;
  const auto old = ledger.admit(5);
  ledger.begin(old);
  ledger.end(Cause::ConnectionLoss);
  const auto fresh = ledger.admit(7);
  ledger.begin(fresh);
  ledger.complete(old, 5);
  EXPECT_EQ(ledger.snapshot().outstanding.requests, 1u);
  ledger.complete(fresh, 7);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 5u);
  EXPECT_EQ(s.written.bytes, 7u);
  expect_conservation(s);
}

TEST(SendAccountingTest, PressureRemovesOnlyTheSelectedRequest) {
  Ledger ledger;
  const auto a = ledger.admit(7), b = ledger.admit(7);
  ledger.discard(a, Cause::QueuePressure);
  ledger.begin(b);
  ledger.complete(b, 7);
  ledger.discard(a, Cause::ExplicitStop);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.queue_pressure.discarded_before_write.requests, 1u);
  EXPECT_EQ(s.written.requests, 1u);
  expect_conservation(s);
}
TEST(SendAccountingTest, StopCompletionRaceTerminatesEachRequestExactlyOnce) {
  for (int round = 0; round < 100; ++round) {
    Ledger ledger;
    const auto request = ledger.admit(19);
    ledger.begin(request);
    std::thread complete([&] { ledger.complete(request, 19); });
    std::thread stop([&] { ledger.end(Cause::ExplicitStop); });
    complete.join();
    stop.join();
    const auto s = ledger.snapshot();
    EXPECT_EQ(s.written.requests + s.explicit_stop.aborted_during_write.requests, 1u);
    EXPECT_EQ(s.outstanding.requests, 0u);
    expect_conservation(s);
  }
}

TEST(SendAccountingTest, GroupsSeparatePeersAndUnattributedRequests) {
  Ledger ledger;
  auto first = std::make_shared<Ledger::Group>();
  auto second = std::make_shared<Ledger::Group>();
  const auto a = ledger.admit(11, first), b = ledger.admit(17, second), raw = ledger.admit(23);
  ledger.begin(a);
  ledger.complete(a, 11);
  ledger.discard(b, Cause::QueuePressure);
  ledger.begin(raw);
  ledger.complete(raw, 23);
  EXPECT_EQ(ledger.snapshot(first).written.bytes, 11u);
  EXPECT_EQ(ledger.snapshot(second).queue_pressure.discarded_before_write.bytes, 17u);
  EXPECT_EQ(ledger.snapshot().accepted.bytes, 51u);
  EXPECT_EQ(ledger.snapshot().written.bytes, 34u);
  expect_conservation(ledger.snapshot(first));
  expect_conservation(ledger.snapshot(second));
  expect_conservation(ledger.snapshot());
}

TEST(SendAccountingTest, GroupResetExcludesCompletedAndOutstandingOldEpoch) {
  Ledger ledger;
  auto group = std::make_shared<Ledger::Group>();
  const auto completed = ledger.admit(5, group), active = ledger.admit(7, group);
  ledger.begin(completed);
  ledger.complete(completed, 5);
  ledger.begin(active);
  ledger.reset();
  EXPECT_EQ(ledger.snapshot(group).accepted.requests, 0u);
  const auto fresh = ledger.admit(11, group);
  ledger.complete(active, 7);
  ledger.begin(fresh);
  ledger.complete(fresh, 11);
  EXPECT_EQ(ledger.snapshot(group).accepted.requests, 1u);
  EXPECT_EQ(ledger.snapshot(group).confirmed_written_bytes, 11u);
  expect_conservation(ledger.snapshot(group));
  expect_conservation(ledger.snapshot());
}

TEST(SendAccountingTest, RetiredGroupCompletionDoesNotUpdateReplacement) {
  Ledger ledger;
  auto retired = std::make_shared<Ledger::Group>();
  const auto old = ledger.admit(7, retired);
  ledger.begin(old);
  std::weak_ptr<Ledger::Group> lifetime = retired;
  retired.reset();
  EXPECT_FALSE(lifetime.expired());
  auto replacement = std::make_shared<Ledger::Group>();
  const auto fresh = ledger.admit(11, replacement);
  ledger.complete(old, 7);
  EXPECT_TRUE(lifetime.expired());
  EXPECT_EQ(ledger.snapshot(replacement).written.requests, 0u);
  ledger.discard(fresh, Cause::ExplicitStop);
  EXPECT_EQ(ledger.snapshot().written.bytes, 7u);
  EXPECT_EQ(ledger.snapshot(replacement).explicit_stop.discarded_before_write.bytes, 11u);
  expect_conservation(ledger.snapshot(replacement));
  expect_conservation(ledger.snapshot());
}

TEST(SendAccountingTest, GroupRollbackAndTerminalRaceMatchAggregate) {
  Ledger ledger;
  auto group = std::make_shared<Ledger::Group>();
  { Ledger::Admission failed(ledger, 13, group); }
  EXPECT_EQ(ledger.snapshot(group).accepted.requests, 0u);
  const auto request = ledger.admit(19, group);
  ledger.begin(request);
  std::thread complete([&] { ledger.complete(request, 19); });
  std::thread stop([&] { ledger.end(Cause::ExplicitStop); });
  complete.join();
  stop.join();
  const auto total = ledger.snapshot(), peer = ledger.snapshot(group);
  EXPECT_EQ(peer.written.requests, total.written.requests);
  EXPECT_EQ(peer.explicit_stop.aborted_during_write.requests, total.explicit_stop.aborted_during_write.requests);
  EXPECT_EQ(peer.written.requests + peer.explicit_stop.aborted_during_write.requests, 1u);
  expect_conservation(peer);
  expect_conservation(total);
}

TEST(SendAccountingTest, ExpiryDiscardsOnlyWaitingRequestsOfSelectedGroup) {
  Ledger ledger;
  auto a = std::make_shared<Ledger::Group>(), b = std::make_shared<Ledger::Group>();
  const auto active = ledger.admit(7, a), queued = ledger.admit(11, a), other = ledger.admit(13, b);
  ledger.begin(active);
  ledger.discard_waiting(a, Cause::SessionExpiry);
  EXPECT_FALSE(ledger.contains(queued));
  EXPECT_TRUE(ledger.contains(active));
  EXPECT_TRUE(ledger.contains(other));
  ledger.complete(active, 7);
  ledger.begin(other);
  ledger.complete(other, 13);
  ledger.end(Cause::ExplicitStop);
  EXPECT_EQ(ledger.snapshot(a).session_expiry.discarded_before_write.bytes, 11u);
  EXPECT_EQ(ledger.snapshot(a).written.bytes, 7u);
  EXPECT_EQ(ledger.snapshot(b).written.bytes, 13u);
  expect_conservation(ledger.snapshot(a));
  expect_conservation(ledger.snapshot(b));
  expect_conservation(ledger.snapshot());
}
TEST(SendAccountingTest, ExpiredActiveWriteRetainsFailureOrStopOutcome) {
  for (bool stop : {false, true}) {
    Ledger ledger;
    auto group = std::make_shared<Ledger::Group>();
    const auto active = ledger.admit(7, group);
    ledger.begin(active);
    ledger.discard_waiting(group, Cause::SessionExpiry);
    if (stop)
      ledger.end(Cause::ExplicitStop);
    else
      ledger.complete(active, 3);
    auto s = ledger.snapshot(group);
    EXPECT_EQ(s.session_expiry.aborted_during_write.requests, 0u);
    EXPECT_EQ(s.explicit_stop.aborted_during_write.requests, stop ? 1u : 0u);
    EXPECT_EQ(s.connection_loss.aborted_during_write.requests, stop ? 0u : 1u);
    expect_conservation(s);
  }
}
TEST(SendAccountingTest, ExpiryAfterResetRemovesOldWaitingWorkWithoutCountingIt) {
  Ledger ledger;
  auto group = std::make_shared<Ledger::Group>();
  const auto old = ledger.admit(7, group);
  ledger.reset();
  ledger.admit(11, group);
  ledger.discard_waiting(group, Cause::SessionExpiry);
  EXPECT_FALSE(ledger.contains(old));
  EXPECT_EQ(ledger.snapshot(group).session_expiry.discarded_before_write.bytes, 11u);
  expect_conservation(ledger.snapshot(group));
  expect_conservation(ledger.snapshot());
}
}  // namespace
