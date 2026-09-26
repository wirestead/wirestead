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
#include <memory>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wrapper_contract_test_utils.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace wirestead::test::wrapper_support;
using namespace std::chrono_literals;

// wrapper::TcpClient's tuning setters have two halves: they store the value
// for the next channel build, and - when a channel already exists - they push
// it into the live transport. Every test so far called them before start(),
// so impl_->channel_ was null and the second half never ran. That is the half
// that matters and the half that has broken before: #432 was a live setter
// whose value reached the config but not the running client.

TEST(LiveSetterForwardingTest, IdleTimeoutSetOnALiveClientClosesTheStaleLink) {
  TcpServerLoopbackHarness harness;
  auto server = harness.start_server();
  auto client = harness.connect_client();
  ASSERT_TRUE(client->connected());

  // Neither was configured before start(), so the transport opened with the
  // idle watchdog off. Turning it on now only works if the wrapper forwards
  // to the transport rather than only recording it for a future build.
  client->idle_timeout_action(IdleTimeoutAction::Close);
  client->idle_timeout(200ms);

  // The transport arms the idle timer from the activity path, so one write is
  // what starts the clock. The harness server does not echo, so the link then
  // goes quiet and stays quiet.
  ASSERT_TRUE(client->send("wake"));

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return !client->connected(); }, 5000))
      << "the live idle timeout never fired, so the setter did not reach the transport";

  // Close, not Reconnect: it must stay down rather than come back.
  std::this_thread::sleep_for(500ms);
  EXPECT_FALSE(client->connected());
}

TEST(LiveSetterForwardingTest, TuningSettersAreAcceptedWhileConnected) {
  TcpServerLoopbackHarness harness;
  auto server = harness.start_server();
  auto client = harness.connect_client();
  ASSERT_TRUE(client->connected());

  // Each of these takes the wrapper's unique_lock and then calls into the
  // transport while its io thread is live, so a lock-order mistake here shows
  // up as this test hanging rather than failing.
  client->retry_interval(300ms);
  client->max_retries(7);
  client->connection_timeout(2500ms);
  EXPECT_THROW(client->read_buffer_size(8192), std::logic_error);

  EXPECT_TRUE(client->connected());
  ASSERT_TRUE(client->send("still here"));
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return client->stats().messages_accepted > 0; }, 2000));
}
