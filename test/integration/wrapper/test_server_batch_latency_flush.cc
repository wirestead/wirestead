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
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wrapper_contract_test_utils.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace wirestead::test::wrapper_support;
using namespace std::chrono_literals;

namespace {

// The client-side half of this behaviour lives in
// test/unit/wrapper/test_batch_latency_flush.cc, which can drive a wrapper
// through an injected interface::Channel. The server wrappers cannot be
// reached that way: their receive path hangs off
// transport_server->on_multi_data(), obtained by dynamic_pointer_cast to the
// concrete transport, so a fake channel delivers nothing to them and only a
// real loopback exercises the queue.
//
// What is under test is the same either way - one message, a batch size it
// cannot reach, and a latency it must wait out, so the only thing that can
// deliver the partial batch is the timer that calls flush_batches().
template <typename Harness>
void expect_server_partial_batch_flushes_after_latency() {
  Harness harness;
  auto server = harness.start_server();

  std::mutex mutex;
  std::vector<std::string> data_payloads;
  std::vector<std::string> message_payloads;
  std::atomic<int> data_batches{0};
  std::atomic<int> message_batches{0};

  server->batch_size(10).batch_latency(50ms);
  // Set before the client connects: the per-client framer is built from this
  // factory when the connection arrives.
  server->framer([]() { return std::make_unique<framer::LineFramer>(); });
  server->on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& ctx : batch) data_payloads.push_back(ctx.data_as_string());
    data_batches++;
  });
  server->on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& ctx : batch) message_payloads.push_back(ctx.data_as_string());
    message_batches++;
  });

  std::shared_ptr<void> keep_client_alive;
  if constexpr (requires { harness.connect_client(); }) {
    auto client = harness.connect_client();
    ASSERT_TRUE(harness.wait_for_client_count(1));
    ASSERT_TRUE(client->send("only\n"));
    keep_client_alive = client;
  } else {
    auto sender = harness.start_sender();
    ASSERT_TRUE(sender->send("only\n"));
    keep_client_alive = sender;
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return data_batches.load() > 0 && message_batches.load() > 0; }, 5000))
      << "partial batch was never flushed: data_batches=" << data_batches.load()
      << " message_batches=" << message_batches.load();

  std::lock_guard<std::mutex> lock(mutex);
  ASSERT_EQ(data_payloads.size(), 1U);
  ASSERT_EQ(message_payloads.size(), 1U);
  EXPECT_EQ(data_payloads[0], "only\n");
  EXPECT_EQ(message_payloads[0], "only");
}

}  // namespace

TEST(ServerBatchLatencyFlushTest, TcpServerFlushesAPartialBatch) {
  expect_server_partial_batch_flushes_after_latency<TcpServerLoopbackHarness>();
}

TEST(ServerBatchLatencyFlushTest, UdsServerFlushesAPartialBatch) {
  expect_server_partial_batch_flushes_after_latency<UdsServerLoopbackHarness>();
}

TEST(ServerBatchLatencyFlushTest, UdpServerFlushesAPartialBatch) {
  expect_server_partial_batch_flushes_after_latency<UdpServerLoopbackHarness>();
}

namespace {
template <typename H>
class ServerSessionBatchTest : public ::testing::Test {};
using BatchServers = ::testing::Types<TcpServerLoopbackHarness, UdsServerLoopbackHarness, UdpServerLoopbackHarness>;
TYPED_TEST_SUITE(ServerSessionBatchTest, BatchServers);
TYPED_TEST(ServerSessionBatchTest, CountAndLatencyBatchesNeverMixSessions) {
  TypeParam harness;
  auto server = harness.start_server();
  server->batch_size(2).batch_latency(200ms);
  server->framer([] { return std::make_unique<framer::LineFramer>(); });
  std::mutex mutex;
  std::vector<std::vector<std::string>> deliveries;
  server->on_message_batch([&](const auto& batch) {
    if (batch.empty()) return;
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<std::string> payloads;
    for (const auto& message : batch) {
      EXPECT_EQ(message.client_id(), batch.front().client_id());
      payloads.push_back(message.data_as_string());
    }
    deliveries.push_back(std::move(payloads));
  });
  auto connect = [&] {
    if constexpr (requires { harness.connect_client(); })
      return harness.connect_client();
    else
      return harness.start_sender();
  };
  auto first = connect();
  auto second = connect();
  ASSERT_TRUE(first->send("first-a\n"));
  ASSERT_TRUE(second->send("second-a\n"));
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 2; }, 5000));
  ASSERT_TRUE(first->send("first-b\n"));
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] {
        std::lock_guard<std::mutex> lock(mutex);
        size_t count = 0;
        for (const auto& batch : deliveries) count += batch.size();
        return count == 3;
      },
      5000));
  first->stop();
  second->stop();
  server->stop();
  std::lock_guard<std::mutex> lock(mutex);
  bool first_seen = false, second_seen = false;
  for (const auto& batch : deliveries) {
    if (batch == std::vector<std::string>{"first-a", "first-b"}) first_seen = true;
    if (batch == std::vector<std::string>{"second-a"}) second_seen = true;
  }
  EXPECT_TRUE(first_seen);
  EXPECT_TRUE(second_seen);
}
}  // namespace
