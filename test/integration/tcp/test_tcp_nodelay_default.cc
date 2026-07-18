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
#include <cstdint>
#include <mutex>
#include <string>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;

// wirestead::tcp_client()/tcp_server() go through three separate layers that
// each used to carry their own tcp_no_delay default (config struct, wrapper
// impl, and the fluent builder that actually constructs those wrappers). A
// prior fix updated the first two but missed the builder layer, so real
// callers going through the public tcp_client()/tcp_server() factory
// functions never actually got TCP_NODELAY enabled. This test exercises that
// exact public path with no explicit .tcp_no_delay() call and a payload
// large enough to span multiple MSS segments in production.
//
// NOTE: this only checks data-integrity of the round trip, not latency. The
// Nagle/delayed-ACK stall this bug caused (~40-48ms, observed on a Jetson
// benchmark) depends on the loopback interface's MTU/MSS and didn't
// reproduce locally on this dev machine (its loopback MTU is 65536, large
// enough that this payload doesn't get split the same way). No unit test
// here can substitute for re-running the real benchmark on the original
// hardware to confirm the latency symptom is actually gone.
TEST(TcpNoDelayDefaultTest, LargePayloadRoundTripSucceedsByDefault) {
  uint16_t test_port = TestUtils::getAvailableTestPort();

  auto server = wirestead::tcp_server(test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(server, nullptr);

  std::atomic<ClientId> server_client_id{0};
  server->on_connect([&](const wrapper::ConnectionContext& ctx) { server_client_id = ctx.client_id(); });
  server->on_data([&](const wrapper::MessageContext& ctx) { server->send_to(server_client_id.load(), ctx.data()); });
  ASSERT_TRUE(server->start().get());

  std::atomic<bool> client_connected{false};
  std::mutex received_mutex;
  std::string received;

  auto client = wirestead::tcp_client("127.0.0.1", test_port)
                    .on_connect([&](const wrapper::ConnectionContext&) { client_connected = true; })
                    .on_data([&](const wrapper::MessageContext& ctx) {
                      std::lock_guard<std::mutex> lock(received_mutex);
                      received.append(ctx.data());
                    })
                    .on_error([](auto&&) {})
                    .build();
  ASSERT_NE(client, nullptr);
  client->start();

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return client_connected.load(); }, 2000))
      << "Client failed to connect within 2 seconds";

  // Larger than a typical MSS (~1448B) on a real NIC, matching the payload
  // size that reproduced the production stall.
  const std::string payload(8192, 'A');

  ASSERT_TRUE(client->send(payload));

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() {
        std::lock_guard<std::mutex> lock(received_mutex);
        return received.size() >= payload.size();
      },
      2000))
      << "Echoed payload was not fully received within 2 seconds";

  {
    std::lock_guard<std::mutex> lock(received_mutex);
    EXPECT_EQ(received, payload);
  }

  client->stop();
  server->stop();
}
