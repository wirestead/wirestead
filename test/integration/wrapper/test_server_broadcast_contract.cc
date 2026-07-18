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
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"
#include "wrapper_contract_test_utils.hpp"

namespace {

using namespace std::chrono_literals;
using wirestead::test::TestUtils;

void expect_fast_broadcast(std::string_view transport, const std::function<bool()>& broadcast) {
  SCOPED_TRACE(std::string(transport));
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(broadcast());
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, 100ms);
}

bool is_port_allocation_failure(const std::exception& ex) {
  return std::string_view(ex.what()).find("Unable to find available test port") != std::string_view::npos;
}

}  // namespace

TEST(ServerBroadcastContractTest, TcpBroadcastIsNonBlockingFanoutAndDeliversToConnectedClients) {
  std::unique_ptr<wirestead::test::wrapper_support::TcpServerLoopbackHarness> harness;
  try {
    harness = std::make_unique<wirestead::test::wrapper_support::TcpServerLoopbackHarness>();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "TCP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = harness->start_server();
  std::atomic<int> received{0};

  auto client = harness->connect_client();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  ASSERT_TRUE(harness->wait_for_client_count(1));

  expect_fast_broadcast("tcp", [&]() { return server->broadcast("tcp-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("tcp-try", [&]() { return server->try_broadcast("tcp-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}

TEST(ServerBroadcastContractTest, UdpBroadcastIsNonBlockingFanoutAndDeliversToKnownClients) {
  std::unique_ptr<wirestead::test::wrapper_support::UdpServerLoopbackHarness> harness;
  try {
    harness = std::make_unique<wirestead::test::wrapper_support::UdpServerLoopbackHarness>();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "UDP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = harness->start_server();
  std::atomic<int> received{0};

  auto client = harness->start_sender();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  harness->send_from_client("register");
  ASSERT_TRUE(harness->wait_for_client_count(1));

  expect_fast_broadcast("udp", [&]() { return server->broadcast("udp-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("udp-try", [&]() { return server->try_broadcast("udp-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}

#ifndef _WIN32
TEST(ServerBroadcastContractTest, UdsBroadcastIsNonBlockingFanoutAndDeliversToConnectedClients) {
  wirestead::test::wrapper_support::UdsServerLoopbackHarness harness("broadcast-contract");
  std::shared_ptr<wirestead::wrapper::UdsServer> server;
  try {
    server = harness.start_server();
  } catch (const std::exception& ex) {
    GTEST_SKIP() << "UDS unavailable in this environment: " << ex.what();
  }

  std::atomic<int> received{0};
  auto client = harness.connect_client();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  ASSERT_TRUE(harness.wait_for_client_count(1));

  expect_fast_broadcast("uds", [&]() { return server->broadcast("uds-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("uds-try", [&]() { return server->try_broadcast("uds-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}
#endif
