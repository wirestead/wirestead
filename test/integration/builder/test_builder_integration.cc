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
#include <string>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class UnifiedBuilderIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }
  uint16_t port_;
};

TEST_F(UnifiedBuilderIntegrationTest, RealCommunicationBetweenBuilderObjects) {
  std::atomic<bool> data_received{false};
  std::string received_msg;

  auto server = builder::UnifiedBuilder::tcp_server(port_)
                    .on_data([&](const wrapper::MessageContext& ctx) {
                      received_msg = std::string(ctx.data());
                      data_received = true;
                    })
                    .on_error([](auto&&) {})
                    .build();

  auto client =
      builder::UnifiedBuilder::tcp_client("127.0.0.1", port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  auto start_f = server->start();
  ASSERT_TRUE(start_f.get());

  client->start();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client->connected(); }, 5000));

  if (client->connected()) {
    client->send("hello from unified");
    EXPECT_TRUE(TestUtils::waitForCondition([&]() { return data_received.load(); }, 5000));
    EXPECT_EQ(received_msg, "hello from unified");
  }

  client->stop();
  server->stop();
}

TEST_F(UnifiedBuilderIntegrationTest, BuilderConfigurationAffectsCommunication) {
  // Use a unique port for this sub-test
  uint16_t p = TestUtils::getAvailableTestPort();

  auto client = builder::UnifiedBuilder::tcp_client("127.0.0.1", p)
                    .retry_interval(100ms)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  // Start client without server
  auto f = client->start();
  // Should not block here as it's retrying in background

  TestUtils::waitFor(300);

  auto server = builder::UnifiedBuilder::tcp_server(p).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server->start();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client->connected(); }, 5000));

  client->stop();
  server->stop();
}
