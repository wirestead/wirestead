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

class StableCoreIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }
  uint16_t port_;
};

TEST_F(StableCoreIntegrationTest, ServerClientStability) {
  std::atomic<int> msg_count{0};
  auto server =
      tcp_server(port_).on_data([&](const wrapper::MessageContext&) { msg_count++; }).on_error([](auto&&) {}).build();

  ASSERT_TRUE(server->start().get());

  auto client = tcp_client("127.0.0.1", port_).auto_start(true).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client->connected(); }, 2000));

  for (int i = 0; i < 10; ++i) {
    client->send("ping");
    std::this_thread::sleep_for(10ms);
  }

  TestUtils::waitForCondition([&]() { return msg_count.load() >= 10; }, 2000);
  EXPECT_GE(msg_count.load(), 10);

  client->stop();
  server->stop();
}
