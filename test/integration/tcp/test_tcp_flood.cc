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
#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class TcpFloodTest : public ::testing::Test {
 protected:
  void SetUp() override { test_port_ = TestUtils::getAvailableTestPort(); }
  uint16_t test_port_;
};

TEST_F(TcpFloodTest, FloodServer) {
  std::atomic<size_t> received_bytes{0};
  auto server = tcp_server(test_port_)
                    .on_data([&](const wrapper::MessageContext& ctx) { received_bytes += ctx.data().size(); })
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_TRUE(server->start().get());

  const int num_clients = 3;
  const int messages_per_client = 20;
  const size_t expected_bytes = static_cast<size_t>(num_clients * messages_per_client);  // "p" is 1 byte
  std::atomic<bool> client_connect_failed{false};
  std::vector<std::thread> clients;

  for (int i = 0; i < num_clients; ++i) {
    clients.emplace_back([&]() {
      auto client =
          tcp_client("127.0.0.1", test_port_).auto_start(true).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
      bool connected = TestUtils::waitForCondition([&]() { return client->connected(); }, 5000);
      if (!connected) {
        client_connect_failed.store(true);
        client->stop();
        return;
      }
      for (int j = 0; j < messages_per_client; ++j) {
        client->send("p");
        std::this_thread::sleep_for(1ms);
      }
      // Prefer waiting for receipt to avoid dropping queued writes on slower CI runners.
      TestUtils::waitForCondition([&]() { return received_bytes.load() >= expected_bytes; }, 2000);
      client->stop();
    });
  }

  for (auto& t : clients) t.join();
  EXPECT_FALSE(client_connect_failed.load()) << "One or more flood clients failed to connect";

  bool success = TestUtils::waitForCondition([&]() { return received_bytes.load() >= expected_bytes; }, 10000);

  EXPECT_TRUE(success) << "Final received bytes: " << received_bytes.load() << " / " << expected_bytes;
  server->stop();
}
