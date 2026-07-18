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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/base/common.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class StressTest : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetUp(); }

  void TearDown() override { BaseTest::TearDown(); }
};

TEST_F(StressTest, RealNetworkHighThroughput) {
  const uint16_t port = TestUtils::getAvailableTestPort();
  const size_t chunk_size = 64 * 1024;
  const int chunk_count = 50;

  std::atomic<size_t> server_received_bytes{0};

  auto server = tcp_server(port)
                    .on_data([&](const wrapper::MessageContext& ctx) { server_received_bytes += ctx.data().size(); })
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(server, nullptr);
  server->start();
  TestUtils::waitFor(100);

  std::atomic<bool> client_connected{false};
  auto client = tcp_client("127.0.0.1", port)
                    .on_connect([&](const wrapper::ConnectionContext&) { client_connected = true; })
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(client, nullptr);
  client->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client_connected.load(); }, 5000));

  std::string chunk(chunk_size, 'X');
  auto target_bytes = chunk_size * chunk_count;

  for (int i = 0; i < chunk_count; ++i) {
    client->send(chunk);
    std::this_thread::sleep_for(std::chrono::microseconds(500));
  }

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_received_bytes.load() >= target_bytes; }, 10000));
  EXPECT_EQ(server_received_bytes.load(), target_bytes);

  client->stop();
  server->stop();
}

TEST_F(StressTest, RapidStartStop) {
  const uint16_t port = TestUtils::getAvailableTestPort();
  const int iterations = 20;

  for (int i = 0; i < iterations; ++i) {
    auto server = tcp_server(port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
    ASSERT_NE(server, nullptr);

    auto start_future = server->start();
    // Don't even wait for get() in some cases, or stop immediately
    if (i % 2 == 0) {
      start_future.get();
    }
    server->stop();
  }
}

TEST_F(StressTest, ConcurrentClientConnections) {
  const uint16_t port = TestUtils::getAvailableTestPort();
  const int num_clients = 20;
  const int messages_per_client = 50;
  const std::string payload = "stress_test_data";
  std::atomic<int> connected_count{0};
  std::atomic<int> received_count{0};
  std::atomic<int> send_success_count{0};

  auto server = tcp_server(port)
                    .use_line_framer()
                    .on_connect([&](const wrapper::ConnectionContext&) { connected_count++; })
                    .on_message([&](const wrapper::MessageContext& ctx) {
                      EXPECT_EQ(ctx.data_as_string(), payload);
                      received_count++;
                    })
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  server->start();
  TestUtils::waitForCondition([&]() { return server->listening(); }, 1000);

  std::vector<std::shared_ptr<wrapper::TcpClient>> clients;
  for (int i = 0; i < num_clients; ++i) {
    auto client = tcp_client("127.0.0.1", port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
    clients.push_back(std::move(client));
    clients.back()->start();
  }

  // Wait for all to connect
  TestUtils::waitForCondition([&]() { return connected_count.load() == num_clients; }, 5000);
  EXPECT_EQ(connected_count.load(), num_clients);

  // Small delay to ensure all connections are fully established in the OS
  std::this_thread::sleep_for(200ms);

  // Send messages concurrently
  std::vector<std::thread> senders;
  for (auto& client : clients) {
    senders.emplace_back([client, &send_success_count, messages_per_client, &payload]() {
      for (int i = 0; i < messages_per_client; ++i) {
        if (client->send_line(payload)) {
          send_success_count++;
        }
        std::this_thread::sleep_for(5ms);  // Increased delay
      }
    });
  }

  for (auto& t : senders) t.join();

  EXPECT_EQ(send_success_count.load(), num_clients * messages_per_client);

  // Wait for all data
  TestUtils::waitForCondition([&]() { return received_count.load() == num_clients * messages_per_client; }, 5000);
  EXPECT_EQ(received_count.load(), num_clients * messages_per_client);

  for (auto& client : clients) client->stop();
  server->stop();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
