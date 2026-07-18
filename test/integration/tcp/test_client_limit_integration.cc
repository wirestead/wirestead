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
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using wirestead::test::TestUtils;
using namespace std::chrono_literals;

class ClientLimitIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset logger or other states if needed
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
    }
  }

  uint16_t getTestPort() { return TestUtils::getAvailableTestPort(); }

  // Simulation that keeps connections alive for a fixed duration
  std::vector<std::future<bool>> simulateClients(const std::string& host, uint16_t port, int count,
                                                 int hold_ms = 2000) {
    std::vector<std::future<bool>> futures;
    for (int i = 0; i < count; ++i) {
      futures.push_back(std::async(std::launch::async, [host, port, i, hold_ms]() {
        try {
          std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));  // Jitter

          boost::asio::io_context ioc;
          boost::asio::ip::tcp::socket socket(ioc);
          boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::make_address(host), port);

          boost::system::error_code ec;
          socket.connect(endpoint, ec);

          if (!ec) {
            // Keep the connection alive so the server can count it
            std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms));
            return true;
          }
          return false;
        } catch (...) {
          return false;
        }
      }));
    }
    return futures;
  }

 protected:
  std::shared_ptr<wrapper::TcpServer> server_;
};

TEST_F(ClientLimitIntegrationTest, SingleClientLimitTest) {
  uint16_t test_port = getTestPort();
  server_ = wirestead::tcp_server(test_port).max_clients(1).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(server_, nullptr);
  ASSERT_TRUE(server_->start().get());

  // Start 3 clients, they will try to hold connection for 2s
  auto client_futures = simulateClients("127.0.0.1", test_port, 3);

  // Wait for at least one to be established
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  // Verify it never exceeds 1
  std::this_thread::sleep_for(500ms);
  EXPECT_LE(server_->client_count(), 1);

  for (auto& f : client_futures) f.wait();
}

TEST_F(ClientLimitIntegrationTest, MultiClientLimitTest) {
  uint16_t test_port = getTestPort();
  server_ = wirestead::tcp_server(test_port).max_clients(2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(server_, nullptr);
  ASSERT_TRUE(server_->start().get());

  auto client_futures = simulateClients("127.0.0.1", test_port, 4);

  // Wait for connections to reach limit
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 2; }, 5000));

  // Verify it never exceeds 2
  std::this_thread::sleep_for(500ms);
  EXPECT_LE(server_->client_count(), 2);

  for (auto& f : client_futures) f.wait();
}

TEST_F(ClientLimitIntegrationTest, DefaultClientLimitAllowsTypicalLoad) {
  uint16_t test_port = getTestPort();
  server_ = wirestead::tcp_server(test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(server_, nullptr);
  ASSERT_TRUE(server_->start().get());

  // Hold connections for 3 seconds to ensure they overlap
  auto client_futures = simulateClients("127.0.0.1", test_port, 5, 3000);

  // Verify server sees all 5 simultaneous connections
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 5; }, 10000));

  for (auto& f : client_futures) f.wait();
}

TEST_F(ClientLimitIntegrationTest, MaxClientsZeroAllowsServerCreation) {
  uint16_t test_port = getTestPort();
  server_ = wirestead::tcp_server(test_port).max_clients(0).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  ASSERT_NE(server_, nullptr);
  EXPECT_TRUE(server_->start().get());
}
