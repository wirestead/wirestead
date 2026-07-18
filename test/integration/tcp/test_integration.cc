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
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

// Helper fixture for Integration Tests
class TcpIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { test_port_ = TestUtils::getAvailableTestPort(); }
  uint16_t test_port_;
};

// ============================================================================
// BUILDER INTEGRATION TESTS
// ============================================================================

TEST_F(TcpIntegrationTest, BuilderPatternIntegration) {
  auto server = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  EXPECT_NE(server, nullptr);

  auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  EXPECT_NE(client, nullptr);
}

TEST_F(TcpIntegrationTest, AutoInitialization) {
  builder::AutoInitializer::ensure_io_context_running();
  TestUtils::waitFor(100);
  EXPECT_TRUE(builder::AutoInitializer::io_context_running());
}

TEST_F(TcpIntegrationTest, MethodChaining) {
  auto client =
      wirestead::tcp_client("127.0.0.1", test_port_)
          .auto_start(false)
          .on_connect([](const wrapper::ConnectionContext&) { std::cout << "Connected!" << std::endl; })
          .on_disconnect([](const wrapper::ConnectionContext&) { std::cout << "Disconnected!" << std::endl; })
          .on_data([](const wrapper::MessageContext& ctx) { std::cout << "Data: " << ctx.data() << std::endl; })
          .on_error([](const wrapper::ErrorContext& ctx) { std::cout << "Error: " << ctx.message() << std::endl; })
          .build();

  EXPECT_NE(client, nullptr);
}

TEST_F(TcpIntegrationTest, IndependentContext) {
  auto client = wirestead::tcp_client("127.0.0.1", test_port_)
                    .independent_context(true)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  EXPECT_NE(client, nullptr);

  auto server = wirestead::tcp_server(test_port_)
                    .independent_context(false)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  EXPECT_NE(server, nullptr);
}

// ============================================================================
// COMMUNICATION TESTS
// ============================================================================

TEST_F(TcpIntegrationTest, BasicCommunication) {
  uint16_t comm_port = TestUtils::getAvailableTestPort();

  std::atomic<bool> server_connected{false};
  std::atomic<bool> client_connected{false};
  std::atomic<bool> data_received{false};
  std::string received_data;

  auto server = wirestead::tcp_server(comm_port)
                    .on_connect([&server_connected](const wrapper::ConnectionContext&) { server_connected = true; })
                    .on_data([&data_received, &received_data](const wrapper::MessageContext& ctx) {
                      received_data = std::string(ctx.data());
                      data_received = true;
                    })
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(server, nullptr);
  EXPECT_TRUE(server->start().get());

  auto client = wirestead::tcp_client("127.0.0.1", comm_port)
                    .on_connect([&client_connected](const wrapper::ConnectionContext&) { client_connected = true; })
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(client, nullptr);
  client->start();

  // Wait for connections with timeout
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return client_connected.load() && server_connected.load(); }, 2000))
      << "Server or client failed to connect within 2 seconds";

  std::string test_msg = "Hello Wirestead!";
  client->send(test_msg);

  // Wait for data with timeout
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return data_received.load(); }, 2000))
      << "Data was not received within 2 seconds";
  EXPECT_EQ(received_data, test_msg);

  client->stop();
  server->stop();
}

TEST_F(TcpIntegrationTest, ErrorHandling) {
  // Test invalid port (now mostly caught by InputValidator if used, but let's test runtime)
  // wirestead::tcp_server(0) might throw or return nullptr depending on version
  try {
    auto server = wirestead::tcp_server(0).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
    if (server) server->start();
  } catch (...) {
  }

  std::atomic<bool> error_occurred{false};
  auto client = wirestead::tcp_client("127.0.0.1", 1)
                    .on_error([&error_occurred](const wrapper::ErrorContext&) { error_occurred = true; })
                    .on_data([](auto&&) {})
                    .build();

  EXPECT_NE(client, nullptr);
  client->start();
  TestUtils::waitFor(200);
  EXPECT_TRUE(error_occurred.load() || !client->connected());
}

// ============================================================================
// ARCHITECTURE TESTS
// ============================================================================

TEST_F(TcpIntegrationTest, ResourceSharing) {
  std::vector<std::unique_ptr<wrapper::TcpClient>> clients;
  for (int i = 0; i < 3; ++i) {
    auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
    EXPECT_NE(client, nullptr);
    clients.push_back(std::move(client));
  }
  EXPECT_EQ(clients.size(), 3);
}

TEST_F(TcpIntegrationTest, MultipleClientConnections) {
  uint16_t comm_port = TestUtils::getAvailableTestPort();
  std::atomic<int> connection_count{0};

  auto server = wirestead::tcp_server(comm_port)
                    .on_connect([&connection_count](const wrapper::ConnectionContext&) { connection_count++; })
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(server, nullptr);
  server->start();

  std::vector<std::unique_ptr<wrapper::TcpClient>> clients;
  for (int i = 0; i < 3; ++i) {
    auto client = wirestead::tcp_client("127.0.0.1", comm_port)
                      .auto_start(true)
                      .on_data([](auto&&) {})
                      .on_error([](auto&&) {})
                      .build();
    clients.push_back(std::move(client));
    TestUtils::waitFor(100);
  }

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connection_count.load() >= 3; }, 5000));
}

TEST_F(TcpIntegrationTest, ComprehensiveBuilderMethodChaining) {
  auto client = wirestead::tcp_client("127.0.0.1", test_port_)
                    .auto_start(false)
                    .independent_context(true)
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_data([](const wrapper::MessageContext&) {})
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  EXPECT_NE(client, nullptr);

  auto server = wirestead::tcp_server(test_port_)
                    .auto_start(false)
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_data([](const wrapper::MessageContext&) {})
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  EXPECT_NE(server, nullptr);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
