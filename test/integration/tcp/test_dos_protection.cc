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
#include "wirestead/base/platform.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using wirestead::test::TestUtils;
using namespace std::chrono_literals;

class DoSProtectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset logger callback
    diagnostics::Logger::instance().set_callback(nullptr);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
    }
    diagnostics::Logger::instance().set_callback(nullptr);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  uint16_t getTestPort() { return TestUtils::getAvailableTestPort(); }

  std::shared_ptr<wrapper::TcpServer> server_;
};

TEST_F(DoSProtectionTest, TightLoopPrevention) {
  uint16_t test_port = getTestPort();
  std::cout << "Testing DoS protection, port: " << test_port << std::endl;

  // Setup logger to count rejections
  std::atomic<int> rejection_count{0};
  diagnostics::Logger::instance().set_callback([&](diagnostics::LogLevel level, const std::string& msg) {
    if (msg.find("Client connection rejected") != std::string::npos) {
      rejection_count++;
    }
  });
  // Ensure we capture everything
  diagnostics::Logger::instance().set_level(diagnostics::LogLevel::DEBUG);

  // Create single client server
  server_ = wirestead::tcp_server(test_port)
                .on_data([](const MessageContext&) {})
                .on_error([](const ErrorContext&) {})
                .max_clients(1)
                .port_retry(true, 3, 1000)
                .build();
  ASSERT_NE(server_, nullptr) << "Server creation failed";
  server_->start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // 1. Connect first client (Success)
  auto s1 = wirestead::tcp_client("127.0.0.1", test_port)
                .on_data([](const MessageContext&) {})
                .on_error([](const ErrorContext&) {})
                .build();
  s1->start();

  // Wait for connection
  bool s1_connected = TestUtils::waitForCondition([&]() { return s1->connected(); }, 2000);

  if (!s1_connected) {
    FAIL() << "Client 1 failed to connect";
  }
  std::cout << "Client 1 connected" << std::endl;

  // Verify s1 is holding the slot
  bool server_saw_client = TestUtils::waitForCondition([&]() { return server_->client_count() == 1; }, 1000);
  if (!server_saw_client) {
    FAIL() << "Server failed to detect Client 1 connection. Client count: " << server_->client_count();
  }

  // 2. Flood server with attempts
  std::cout << "Flooding server..." << std::endl;

  std::atomic<bool> flooding{true};
  std::atomic<uint64_t> attempt_count{0};
  std::thread flooder([&]() {
    boost::asio::io_context ioc2;
    boost::asio::ip::tcp::resolver resolver(ioc2);

    boost::system::error_code rec;
    auto endpoints = resolver.resolve("127.0.0.1", std::to_string(test_port), rec);
    if (rec || endpoints.begin() == endpoints.end()) {
      return;
    }
    auto ep = endpoints.begin()->endpoint();

    while (flooding.load(std::memory_order_relaxed)) {
      boost::asio::ip::tcp::socket s(ioc2);

      boost::system::error_code ec;
      s.open(ep.protocol(), ec);
      if (!ec) {
        s.non_blocking(true, ec);
      }
      if (!ec) {
        s.connect(ep, ec);
        attempt_count.fetch_add(1, std::memory_order_relaxed);
      }

      boost::system::error_code ignored;
      s.close(ignored);

      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  // Let it flood for 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));
  flooding = false;
  flooder.join();

  std::cout << "Flood finished. Attempts: " << attempt_count << ", Rejections: " << rejection_count << std::endl;

  // Verify s1 is STILL connected
  if (server_->client_count() != 1) {
    FAIL() << "Client 1 disconnected during flood. Client count: " << server_->client_count();
  }

  // With the fix, rejections should be minimal (typically 1).
  // Without fix, rejections would be comparable to attempts or at least high.
  EXPECT_LE(rejection_count, 5) << "Server should pause accepting when full, preventing log flood";

  // Verify we can resume accepting after client disconnects
  s1->stop();
  std::cout << "Client 1 disconnected, waiting for resume..." << std::endl;

  // Wait for server to detect disconnection and resume
  TestUtils::waitForCondition([&]() { return server_->client_count() == 0; }, 2000);

  // Try to connect again
  auto s3 = wirestead::tcp_client("127.0.0.1", test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  s3->start();

  // Wait for connection
  bool s3_connected = TestUtils::waitForCondition([&]() { return s3->connected(); }, 2000);

  if (!s3_connected) {
    FAIL() << "Failed to connect after resume";
  }
  std::cout << "Client 3 connected (Resume success)" << std::endl;
  s3->stop();
}
