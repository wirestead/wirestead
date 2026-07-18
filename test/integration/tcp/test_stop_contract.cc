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
#include "wirestead/base/common.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;

/**
 * @brief Integration tests verifying "No Callbacks after Stop" contract.
 */
class StopContractTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    auto& logger = wirestead::diagnostics::Logger::instance();
    previous_log_level_ = logger.level();
    // Enable debug logging for detailed trace
    logger.set_level(wirestead::diagnostics::LogLevel::DEBUG);
  }

  void TearDown() override {
    wirestead::diagnostics::Logger::instance().set_level(previous_log_level_);
    BaseTest::TearDown();
  }

 private:
  wirestead::diagnostics::LogLevel previous_log_level_{wirestead::diagnostics::LogLevel::INFO};
};

/**
 * @brief Verify that no backpressure callbacks occur after TcpServer stop.
 */
TEST_F(StopContractTest, NoBackpressureCallbackAfterServerStop) {
  uint16_t port = 0;
  try {
    port = TestUtils::getAvailableTestPort();
  } catch (const std::exception& ex) {
    GTEST_SKIP() << "TCP port allocation unavailable in this environment: " << ex.what();
  }

  std::atomic<bool> backpressure_triggered{false};
  std::atomic<int> backpressure_calls{0};
  std::atomic<bool> stop_called{false};

  config::TcpServerConfig cfg;
  cfg.port = port;
  // Lower threshold to ensure backpressure triggers easily when client doesn't read
  cfg.backpressure_threshold = 4096;

  auto server = transport::TcpServer::create(cfg);

  server->on_backpressure([&](size_t queued) {
    if (stop_called.load()) {
      ADD_FAILURE() << "Backpressure callback received AFTER stop! Queued: " << queued;
    }
    if (queued >= cfg.backpressure_threshold || queued <= (cfg.backpressure_threshold / 2)) {
      backpressure_calls++;
    }
    if (queued >= cfg.backpressure_threshold) {
      backpressure_triggered = true;
    }
  });

  server->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server->state() == base::LinkState::Listening; }, 1000));

  // Use a raw Boost Asio client that connects but DOES NOT READ.
  // This forces the server's socket buffer to fill up, causing backpressure.
  boost::asio::io_context ioc;
  boost::asio::ip::tcp::socket socket(ioc);
  boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::make_address("127.0.0.1"), port);

  std::atomic<bool> connected{false};
  socket.async_connect(endpoint, [&](const boost::system::error_code& ec) {
    if (!ec) connected = true;
  });

  // Run IO context until connected
  while (!connected && !ioc.stopped()) {
    ioc.run_one_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(connected);
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server->client_count() == 1; }, 2000));
  ASSERT_EQ(server->connected_clients().size(), 1u);
  const auto client_id = server->connected_clients().front();

  // Send enough data to fill socket buffer and trigger backpressure
  std::string data(1024 * 1024, 'X');
  for (int i = 0; i < 50; ++i) {
    server->send_to_client(client_id, std::string_view(data.data(), data.size()));
  }

  // Wait for backpressure to trigger (because client is not reading)
  if (!TestUtils::waitForCondition([&]() { return backpressure_triggered.load(); }, 5000)) {
    GTEST_SKIP() << "Could not induce TCP send-side backpressure in this environment";
  }
  EXPECT_GT(backpressure_calls.load(), 0);

  server->stop();
  stop_called = true;
  TestUtils::waitFor(200);

  // Cleanup client
  boost::system::error_code ec;
  socket.close(ec);
}

/**
 * @brief Verify that no on_bytes callbacks occur after session is stopped.
 */
TEST_F(StopContractTest, NoDataCallbackAfterServerStop) {
  uint16_t port = TestUtils::getAvailableTestPort();
  std::atomic<bool> stop_called{false};
  std::atomic<int> data_calls{0};

  auto server = tcp_server(port)
                    .on_data([&](const wrapper::MessageContext& ctx) {
                      if (stop_called.load()) {
                        ADD_FAILURE() << "Data callback received AFTER stop! Size: " << ctx.data().size();
                      }
                      data_calls++;
                      std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    })
                    .on_error([](auto&&) {})
                    .build();

  server->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server->listening(); }, 1000));

  boost::asio::io_context ioc;
  boost::asio::ip::tcp::socket socket(ioc);
  boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::make_address("127.0.0.1"), port);
  boost::system::error_code ec;
  socket.connect(endpoint, ec);
  ASSERT_FALSE(ec) << ec.message();
  socket.non_blocking(true, ec);
  ASSERT_FALSE(ec) << ec.message();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server->client_count() == 1; }, 2000));

  std::atomic<bool> sending{true};
  std::thread sender([&]() {
    std::string chunk(1024, 'A');
    while (sending.load()) {
      boost::system::error_code write_ec;
      socket.write_some(boost::asio::buffer(chunk), write_ec);
      if (write_ec == boost::asio::error::would_block || write_ec == boost::asio::error::try_again) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }
      if (write_ec) break;
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return data_calls.load() > 5; }, 2000));

  server->stop();
  stop_called = true;

  sending = false;
  if (sender.joinable()) sender.join();
  socket.close(ec);
  TestUtils::waitFor(200);
}
