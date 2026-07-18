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

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class CoreIntegratedTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }
  uint16_t port_;
};

TEST_F(CoreIntegratedTest, BasicLifecycle) {
  auto server = tcp_server(port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  EXPECT_TRUE(server->start().get());
  EXPECT_TRUE(server->listening());
  server->stop();
  EXPECT_FALSE(server->listening());
}

TEST_F(CoreIntegratedTest, ExternalIoContextSharing) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  bool task_executed = false;

  auto server = std::make_shared<wrapper::TcpServer>(port_, ioc);
  server->start();

  boost::asio::post(*ioc, [&task_executed]() { task_executed = true; });

  // Run manually or via thread
  std::thread t([&]() { ioc->run(); });

  TestUtils::waitForCondition([&]() { return task_executed; }, 1000);
  EXPECT_TRUE(task_executed);

  server->stop();
  ioc->stop();
  if (t.joinable()) t.join();
}
