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

namespace {

using namespace wirestead;
using namespace wirestead::test;

class TcpServerConnectivityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = TestUtils::getAvailableTestPort();
    server_ = std::make_shared<wrapper::TcpServer>(port_);
    server_->on_connect([this](const wrapper::ConnectionContext&) { connected_clients_++; });
    server_->on_disconnect([this](const wrapper::ConnectionContext&) { disconnected_clients_++; });
    auto f = server_->start();
    f.get();  // Ensure listening starts
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
    }
  }

  uint16_t port_;
  std::shared_ptr<wrapper::TcpServer> server_;
  std::atomic<int> connected_clients_{0};
  std::atomic<int> disconnected_clients_{0};
};

TEST_F(TcpServerConnectivityTest, StartsListeningOnAvailablePort) { EXPECT_TRUE(server_->listening()); }

}  // namespace
