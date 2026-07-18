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
#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;

namespace net = boost::asio;
using tcp = net::ip::tcp;

class TcpAbortTest : public ::testing::Test {
 protected:
  void SetUp() override { test_port_ = TestUtils::getAvailableTestPort(); }
  uint16_t test_port_;
};

TEST_F(TcpAbortTest, SessionAbortion) {
  net::io_context ioc;
  auto guard = net::make_work_guard(ioc);
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), test_port_));

  std::thread ioc_thread([&] { ioc.run(); });

  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = test_port_;
  cfg.connection_timeout_ms = 500;

  auto client = TcpClient::create(cfg);

  std::atomic<bool> aborted{false};
  client->on_state([&](LinkState state) {
    if (state == LinkState::Error) aborted = true;
  });

  client->start();

  // Connect and then force close socket to simulate reset/abort
  tcp::socket server_side(ioc);
  acceptor.accept(server_side);

  server_side.set_option(tcp::no_delay(true));
  server_side.close();

  TestUtils::waitForCondition([&]() { return aborted.load() || !client->is_connected(); }, 2000);

  client->stop();
  guard.reset();
  ioc.stop();
  if (ioc_thread.joinable()) ioc_thread.join();

  SUCCEED();
}
