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
#include <memory>
#include <mutex>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;

namespace net = boost::asio;
using tcp = net::ip::tcp;

class StopCallbackIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { test_port_ = TestUtils::getAvailableTestPort(); }
  uint16_t test_port_;
};

TEST_F(StopCallbackIntegrationTest, TcpClientStopFromCallbackDoesNotDeadlock) {
  net::io_context server_ioc;
  auto guard = net::make_work_guard(server_ioc);
  tcp::acceptor acceptor(server_ioc, tcp::endpoint(tcp::v4(), test_port_));
  auto server_socket = std::make_shared<tcp::socket>(server_ioc);

  std::atomic<bool> stop_from_state{false};
  std::atomic<bool> stop_from_bytes{false};

  acceptor.async_accept(*server_socket, [&](const boost::system::error_code& ec) {
    if (ec) return;
    net::post(server_ioc, [server_socket]() {
      boost::system::error_code send_ec;
      net::write(*server_socket, net::buffer("ping", 4), send_ec);
    });
  });

  std::thread server_thread([&]() {
    try {
      server_ioc.run();
    } catch (...) {
    }
  });

  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = test_port_;
  cfg.connection_timeout_ms = 500;
  cfg.retry_interval_ms = 50;
  cfg.max_retries = 0;

  auto client = TcpClient::create(cfg);
  std::weak_ptr<TcpClient> weak_client = client;

  client->on_state([weak_client, &stop_from_state](LinkState state) {
    if (state == LinkState::Connected) {
      stop_from_state.store(true);
      if (auto c = weak_client.lock()) c->stop();
    }
  });
  client->on_bytes([weak_client, &stop_from_bytes](memory::ConstByteSpan) {
    stop_from_bytes.store(true);
    if (auto c = weak_client.lock()) c->stop();
  });

  client->start();
  TestUtils::waitFor(1000);
  client->stop();

  EXPECT_FALSE(client->is_connected());
  EXPECT_TRUE(stop_from_state.load() || stop_from_bytes.load());

  guard.reset();
  server_ioc.stop();
  if (server_thread.joinable()) server_thread.join();
}
