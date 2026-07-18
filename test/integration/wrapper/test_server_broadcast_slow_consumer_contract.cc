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
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/wirestead.hpp"

namespace {

using namespace std::chrono_literals;
using wirestead::test::TestUtils;
namespace net = boost::asio;
using tcp = net::ip::tcp;
using udp = net::ip::udp;
using uds = net::local::stream_protocol;

constexpr size_t kThreshold = wirestead::base::constants::MIN_BACKPRESSURE_THRESHOLD;

bool is_port_allocation_failure(const std::exception& ex) {
  return std::string_view(ex.what()).find("Unable to find available test port") != std::string_view::npos;
}

void expect_fast(std::string_view label, const std::function<bool()>& fn) {
  SCOPED_TRACE(std::string(label));
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(fn());
  EXPECT_LT(std::chrono::steady_clock::now() - start, 100ms);
}

bool call_fast(std::string_view label, const std::function<bool()>& fn) {
  SCOPED_TRACE(std::string(label));
  const auto start = std::chrono::steady_clock::now();
  const bool result = fn();
  EXPECT_LT(std::chrono::steady_clock::now() - start, 100ms);
  return result;
}

template <typename Server>
void expect_observable_broadcast_stats(const Server& server) {
  const auto stats = server.stats();
  EXPECT_GT(stats.messages_accepted, 0u);
  EXPECT_GT(stats.bytes_accepted, 0u);
}

}  // namespace

TEST(ServerBroadcastSlowConsumerContractTest, TcpBroadcastDoesNotBlockOnSingleSlowClient) {
  uint16_t port = 0;
  try {
    port = TestUtils::getAvailableTestPort();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "TCP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->backpressure_threshold(kThreshold).send_buffer_size(kThreshold);
  ASSERT_TRUE(server->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->listening(); }, 5000));

  std::atomic<size_t> healthy_bytes{0};
  auto healthy = std::make_shared<wirestead::wrapper::TcpClient>("127.0.0.1", port);
  healthy->on_data([&](const wirestead::MessageContext& msg) { healthy_bytes.fetch_add(msg.data().size()); });
  ASSERT_TRUE(healthy->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return healthy->connected(); }, 5000));

  net::io_context slow_ioc;
  tcp::socket slow_socket(slow_ioc);
  slow_socket.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  slow_socket.set_option(net::socket_base::receive_buffer_size(static_cast<int>(kThreshold)));
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() >= 2; }, 5000));

  const std::string payload(kThreshold, 't');
  int accepted = 0;
  for (int i = 0; i < 128; ++i) {
    if (call_fast("tcp-broadcast", [&] { return server->broadcast(payload); })) {
      ++accepted;
    }
  }

  EXPECT_GT(accepted, 0);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return healthy_bytes.load() > 0; }, 5000));
  expect_observable_broadcast_stats(*server);

  boost::system::error_code ec;
  slow_socket.close(ec);
  healthy->stop();
  server->stop();
}

TEST(ServerBroadcastSlowConsumerContractTest, TcpBroadcastContinuesDeliveringToHealthyClient) {
  uint16_t port = 0;
  try {
    port = TestUtils::getAvailableTestPort();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "TCP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->backpressure_threshold(kThreshold);
  ASSERT_TRUE(server->start().get());

  std::atomic<int> healthy_callbacks{0};
  auto healthy = std::make_shared<wirestead::wrapper::TcpClient>("127.0.0.1", port);
  healthy->on_data([&](const wirestead::MessageContext&) { healthy_callbacks.fetch_add(1); });
  ASSERT_TRUE(healthy->start().get());

  net::io_context slow_ioc;
  tcp::socket slow_socket(slow_ioc);
  slow_socket.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() >= 2; }, 5000));

  for (int i = 0; i < 16; ++i) {
    expect_fast("tcp-healthy", [&] { return server->broadcast("healthy-tcp"); });
    ASSERT_TRUE(TestUtils::waitForCondition([&] { return healthy_callbacks.load() > i; }, 5000));
  }

  expect_observable_broadcast_stats(*server);

  boost::system::error_code ec;
  slow_socket.close(ec);
  healthy->stop();
  server->stop();
}

TEST(ServerBroadcastSlowConsumerContractTest, UdpBroadcastDoesNotBlockOnSlowVirtualClient) {
  uint16_t port = 0;
  try {
    port = TestUtils::getAvailableTestPort();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "UDP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  wirestead::config::UdpConfig server_cfg;
  server_cfg.bind_address = "127.0.0.1";
  server_cfg.local_port = port;
  server_cfg.backpressure_threshold = kThreshold;

  auto server = std::make_shared<wirestead::wrapper::UdpServer>(server_cfg);
  ASSERT_TRUE(server->start().get());

  wirestead::config::UdpConfig healthy_cfg;
  healthy_cfg.bind_address = "127.0.0.1";
  healthy_cfg.local_port = 0;
  healthy_cfg.remote_address = "127.0.0.1";
  healthy_cfg.remote_port = port;
  auto healthy = std::make_shared<wirestead::wrapper::UdpClient>(healthy_cfg);

  std::atomic<int> healthy_callbacks{0};
  healthy->on_data([&](const wirestead::MessageContext&) { healthy_callbacks.fetch_add(1); });
  ASSERT_TRUE(healthy->start().get());
  healthy->send("register-healthy");

  net::io_context slow_ioc;
  udp::socket slow_socket(slow_ioc, udp::endpoint(net::ip::make_address("127.0.0.1"), 0));
  const udp::endpoint server_ep(net::ip::make_address("127.0.0.1"), port);
  slow_socket.send_to(net::buffer("register-slow", 13), server_ep);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() >= 2; }, 5000));

  for (int i = 0; i < 32; ++i) {
    expect_fast("udp-broadcast", [&] { return server->broadcast("healthy-udp"); });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return healthy_callbacks.load() > 0; }, 5000));
  expect_observable_broadcast_stats(*server);

  boost::system::error_code ec;
  slow_socket.close(ec);
  healthy->stop();
  server->stop();
}

#ifndef _WIN32
TEST(ServerBroadcastSlowConsumerContractTest, UdsBroadcastDoesNotBlockOnSingleSlowClient) {
  auto socket_path = TestUtils::makeUniqueUdsSocketPath("slow-broadcast-contract").string();
  TestUtils::removeFileIfExists(socket_path);

  auto server = std::make_shared<wirestead::wrapper::UdsServer>(socket_path);
  server->backpressure_threshold(kThreshold);
  ASSERT_TRUE(server->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->listening(); }, 5000));

  std::atomic<int> healthy_callbacks{0};
  auto healthy = std::make_shared<wirestead::wrapper::UdsClient>(socket_path);
  healthy->on_data([&](const wirestead::MessageContext&) { healthy_callbacks.fetch_add(1); });
  ASSERT_TRUE(healthy->start().get());

  net::io_context slow_ioc;
  uds::socket slow_socket(slow_ioc);
  slow_socket.connect(uds::endpoint(socket_path));
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() >= 2; }, 5000));

  for (int i = 0; i < 32; ++i) {
    expect_fast("uds-broadcast", [&] { return server->broadcast("healthy-uds"); });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return healthy_callbacks.load() > 0; }, 5000));
  expect_observable_broadcast_stats(*server);

  boost::system::error_code ec;
  slow_socket.close(ec);
  healthy->stop();
  server->stop();
  TestUtils::removeFileIfExists(socket_path);
}
#endif
