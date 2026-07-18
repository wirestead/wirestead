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

#include "wirestead/config/udp_config.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"
#include "wrapper_contract_test_utils.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

TEST(UdpServerWrapperTransportTest, AutoManageStartsInjectedTransport) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;

  boost::asio::io_context ioc;
  auto transport_server = transport::UdpChannel::create(cfg, ioc);
  wrapper::UdpServer server(std::static_pointer_cast<interface::Channel>(transport_server));

  server.auto_start(true);
  ioc.run_for(50ms);

  EXPECT_TRUE(server.listening());

  server.stop();
}

TEST(UdpServerWrapperTransportTest, StartFutureReflectsBindFailure) {
  boost::asio::io_context guard_ioc;
  boost::asio::ip::udp::socket occupied_socket(guard_ioc);
  occupied_socket.open(boost::asio::ip::udp::v4());
  occupied_socket.bind({boost::asio::ip::make_address("127.0.0.1"), 0});

  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = occupied_socket.local_endpoint().port();

  wrapper::UdpServer server(cfg);
  auto started = server.start();

  ASSERT_EQ(started.wait_for(2s), std::future_status::ready);
  EXPECT_FALSE(started.get());
  EXPECT_FALSE(server.listening());

  server.stop();
}

TEST(UdpServerWrapperContractTest, ConnectHandlerReplacementUsesLatestCallback) {
  std::atomic<int> connected{0};
  test::wrapper_support::UdpServerLoopbackHarness harness;
  auto server = harness.start_server();
  server->on_connect([&](const wrapper::ConnectionContext&) { connected = 1; });
  server->on_connect([&](const wrapper::ConnectionContext&) { connected = 2; });

  auto client = harness.start_sender();
  (void)client;
  harness.send_from_client("hello");

  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return connected.load() > 0; }, 5000));
  EXPECT_EQ(connected.load(), 2);
  EXPECT_TRUE(harness.wait_for_client_count(1));
}

TEST(UdpServerWrapperContractTest, DataHandlerReplacementUsesLatestCallback) {
  std::atomic<int> data{0};
  test::wrapper_support::UdpServerLoopbackHarness harness;
  auto server = harness.start_server();
  server->on_data([&](const wrapper::MessageContext&) { data = 1; });
  server->on_data([&](const wrapper::MessageContext&) { data = 2; });

  auto client = harness.start_sender();
  (void)client;
  harness.send_from_client("payload");

  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return data.load() > 0; }, 5000));
  EXPECT_EQ(data.load(), 2);
}
