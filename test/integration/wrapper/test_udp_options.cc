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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <memory>

#include "test_utils.hpp"
#include "wirestead/builder/udp_builder.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp.hpp"

using namespace wirestead::wrapper;
using namespace wirestead::config;
using namespace wirestead::test;

class UdpOptionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Basic setup
  }

  void TearDown() override {
    // Basic teardown
  }
};

TEST_F(UdpOptionsTest, SetterCoverage) {
  UdpConfig config;
  config.local_port = 0;

  UdpClient udp(config);

  // Test auto_start setter
  // It returns ChannelInterface& so we can chain it, but UdpClient wrapper implements it.
  udp.auto_start(true);
  udp.auto_start(false);

  // Test manage_external_context
  udp.manage_external_context(true);
  udp.manage_external_context(false);
}

TEST_F(UdpOptionsTest, ConstructorWithExternalContext) {
  UdpConfig config;
  config.local_port = 0;

  auto ioc = std::make_shared<boost::asio::io_context>();
  UdpClient udp(config, ioc);

  // Should not throw
  udp.auto_start(false);
}

TEST_F(UdpOptionsTest, AutoManageStartsInjectedTransport) {
  using namespace std::chrono_literals;

  UdpConfig sender_cfg;
  sender_cfg.local_port = 0;
  sender_cfg.remote_address = "127.0.0.1";
  const uint16_t receiver_port = TestUtils::getAvailableTestPort();
  sender_cfg.remote_port = receiver_port;

  UdpConfig receiver_cfg;
  receiver_cfg.bind_address = "127.0.0.1";
  receiver_cfg.local_port = receiver_port;

  boost::asio::io_context sender_ioc;
  boost::asio::io_context receiver_ioc;

  auto sender_transport = wirestead::transport::UdpChannel::create(sender_cfg, sender_ioc);
  auto receiver_transport = wirestead::transport::UdpChannel::create(receiver_cfg, receiver_ioc);

  UdpClient receiver(std::static_pointer_cast<wirestead::interface::Channel>(receiver_transport));
  auto receiver_started = receiver.start();
  receiver_ioc.run_for(50ms);
  ASSERT_EQ(receiver_started.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(receiver_started.get());

  UdpClient sender(std::static_pointer_cast<wirestead::interface::Channel>(sender_transport));
  sender.auto_start(true);
  sender_ioc.run_for(50ms);

  EXPECT_TRUE(sender.connected());

  sender.stop();
  receiver.stop();
}

TEST_F(UdpOptionsTest, StartFutureReflectsBindFailure) {
  using namespace std::chrono_literals;

  boost::asio::io_context guard_ioc;
  boost::asio::ip::udp::socket occupied_socket(guard_ioc);
  occupied_socket.open(boost::asio::ip::udp::v4());
  occupied_socket.bind({boost::asio::ip::make_address("127.0.0.1"), 0});

  UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = occupied_socket.local_endpoint().port();

  UdpClient udp(cfg);
  auto started = udp.start();

  ASSERT_EQ(started.wait_for(2s), std::future_status::ready);
  EXPECT_FALSE(started.get());
  EXPECT_FALSE(udp.connected());

  udp.stop();
}

TEST_F(UdpOptionsTest, BuilderCoverageForUdpSocketOptions) {
  wirestead::builder::UdpClientBuilder client_builder;
  client_builder.broadcast(true).reuse_address(true);

  wirestead::builder::UdpServerBuilder server_builder;
  server_builder.broadcast(true).reuse_address(true);
}
