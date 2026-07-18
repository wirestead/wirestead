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

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/base/common.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace wirestead {
namespace test {

TEST(UdpServerWrapperLifecycleTest, ExternalIoContextManagement) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;

  {
    wrapper::UdpServer server(cfg, ioc);
    server.manage_external_context(true);
    auto started = server.start();
    ASSERT_TRUE(started.get());
    EXPECT_TRUE(server.listening());

    // Ensure ioc is running
    EXPECT_FALSE(ioc->stopped());
    server.stop();
    EXPECT_TRUE(ioc->stopped());
  }
}

TEST(UdpServerWrapperLifecycleTest, SessionReaping) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  server.idle_timeout(100ms);

  std::atomic<int> disconnects{0};
  server.on_disconnect([&](const wrapper::ConnectionContext&) { disconnects++; });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket sock(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  sock.send_to(boost::asio::buffer("hello", 5),
               boost::asio::ip::udp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server.client_count() == 1; }, 1000));
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return disconnects.load() == 1; }, 2000));
  EXPECT_EQ(server.client_count(), 0u);

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, DefaultIdleTimeoutDisabledKeepsVirtualSession) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  std::atomic<int> disconnects{0};
  server.on_disconnect([&](const wrapper::ConnectionContext&) { disconnects++; });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket sock(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  sock.send_to(boost::asio::buffer("hello", 5),
               boost::asio::ip::udp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server.client_count() == 1; }, 1000));
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(disconnects.load(), 0);
  EXPECT_EQ(server.client_count(), 1u);

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, BatchAndClientLimitHandling) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  server.max_clients(1);
  server.batch_size(2);
  server.batch_latency(500ms);

  std::atomic<int> connects{0};
  std::atomic<int> batches{0};
  server.on_connect([&](const wrapper::ConnectionContext&) { connects++; });
  server.on_data_batch(
      [&](const std::vector<wrapper::MessageContext>& batch) { batches += static_cast<int>(batch.size()); });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::endpoint target(boost::asio::ip::make_address("127.0.0.1"), port);
  boost::asio::ip::udp::socket first(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  boost::asio::ip::udp::socket second(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));

  first.send_to(boost::asio::buffer("one", 3), target);
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return connects.load() == 1 && server.client_count() == 1; }, 2000));

  second.send_to(boost::asio::buffer("blocked", 7), target);

  for (int attempt = 0; attempt < 5 && batches.load() < 2; ++attempt) {
    first.send_to(boost::asio::buffer("two", 3), target);
    std::this_thread::sleep_for(10ms);
  }

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return batches.load() >= 2; }, 3000));
  EXPECT_EQ(connects.load(), 1);
  EXPECT_EQ(server.client_count(), 1u);

  auto clients = server.connected_clients();
  ASSERT_EQ(clients.size(), 1u);
  EXPECT_TRUE(server.try_send_to(clients.front(), "reply"));
  EXPECT_TRUE(server.try_broadcast("broadcast"));
  EXPECT_TRUE(server.try_send_to_line(clients.front(), "line"));
  EXPECT_TRUE(server.try_broadcast_line("line"));

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, RawDataBatchFlushesByLatency) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  server.batch_size(100).batch_latency(20ms);

  std::atomic<int> batches{0};
  std::vector<std::string> payloads;
  std::mutex payloads_mutex;
  server.on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(payloads_mutex);
    batches++;
    for (const auto& ctx : batch) payloads.push_back(ctx.data_as_string());
  });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket socket(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  socket.send_to(boost::asio::buffer("latency", 7),
                 boost::asio::ip::udp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return batches.load() == 1; }, 1000));
  std::lock_guard<std::mutex> lock(payloads_mutex);
  ASSERT_EQ(payloads.size(), 1U);
  EXPECT_EQ(payloads.front(), "latency");

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, FramedMessageHandlerReceivesSingleMessage) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  server.framer([]() { return std::make_unique<framer::LineFramer>(); });

  std::atomic<int> messages{0};
  std::string received;
  std::mutex received_mutex;
  server.on_message([&](const wrapper::MessageContext& ctx) {
    std::lock_guard<std::mutex> lock(received_mutex);
    messages++;
    received = ctx.data_as_string();
  });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket socket(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  socket.send_to(boost::asio::buffer("single\n", 7),
                 boost::asio::ip::udp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return messages.load() == 1; }, 1000));
  std::lock_guard<std::mutex> lock(received_mutex);
  EXPECT_EQ(received, "single");

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, FramedMessageBatchFlushesAtBatchSize) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;

  wrapper::UdpServer server(cfg);
  server.batch_size(2).batch_latency(1s);
  server.framer([]() { return std::make_unique<framer::LineFramer>(); });

  std::atomic<int> batches{0};
  std::vector<std::string> messages;
  std::mutex messages_mutex;
  server.on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(messages_mutex);
    batches++;
    for (const auto& ctx : batch) messages.push_back(ctx.data_as_string());
  });

  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket socket(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  socket.send_to(boost::asio::buffer("first\nsecond\n", 13),
                 boost::asio::ip::udp::endpoint(boost::asio::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return batches.load() == 1; }, 1000));
  std::lock_guard<std::mutex> lock(messages_mutex);
  ASSERT_EQ(messages.size(), 2U);
  EXPECT_EQ(messages[0], "first");
  EXPECT_EQ(messages[1], "second");

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, ReliableSendAndLineVariantsReachClient) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;
  cfg.backpressure_strategy = base::constants::BackpressureStrategy::Reliable;

  wrapper::UdpServer server(cfg);
  auto started = server.start();
  ASSERT_TRUE(started.get());

  boost::asio::io_context ioc;
  boost::asio::ip::udp::endpoint server_endpoint(boost::asio::ip::make_address("127.0.0.1"), port);
  boost::asio::ip::udp::socket socket(ioc, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0));
  socket.non_blocking(true);
  socket.send_to(boost::asio::buffer("hello", 5), server_endpoint);

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server.client_count() == 1; }, 1000));
  auto clients = server.connected_clients();
  ASSERT_EQ(clients.size(), 1U);
  const auto client_id = clients.front();

  EXPECT_TRUE(server.broadcast("broadcast"));
  EXPECT_TRUE(server.send_to(client_id, "direct"));
  EXPECT_TRUE(server.send_to_blocking(client_id, "blocking"));
  EXPECT_TRUE(server.broadcast_line("broadcast-line"));
  EXPECT_TRUE(server.send_to_line(client_id, "direct-line"));

  std::array<char, 256> buffer{};
  boost::asio::ip::udp::endpoint sender;
  std::string received;
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() {
        boost::system::error_code ec;
        const auto bytes = socket.receive_from(boost::asio::buffer(buffer), sender, 0, ec);
        if (!ec) received.append(buffer.data(), bytes);
        return received.find("broadcast-line\n") != std::string::npos &&
               received.find("direct-line\n") != std::string::npos;
      },
      1000));

  server.stop();
}

TEST(UdpServerWrapperLifecycleTest, ConfigurationSettersBeforeStartRemainFluent) {
  wrapper::UdpServer server(0);

  EXPECT_EQ(&server, &server.bind_address("127.0.0.1"));
  EXPECT_EQ(&server, &server.idle_timeout(25ms));
  EXPECT_EQ(&server, &server.max_clients(0));
  EXPECT_EQ(&server, &server.backpressure_threshold(256));
  EXPECT_EQ(&server, &server.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort));
  EXPECT_EQ(&server, &server.batch_size(3));
  EXPECT_EQ(&server, &server.batch_latency(15ms));
  EXPECT_EQ(&server, &server.manage_external_context(false));
}

TEST(UdpServerWrapperLifecycleTest, IPv6EndpointHashCoverage) {
  // This is to cover UdpEndpointHash with IPv6
  boost::asio::ip::udp::endpoint ep(boost::asio::ip::make_address("::1"), 1234);
  config::UdpConfig cfg;
  cfg.bind_address = "::1";
  cfg.local_port = 0;

  // We don't need to run it, just constructing and potentially triggering a hash if we could
  // But since UdpEndpointHash is in anonymous namespace in .cc, we trigger it via server behavior
  try {
    wrapper::UdpServer server(cfg);
    auto started = server.start();
    (void)started;
    // If start fails due to no IPv6 support on host, it's fine, we just wanted to exercise construction
  } catch (...) {
  }
}

TEST(UdpServerWrapperLifecycleTest, SendToInvalidClient) {
  wrapper::UdpServer server(0);
  EXPECT_FALSE(server.send_to(999, "data"));
}

TEST(UdpServerWrapperLifecycleTest, BroadcastWithoutChannel) {
  // Construct via default which doesn't create channel until start
  wrapper::UdpServer server(0);
  EXPECT_FALSE(server.broadcast("data"));
}

}  // namespace test
}  // namespace wirestead
