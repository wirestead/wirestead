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
#include <future>
#include <mutex>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using namespace std::chrono_literals;
class ReceiveMemoryServerTest : public ::testing::TestWithParam<int> {
 protected:
  std::shared_ptr<net::io_context> io = std::make_shared<net::io_context>();
  std::shared_ptr<interface::Channel> native;
  std::unique_ptr<wrapper::ServerInterface> server;
  std::vector<std::unique_ptr<net::ip::tcp::socket>> tcp;
  std::vector<std::unique_ptr<net::local::stream_protocol::socket>> uds;
  std::vector<std::unique_ptr<net::ip::udp::socket>> udp;
  std::vector<ClientId> ids;
  std::string path;
  uint16_t port{};
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  std::future<void> runner;
  template <class F>
  bool pump(F done) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!done() && std::chrono::steady_clock::now() < deadline) {
      if (io->stopped()) io->restart();
      io->run_one_for(5ms);
    }
    return done();
  }
  void batch_options(size_t size, std::chrono::milliseconds latency = 1ms) {
    if (GetParam() == 0)
      static_cast<wrapper::TcpServer&>(*server).batch_size(size).batch_latency(latency);
    else if (GetParam() == 1)
      static_cast<wrapper::UdsServer&>(*server).batch_size(size).batch_latency(latency);
    else
      static_cast<wrapper::UdpServer&>(*server).batch_size(size).batch_latency(latency);
  }
  void set_limits(wrapper::ReceiveLimits limits) {
    if (GetParam() == 0)
      static_cast<wrapper::TcpServer&>(*server).receive_limits(limits);
    else if (GetParam() == 1)
      static_cast<wrapper::UdsServer&>(*server).receive_limits(limits);
    else
      static_cast<wrapper::UdpServer&>(*server).receive_limits(limits);
  }
  wrapper::ReceiveMemoryStats receive_stats() {
    if (GetParam() == 0) return static_cast<wrapper::TcpServer&>(*server).receive_stats();
    if (GetParam() == 1) return static_cast<wrapper::UdsServer&>(*server).receive_stats();
    return static_cast<wrapper::UdpServer&>(*server).receive_stats();
  }
  void unblock() {
    try {
      release.set_value();
    } catch (const std::future_error&) {
    }
    if (runner.valid()) runner.get();
  }
  void SetUp() override {
    port = test::TestUtils::getAvailableTestPort();
    if (GetParam() == 0) {
      config::TcpServerConfig cfg;
      cfg.port = port;
      cfg.backpressure_threshold = 1024;
      native = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(*io), *io);
      server = std::make_unique<wrapper::TcpServer>(native);
    } else if (GetParam() == 1) {
      config::UdsServerConfig cfg;
      path = cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("executor-policy").string();
      cfg.backpressure_threshold = 1024;
      native = transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(*io), *io);
      server = std::make_unique<wrapper::UdsServer>(native);
    } else {
      config::UdpConfig cfg;
      cfg.local_port = port;
      cfg.bind_address = "127.0.0.1";
      cfg.backpressure_threshold = 1024;
      auto channel = transport::UdpChannel::create(cfg, *io);
      native = channel;
      server = std::make_unique<wrapper::UdpServer>(native);
    }
    set_limits({4096, 64, 8});
    batch_options(100, 1h);
    server->framer([] { return std::make_unique<framer::LineFramer>(); });
    server->on_connect([&](const auto& ctx) { ids.push_back(ctx.client_id()); });
    auto ready = server->start();
    ASSERT_TRUE(pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
    ASSERT_TRUE(ready.get());
    for (int i = 0; i < 2; ++i) {
      if (GetParam() == 0) {
        auto peer = std::make_unique<net::ip::tcp::socket>(*io);
        peer->connect({net::ip::make_address("127.0.0.1"), port});
        tcp.push_back(std::move(peer));
      } else if (GetParam() == 1) {
        auto peer = std::make_unique<net::local::stream_protocol::socket>(*io);
        peer->connect(net::local::stream_protocol::endpoint(path));
        uds.push_back(std::move(peer));
      } else {
        auto peer = std::make_unique<net::ip::udp::socket>(*io, net::ip::udp::endpoint(net::ip::udp::v4(), 0));
        udp.push_back(std::move(peer));
        send_peer(i, "\n");
      }
      ASSERT_TRUE(pump([&] { return ids.size() == static_cast<size_t>(i + 1); }));
    }
    // Drain registration before tests install their receive callbacks.
    io->restart();
    io->poll();
  }
  void send_peer(int i, std::string_view payload) {
    auto buffer = net::buffer(payload.data(), payload.size());
    if (GetParam() == 0)
      net::write(*tcp.at(i), buffer);
    else if (GetParam() == 1)
      net::write(*uds.at(i), buffer);
    else
      udp.at(i)->send_to(buffer, {net::ip::make_address("127.0.0.1"), port});
  }
  void TearDown() override {
    unblock();
    if (server) test::stop_wrapper_with_context(*server, *io);
    if (!path.empty()) test::TestUtils::removeFileIfExists(path);
  }
};

TEST_P(ReceiveMemoryServerTest, OverflowPreservesOtherSessionAndUdpWaitingBatch) {
  std::vector<std::vector<wrapper::MessageContext>> batches;
  server->on_data_batch([&](const auto& batch) { batches.push_back(batch); });
  send_peer(0, "a");
  ASSERT_TRUE(pump([&] { return receive_stats().reserved_bytes > 100; }));
  auto first = receive_stats().reserved_bytes;
  send_peer(1, "b");
  ASSERT_TRUE(pump([&] { return receive_stats().reserved_bytes > first + 100; }));
  auto both = receive_stats().reserved_bytes;
  server->reset_stats();
  EXPECT_EQ(receive_stats().reserved_bytes, both);
  send_peer(0, std::string(5000, 'x'));
  ASSERT_TRUE(pump([&] { return receive_stats().overflow_events == 1; }));
  EXPECT_EQ(receive_stats().overflow_by_reason[0], 1u);
  EXPECT_TRUE(batches.empty());
  if (GetParam() == 2)
    EXPECT_EQ(receive_stats().reserved_bytes, both);
  else
    EXPECT_LT(receive_stats().reserved_bytes, both);
  batch_options(1, 1h);
  send_peer(1, "c");
  ASSERT_TRUE(pump([&] { return !batches.empty(); }));
  ASSERT_EQ(batches[0].size(), 2u);
  EXPECT_EQ(batches[0][0].client_id(), ids[1]);
  EXPECT_EQ(batches[0][0].data(), "b");
  EXPECT_EQ(batches[0][1].data(), "c");
  if (GetParam() == 2) {
    send_peer(0, "d");
    ASSERT_TRUE(pump([&] { return batches.size() == 2; }));
    EXPECT_EQ(batches[1][0].data(), "a");
    EXPECT_EQ(batches[1][1].data(), "d");
  }
}

TEST_P(ReceiveMemoryServerTest, FrameOverflowClosesOnlyStreamPeerOrDropsUdpInput) {
  std::vector<std::string> messages;
  size_t raw = 0;
  std::vector<ClientId> lost;
  server->on_data([&](const auto&) { ++raw; });
  server->on_message([&](const auto& ctx) { messages.push_back(ctx.data_as_string()); });
  server->on_disconnect([&](const auto& ctx) { lost.push_back(ctx.client_id()); });
  send_peer(0, "a");
  ASSERT_TRUE(pump([&] { return raw == 1; }));
  send_peer(0, std::string(65, 'x'));
  ASSERT_TRUE(pump([&] { return receive_stats().overflow_events == 1; }));
  EXPECT_EQ(receive_stats().overflow_by_reason[1], 1u);
  EXPECT_EQ(raw, 1u);
  EXPECT_TRUE(messages.empty());
  if (GetParam() != 2) {
    ASSERT_TRUE(pump([&] { return lost.size() == 1; }));
    EXPECT_EQ(lost[0], ids[0]);
  } else {
    EXPECT_TRUE(lost.empty());
    send_peer(0, "\n");
    ASSERT_TRUE(pump([&] { return messages.size() == 1; }));
    EXPECT_EQ(messages[0], "a");
  }
  send_peer(1, "ok\n");
  ASSERT_TRUE(pump([&] { return !messages.empty() && messages.back() == "ok"; }));
}

TEST_P(ReceiveMemoryServerTest, StopReleasesQueuedReceiveStorage) {
  server->on_data_batch([](const auto&) {});
  send_peer(0, "held");
  ASSERT_TRUE(pump([&] { return receive_stats().reserved_bytes > 100; }));
  test::stop_wrapper_with_context(*server, *io);
  EXPECT_EQ(receive_stats().reserved_bytes, 0u);
  EXPECT_EQ(receive_stats().sessions, 0u);
}

TEST_P(ReceiveMemoryServerTest, LimitChangesRequireStopAndValidateBeforeApply) {
  const auto before = receive_stats().reserved_bytes;
  EXPECT_THROW(set_limits({0, 1, 1}), std::invalid_argument);
  EXPECT_THROW(set_limits({4096, 64, 8}), std::logic_error);
  EXPECT_EQ(receive_stats().reserved_bytes, before);
}

TEST_P(ReceiveMemoryServerTest, CallbackStopKeepsDeliveredBatchChargedUntilReturn) {
  batch_options(1, 1h);
  bool done = false, retained = false;
  server->on_data_batch([&](const auto& batch) {
    server->stop();
    retained = receive_stats().reserved_bytes >= batch[0].data().size();
    done = true;
  });
  send_peer(0, "held");
  ASSERT_TRUE(pump([&] { return done; }));
  EXPECT_TRUE(retained);
  test::stop_wrapper_with_context(*server, *io);
  EXPECT_EQ(receive_stats().reserved_bytes, 0u);
}

TEST_P(ReceiveMemoryServerTest, SessionLimitRejectsOnlyNewPeerAndRecordsCause) {
  test::stop_wrapper_with_context(*server, *io);
  ids.clear();
  tcp.clear();
  uds.clear();
  udp.clear();
  set_limits({4096, 64, 1});
  auto ready = server->start();
  ASSERT_TRUE(pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  for (int i = 0; i < 2; ++i) {
    if (GetParam() == 0) {
      auto peer = std::make_unique<net::ip::tcp::socket>(*io);
      peer->connect({net::ip::make_address("127.0.0.1"), port});
      tcp.push_back(std::move(peer));
    } else if (GetParam() == 1) {
      auto peer = std::make_unique<net::local::stream_protocol::socket>(*io);
      peer->connect(net::local::stream_protocol::endpoint(path));
      uds.push_back(std::move(peer));
    } else {
      udp.push_back(std::make_unique<net::ip::udp::socket>(*io, net::ip::udp::endpoint(net::ip::udp::v4(), 0)));
      send_peer(i, "\n");
    }
    if (i == 0) {
      ASSERT_TRUE(pump([&] { return ids.size() == 1; }));
    } else {
      ASSERT_TRUE(pump([&] { return receive_stats().overflow_by_reason[2] == 1; }));
    }
  }
  EXPECT_EQ(ids.size(), 1u);
  EXPECT_EQ(receive_stats().sessions, 1u);
  std::string message;
  server->on_message([&](const auto& ctx) { message = ctx.data_as_string(); });
  send_peer(0, "still alive\n");
  ASSERT_TRUE(pump([&] { return message == "still alive"; }));
}
INSTANTIATE_TEST_SUITE_P(AllServers, ReceiveMemoryServerTest, ::testing::Values(0, 1, 2));
}  // namespace
