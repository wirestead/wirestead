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

#include <boost/asio.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
thread_local std::function<void()> retiring_action;
void on_retiring() {
  if (retiring_action) retiring_action();
}

class ServerSendAccountingTest : public ::testing::TestWithParam<bool> {
 protected:
  net::io_context io;
  std::shared_ptr<interface::Channel> native;
  std::unique_ptr<wrapper::ServerInterface> server;
  std::vector<std::unique_ptr<net::ip::tcp::socket>> tcp_peers;
  std::vector<std::unique_ptr<net::local::stream_protocol::socket>> uds_peers;
  uint16_t port = 0;
  std::string path;
  void drain() {
    io.restart();
    io.poll();
  }
  bool wait(const std::function<bool()>& ready) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready() && std::chrono::steady_clock::now() < end) {
      io.restart();
      io.run_one_for(std::chrono::milliseconds(5));
    }
    return ready();
  }
  void SetUp() override {
    if (GetParam()) {
      config::TcpServerConfig cfg;
      port = cfg.port = test::TestUtils::getAvailableTestPort();
      native = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
      server = std::make_unique<wrapper::TcpServer>(native);
    } else {
      config::UdsServerConfig cfg;
      path = cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("acct").string();
      native = transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
      server = std::make_unique<wrapper::UdsServer>(native);
    }
    auto ready = server->start();
    ASSERT_TRUE(wait([&] { return ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready; }));
    ASSERT_TRUE(ready.get());
    for (int i = 0; i < 2; ++i) {
      if (GetParam()) {
        auto peer = std::make_unique<net::ip::tcp::socket>(io);
        peer->connect({net::ip::make_address("127.0.0.1"), port});
        tcp_peers.push_back(std::move(peer));
      } else {
        auto peer = std::make_unique<net::local::stream_protocol::socket>(io);
        peer->connect(net::local::stream_protocol::endpoint(path));
        uds_peers.push_back(std::move(peer));
      }
    }
    ASSERT_TRUE(wait([&] { return server->client_count() == 2; }));
  }
  void TearDown() override {
    transport::detail::g_server_sessions_retiring_hook = nullptr;
    retiring_action = {};
    if (server) {
      net::post(native->get_executor(), [&] { server->stop(); });
      drain();
      test::stop_with_context(native, io);
    }
  }
  wrapper::SendAccounting stats() { return *server->stats().send_accounting; }
  void close_peers() {
    boost::system::error_code ec;
    for (auto& p : tcp_peers) p->close(ec);
    for (auto& p : uds_peers) p->close(ec);
  }
  void stop_and_drain() {
    net::post(native->get_executor(), [&] { server->stop(); });
    drain();
  }
  void expect_conserved() {
    const auto s = stats();
    EXPECT_EQ(s.accepted.requests,
              s.written.requests + s.outstanding.requests + s.explicit_stop.discarded_before_write.requests +
                  s.explicit_stop.aborted_during_write.requests + s.connection_loss.discarded_before_write.requests +
                  s.connection_loss.aborted_during_write.requests + s.queue_pressure.discarded_before_write.requests +
                  s.queue_pressure.aborted_during_write.requests);
    EXPECT_EQ(s.accepted.bytes,
              s.written.bytes + s.outstanding.bytes + s.explicit_stop.discarded_before_write.bytes +
                  s.explicit_stop.aborted_during_write.bytes + s.connection_loss.discarded_before_write.bytes +
                  s.connection_loss.aborted_during_write.bytes + s.queue_pressure.discarded_before_write.bytes +
                  s.queue_pressure.aborted_during_write.bytes);
  }
};

TEST_P(ServerSendAccountingTest, FanoutCountsPerTargetAndSurvivesDisconnect) {
  const auto ids = server->connected_clients();
  ASSERT_EQ(ids.size(), 2u);
  ASSERT_TRUE(server->broadcast("abcdefgh"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 2; }));
  for (const auto id : ids) {
    const auto client = server->client_stats(id);
    ASSERT_TRUE(client && client->send_accounting);
    EXPECT_EQ(client->send_accounting->accepted.requests, 1u);
    EXPECT_EQ(client->send_accounting->written.bytes, 8u);
  }
  close_peers();
  ASSERT_TRUE(wait([&] { return !server->client_stats(ids[0]) && !server->client_stats(ids[1]); }));
  EXPECT_EQ(stats().accepted.requests, 2u);
  EXPECT_EQ(stats().written.bytes, 16u);
  EXPECT_EQ(stats().confirmed_written_bytes, 16u);
  stop_and_drain();
  EXPECT_EQ(stats().accepted.requests, 2u);
  EXPECT_EQ(server->stats().bytes_accepted, 16u);
  expect_conserved();
}
TEST_P(ServerSendAccountingTest, StopRetainsCompletedSessionsExactlyOnce) {
  ASSERT_TRUE(server->broadcast("abcdefgh"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 2; }));
  bool observed = false;
  retiring_action = [&] {
    observed = true;
    EXPECT_EQ(server->client_count(), 0u);
    EXPECT_EQ(stats().accepted.requests, 2u);
    EXPECT_EQ(stats().written.bytes, 16u);
    EXPECT_EQ(server->stats().bytes_accepted, 16u);
  };
  transport::detail::g_server_sessions_retiring_hook = on_retiring;
  stop_and_drain();
  ASSERT_TRUE(observed);
  EXPECT_EQ(stats().written.bytes, 16u);
  EXPECT_EQ(server->stats().bytes_accepted, 16u);
  stop_and_drain();
  EXPECT_EQ(stats().accepted.requests, 2u);
  EXPECT_EQ(server->stats().bytes_sent, 16u);
  expect_conserved();
}
TEST_P(ServerSendAccountingTest, StopBeforeEnqueuePreservesDiscardCauseThroughRetirement) {
  net::post(native->get_executor(), [&] {
    EXPECT_TRUE(server->broadcast("abcdefgh"));
    server->stop();
    EXPECT_EQ(stats().accepted.requests, 2u);
    EXPECT_EQ(stats().explicit_stop.discarded_before_write.requests, 2u);
    EXPECT_EQ(stats().outstanding.requests, 0u);
  });
  drain();
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 16u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.bytes, 0u);
  EXPECT_EQ(server->stats().bytes_accepted, 16u);
  expect_conserved();
}
TEST_P(ServerSendAccountingTest, ResetDuringRetirementDoesNotReintroduceOldTotals) {
  ASSERT_TRUE(server->broadcast("abcdefgh"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 2; }));
  bool observed = false;
  retiring_action = [&] {
    observed = true;
    EXPECT_EQ(stats().accepted.requests, 2u);
    server->reset_stats();
    EXPECT_EQ(stats().accepted.requests, 0u);
  };
  transport::detail::g_server_sessions_retiring_hook = on_retiring;
  stop_and_drain();
  ASSERT_TRUE(observed);
  EXPECT_EQ(stats().accepted.requests, 0u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(server->stats().bytes_accepted, 0u);
  EXPECT_EQ(server->stats().bytes_sent, 0u);
  expect_conserved();
}
TEST_P(ServerSendAccountingTest, ResetLiveAndClosedContributorsThenStop) {
  ASSERT_TRUE(server->broadcast("abcdefgh"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 2; }));
  boost::system::error_code ec;
  if (GetParam())
    tcp_peers[0]->close(ec);
  else
    uds_peers[0]->close(ec);
  ASSERT_TRUE(wait([&] { return server->client_count() == 1; }));
  drain();
  server->reset_stats();
  ASSERT_TRUE(server->broadcast("xyz"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 1; }));
  stop_and_drain();
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().written.bytes, 3u);
  EXPECT_EQ(server->stats().bytes_accepted, 3u);
  expect_conserved();
}
TEST_P(ServerSendAccountingTest, NativeRestartClearsRetainedAccounting) {
  ASSERT_TRUE(server->broadcast("abcdefgh"));
  ASSERT_TRUE(wait([&] { return stats().written.requests == 2; }));
  stop_and_drain();
  EXPECT_EQ(stats().accepted.requests, 2u);
  native->start();
  drain();
  EXPECT_EQ(stats().accepted.requests, 0u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(server->stats().bytes_accepted, 0u);
  expect_conserved();
}
INSTANTIATE_TEST_SUITE_P(TcpAndUds, ServerSendAccountingTest, ::testing::Bool());
}  // namespace
