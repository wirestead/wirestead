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
class ServerExecutorPolicyTest : public ::testing::TestWithParam<int> {
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
        send_peer(i, "register\n");
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
  void on_session_end(wrapper::ServerInterface::ConnectionHandler handler) {
    if (GetParam() == 2)
      static_cast<wrapper::UdpServer&>(*server).on_session_expired(std::move(handler));
    else
      server->on_disconnect(std::move(handler));
  }
  void TearDown() override {
    unblock();
    if (server) test::stop_wrapper_with_context(*server, *io);
    if (!path.empty()) test::TestUtils::removeFileIfExists(path);
  }
};
TEST_P(ServerExecutorPolicyTest, CountAndTimerBatchesStayWithTheirSession) {
  batch_options(2, 1h);
  int chunks = 0;
  std::vector<std::vector<wrapper::MessageContext>> batches;
  server->on_data([&](const auto&) { ++chunks; });
  server->on_message_batch([&](const auto& batch) { batches.push_back(batch); });
  send_peer(0, "a\n");
  ASSERT_TRUE(pump([&] { return chunks == 1; }));
  send_peer(1, "b\n");
  ASSERT_TRUE(pump([&] { return chunks == 2; }));
  EXPECT_TRUE(batches.empty());  // two sessions must not satisfy one batch count.
  send_peer(0, "c\n");
  ASSERT_TRUE(pump([&] { return chunks == 3; }));
  ASSERT_EQ(batches.size(), 1u);
  ASSERT_EQ(batches[0].size(), 2u);
  EXPECT_EQ(batches[0][0].client_id(), ids[0]);
  EXPECT_EQ(batches[0][1].client_id(), ids[0]);
  EXPECT_EQ(batches[0][0].data_as_string(), "a");
  EXPECT_EQ(batches[0][1].data_as_string(), "c");
}
TEST_P(ServerExecutorPolicyTest, BatchTimerWaitsForSameSessionMessageCallback) {
  batch_options(100, 10ms);
  std::promise<void> entered;
  auto entry = entered.get_future();
  std::atomic<bool> finished{false}, overlap{false};
  std::atomic<int> batches{0};
  server->on_data_batch([&](const auto&) {
    if (!finished) overlap = true;
    ++batches;
  });
  server->on_message([&](const auto&) {
    entered.set_value();
    released.wait();
    finished = true;
  });
  send_peer(0, "held\n");
  io->restart();
  runner = std::async(std::launch::async, [&] { io->run_for(200ms); });
  const auto status = entry.wait_for(5s);
  EXPECT_EQ(status, std::future_status::ready);
  if (status == std::future_status::ready) {
    io->run_for(50ms);
    EXPECT_EQ(batches, 0);
    EXPECT_FALSE(overlap);
  }
  unblock();
  EXPECT_TRUE(pump([&] { return batches > 0; }));
  EXPECT_FALSE(overlap);
}
TEST_P(ServerExecutorPolicyTest, StopInDataBatchSuppressesFollowingMessageCallback) {
  batch_options(1);
  int data = 0, messages = 0;
  server->on_data_batch([&](const auto&) {
    ++data;
    server->stop();
  });
  server->on_message([&](const auto&) { ++messages; });
  send_peer(0, "stop\n");
  ASSERT_TRUE(pump([&] { return data == 1; }));
  test::stop_wrapper_with_context(*server, *io);
  EXPECT_EQ(messages, 0);
}
TEST_P(ServerExecutorPolicyTest, SessionEndFlushesPartialBatchBeforeNotification) {
  if (GetParam() == 2) static_cast<wrapper::UdpServer&>(*server).idle_timeout(50ms);
  batch_options(100, 1h);
  int chunks = 0;
  std::vector<std::string> events;
  server->on_data([&](const auto&) { ++chunks; });
  server->on_message_batch([&](const auto& batch) {
    for (const auto& message : batch) events.push_back(message.data_as_string());
  });
  on_session_end([&](const auto& ctx) {
    if (ctx.client_id() == ids[0]) events.push_back("disconnect");
  });
  send_peer(0, "tail\n");
  ASSERT_TRUE(pump([&] { return chunks == 1; }));
  if (GetParam() == 0)
    tcp[0]->close();
  else if (GetParam() == 1)
    uds[0]->close();
  ASSERT_TRUE(pump([&] { return events.size() >= 2; }));
  EXPECT_EQ(events, (std::vector<std::string>{"tail", "disconnect"}));
  on_session_end({});
}

TEST_P(ServerExecutorPolicyTest, NativeStopWaitsForSessionEndCallback) {
  if (GetParam() == 2) static_cast<wrapper::UdpServer&>(*server).idle_timeout(50ms);
  std::promise<void> entered;
  auto entry = entered.get_future();
  std::atomic<bool> notified{false}, callback_finished{false};
  on_session_end([&](const auto& ctx) {
    if (ctx.client_id() != ids[0] || notified.exchange(true)) return;
    if (GetParam() != 2) {
      EXPECT_EQ(server->client_count(), 1u);
      EXPECT_EQ(server->connected_clients(), (std::vector<ClientId>{ids[1]}));
    }
    entered.set_value();
    released.wait();
    callback_finished = true;
  });
  if (GetParam() == 0)
    tcp[0]->close();
  else if (GetParam() == 1)
    uds[0]->close();
  auto work = net::make_work_guard(*io);
  io->restart();
  runner = std::async(std::launch::async, [&] { io->run(); });
  EXPECT_EQ(entry.wait_for(5s), std::future_status::ready);
  auto stopper = std::async(std::launch::async, [&] { native->stop(); });
  EXPECT_EQ(stopper.wait_for(50ms), std::future_status::timeout);
  release.set_value();
  EXPECT_EQ(stopper.wait_for(5s), std::future_status::ready);
  server->stop();  // Also cancel wrapper-owned UDP timers before joining the runner.
  work.reset();
  unblock();
  stopper.get();
  EXPECT_TRUE(callback_finished);
}

TEST_P(ServerExecutorPolicyTest, OrdinaryTaskRejectsHardLimitRaceWithoutRetry) {
  // Fill reservations on the executor itself, before posted enqueues can drain.
  std::promise<void> done;
  auto finished = done.get_future();
  auto work = net::make_work_guard(*io);
  io->restart();
  net::post(*io, [&] {
    EXPECT_FALSE(wrapper::detail::in_data_callback());
    auto limit = GetParam() == 0   ? std::static_pointer_cast<transport::TcpServer>(native)->write_queue_limit(ids[0])
                 : GetParam() == 1 ? std::static_pointer_cast<transport::UdsServer>(native)->write_queue_limit(ids[0])
                                   : native->write_queue_limit();
    if (limit) {
      // UDP datagrams must also fit the wire-size limit.
      const size_t piece = GetParam() == 2 ? 1024 : *limit;
      for (size_t total = 0; total + piece <= *limit; total += piece)
        EXPECT_TRUE(server->send_to_blocking(ids[0], std::string(piece, 'p')));
      auto result = server->send_to_blocking(ids[0], "overflow");
      EXPECT_FALSE(result.accepted());
      if (!result.accepted()) {
        EXPECT_EQ(result.reason(), wrapper::SendRejection::WouldBlock);
      }
    } else
      ADD_FAILURE() << "missing hard limit";
    done.set_value();
  });
  runner = std::async(std::launch::async, [&] { io->run(); });
  EXPECT_EQ(finished.wait_for(2s), std::future_status::ready);
  // A failing old implementation is released by stop from outside its context.
  auto stopper = std::async(std::launch::async, [&] { server->stop(); });
  work.reset();
  unblock();
  stopper.get();
}
INSTANTIATE_TEST_SUITE_P(TcpUdsUdp, ServerExecutorPolicyTest, ::testing::Values(0, 1, 2));
}  // namespace
