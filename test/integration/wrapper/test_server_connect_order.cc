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
#include <tuple>

#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using namespace std::chrono_literals;
class ServerConnectOrderTest : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  net::io_context io;
  std::shared_ptr<interface::Channel> native;
  std::shared_ptr<transport::TcpServer> tcp;
  std::shared_ptr<transport::UdsServer> uds;
  std::unique_ptr<wrapper::ServerInterface> wrapper;
  std::unique_ptr<net::ip::tcp::socket> tcp_peer;
  std::unique_ptr<net::local::stream_protocol::socket> uds_peer;
  std::string path;
  uint16_t port{};
  std::promise<void> entered, release;
  std::future<void> entered_future{entered.get_future()};
  std::shared_future<void> released{release.get_future().share()};
  std::future<void> runner;
  std::atomic<ClientId> id{0};
  std::atomic<bool> connect_finished{false}, overlap{false}, stop_in_connect{false};
  std::atomic<int> data{0}, messages{0}, pressure{0};
  template <class F>
  bool pump(F done) {
    const auto end = std::chrono::steady_clock::now() + 5s;
    while (!done() && std::chrono::steady_clock::now() < end) {
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return done();
  }
  void connected(ClientId value) {
    id = value;
    entered.set_value();
    released.wait();
    if (stop_in_connect) {
      if (wrapper)
        wrapper->stop();
      else
        native->stop();
    }
    connect_finished = true;
  }
  void receive() {
    if (!connect_finished) overlap = true;
    ++data;
  }
  void pressured(size_t queued) {
    if (!queued) return;
    if (!connect_finished) overlap = true;
    ++pressure;
  }
  void SetUp() override {
    if (std::get<0>(GetParam())) {
      config::TcpServerConfig cfg;
      port = cfg.port = test::TestUtils::getAvailableTestPort();
      cfg.backpressure_threshold = 1024;
      tcp = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
      native = tcp;
      if (std::get<1>(GetParam())) wrapper = std::make_unique<wrapper::TcpServer>(native);
    } else {
      config::UdsServerConfig cfg;
      path = cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("connect-order").string();
      cfg.backpressure_threshold = 1024;
      uds = transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
      native = uds;
      if (std::get<1>(GetParam())) wrapper = std::make_unique<wrapper::UdsServer>(native);
    }
    if (wrapper) {
      wrapper->framer([] { return std::make_unique<framer::LineFramer>(); });
      wrapper->on_connect([&](const auto& ctx) { connected(ctx.client_id()); });
      wrapper->on_data([&](const auto&) { receive(); });
      wrapper->on_message([&](const auto&) {
        if (!connect_finished) overlap = true;
        ++messages;
      });
      wrapper->on_backpressure([&](size_t queued) { pressured(queued); });
      auto ready = wrapper->start();
      ASSERT_TRUE(pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
      ASSERT_TRUE(ready.get());
    } else {
      auto configure = [&](auto& server) {
        server.on_multi_connect([&](ClientId value, const std::string&) { connected(value); });
        server.on_multi_data([&](ClientId, auto) { receive(); });
        server.on_backpressure([&](size_t queued) { pressured(queued); });
      };
      if (tcp)
        configure(*tcp);
      else
        configure(*uds);
      bool listening = false;
      native->on_state([&](base::LinkState state) {
        if (state == base::LinkState::Listening) listening = true;
      });
      native->start();
      ASSERT_TRUE(pump([&] { return listening; }));
      native->on_state({});
    }
  }
  bool connect_and_hold() {
    if (tcp) {
      tcp_peer = std::make_unique<net::ip::tcp::socket>(io);
      tcp_peer->connect({net::ip::make_address("127.0.0.1"), port});
      net::write(*tcp_peer, net::buffer("first\n", 6));
    } else {
      uds_peer = std::make_unique<net::local::stream_protocol::socket>(io);
      uds_peer->connect(net::local::stream_protocol::endpoint(path));
      net::write(*uds_peer, net::buffer("first\n", 6));
    }
    if (io.stopped()) io.restart();
    runner = std::async(std::launch::async, [&] { pump([&] { return connect_finished.load(); }); });
    return entered_future.wait_for(5s) == std::future_status::ready;
  }
  void unblock() {
    try {
      release.set_value();
    } catch (const std::future_error&) {
    }
    if (runner.valid()) runner.get();
  }
  void stop() {
    if (wrapper)
      test::stop_wrapper_with_context(*wrapper, io);
    else
      test::stop_with_context(native, io);
  }
  void TearDown() override {
    unblock();
    stop();
    if (!path.empty()) test::TestUtils::removeFileIfExists(path);
  }
};
TEST_P(ServerConnectOrderTest, ConnectFinishesBeforeImmediateReceiveAndPressure) {
  ASSERT_TRUE(connect_and_hold());
  const std::string payload(2048, 'p');
  // The published session is alive and accepts sends while connect is held,
  // but its queued work must not invoke a callback ahead of connect's return.
  if (wrapper)
    ASSERT_TRUE(wrapper->send_to_blocking(id, payload));
  else if (tcp)
    ASSERT_TRUE(tcp->send_to_client(id, payload));
  else
    ASSERT_TRUE(uds->send_to_client(id, payload));
  io.run_for(100ms);
  EXPECT_EQ(data.load(), 0);
  EXPECT_EQ(pressure.load(), 0);
  EXPECT_FALSE(overlap);
  unblock();
  ASSERT_TRUE(pump([&] { return data > 0 && pressure > 0; }));
  if (wrapper) {
    EXPECT_EQ(messages.load(), 1);
  }
  EXPECT_FALSE(overlap);
}
TEST_P(ServerConnectOrderTest, StopInConnectSuppressesAlreadyAvailableReceive) {
  stop_in_connect = true;
  ASSERT_TRUE(connect_and_hold());
  io.run_for(100ms);
  EXPECT_EQ(data.load(), 0);
  unblock();
  stop();
  EXPECT_EQ(data.load(), 0);
  EXPECT_EQ(messages.load(), 0);
  EXPECT_FALSE(overlap);
}
TEST_P(ServerConnectOrderTest, ExternalStopWaitsForConnectCompletion) {
  ASSERT_TRUE(connect_and_hold());
  std::promise<void> requested;
  auto request = requested.get_future();
  auto stopper = std::async(std::launch::async, [&] {
    requested.set_value();
    if (wrapper)
      wrapper->stop();
    else
      native->stop();
  });
  EXPECT_EQ(request.wait_for(5s), std::future_status::ready);
  io.run_for(100ms);
  EXPECT_EQ(stopper.wait_for(0ms), std::future_status::timeout);
  unblock();
  EXPECT_TRUE(pump([&] { return stopper.wait_for(0ms) == std::future_status::ready; }));
  stopper.get();
  EXPECT_TRUE(connect_finished);
  EXPECT_EQ(data.load(), 0);
  EXPECT_EQ(messages.load(), 0);
}

INSTANTIATE_TEST_SUITE_P(TcpUdsNativeWrapper, ServerConnectOrderTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));
}  // namespace
