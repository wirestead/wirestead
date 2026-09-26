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
#include <cstdint>
#include <future>
#include <memory>

#include "tcp_stop_with_context.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using udp = net::ip::udp;
using namespace std::chrono_literals;

std::atomic<int64_t> clock_ms{0};
class UdpCallbackSerializationTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  udp::socket peer{io, udp::endpoint(net::ip::address_v4::loopback(), 0)};
  std::shared_ptr<transport::UdpChannel> native;
  std::unique_ptr<wrapper::UdpClient> client;
  std::unique_ptr<wrapper::UdpServer> server;
  std::promise<void> entry;
  std::future<void> entry_ready{entry.get_future()};
  std::promise<void> release;
  std::shared_future<void> released{release.get_future().share()};
  std::future<void> worker;
  std::atomic<bool> entered{false}, active{false}, overlap{false};
  std::atomic<int> received{0}, batches{0}, disconnects{0};
  template <class F>
  bool pump(F done) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!done() && std::chrono::steady_clock::now() < deadline) {
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return done();
  }
  void hold() {
    active = true;
    entered = true;
    entry.set_value();
    released.wait();
    active = false;
  }
  void open(bool as_server) {
    config::UdpConfig cfg;
    cfg.bind_address = "127.0.0.1";
    udp::socket reservation(io, udp::endpoint(net::ip::address_v4::loopback(), 0));
    cfg.local_port = reservation.local_endpoint().port();
    reservation.close();
    if (!as_server) {
      cfg.remote_address = "127.0.0.1";
      cfg.remote_port = peer.local_endpoint().port();
    }
    native = transport::UdpChannel::create(cfg, io);
    if (as_server)
      server = std::make_unique<wrapper::UdpServer>(native);
    else
      client = std::make_unique<wrapper::UdpClient>(native);
  }
  bool start() {
    auto ready = server ? server->start() : client->start();
    return pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }) && ready.get();
  }
  void send() { peer.send_to(net::buffer("payload\n", 8), native->local_endpoint()); }
  bool run_until_held() {
    if (io.stopped()) io.restart();
    worker = std::async(std::launch::async, [&] { pump([&] { return entered.load(); }); });
    return entry_ready.wait_for(5s) == std::future_status::ready;
  }
  void unblock() {
    try {
      release.set_value();
    } catch (const std::future_error&) {
    }
    if (worker.valid()) worker.get();
  }
  void TearDown() override {
    unblock();
    if (server) test::stop_wrapper_with_context(*server, io);
    if (client) test::stop_wrapper_with_context(*client, io);
    transport::detail::g_udp_session_clock_hook = nullptr;
  }
};

TEST_P(UdpCallbackSerializationTest, LatencyBatchCannotOverlapReceiveOnAnotherRunner) {
  const bool as_server = GetParam() >= 2;
  const bool message_batch = GetParam() % 2;
  open(as_server);
  auto configure = [&](auto& target) {
    target.batch_size(10).batch_latency(100ms);
    auto batch = [&](const auto& contexts) {
      if (active) overlap = true;
      EXPECT_FALSE(contexts.empty());
      ++batches;
    };
    if (message_batch) {
      target.on_message_batch(batch);
      target.on_data([&](const auto&) {
        if (++received == 2) hold();
      });
    } else {
      target.on_data_batch(batch);
      target.on_message([&](const auto&) { hold(); });
    }
  };
  if (server) {
    server->framer([] { return std::make_unique<framer::LineFramer>(); });
    configure(*server);
  } else {
    client->framer(std::make_unique<framer::LineFramer>());
    configure(*client);
  }
  ASSERT_TRUE(start());
  send();
  if (message_batch) {
    ASSERT_TRUE(pump([&] { return received == 1; }));
    send();
  }
  ASSERT_TRUE(run_until_held());
  // The first runner is inside a receive callback. A second runner gives the
  // already-armed timer time to expire; its handler must wait for the strand.
  io.run_for(200ms);
  EXPECT_EQ(batches.load(), 0);
  EXPECT_FALSE(overlap);
  unblock();
  ASSERT_TRUE(pump([&] { return batches > 0; }));
  EXPECT_FALSE(overlap);
}

class UdpExpirySerializationTest : public UdpCallbackSerializationTest {};
TEST_F(UdpExpirySerializationTest, ExpiryCannotOverlapReceiveOnAnotherRunner) {
  open(true);
  clock_ms = 0;
  transport::detail::g_udp_session_clock_hook =
      +[] { return std::chrono::steady_clock::time_point(std::chrono::milliseconds(clock_ms.load())); };
  server->idle_timeout(100ms);
  server->on_data([&](const auto&) { hold(); });
  server->on_disconnect([&](const auto&) {
    if (active) overlap = true;
    ++disconnects;
  });
  ASSERT_TRUE(start());
  send();
  ASSERT_TRUE(run_until_held());
  clock_ms = 200;
  io.run_for(150ms);
  EXPECT_EQ(disconnects.load(), 0);
  EXPECT_EQ(server->client_count(), 1u);
  EXPECT_FALSE(overlap);
  unblock();
  ASSERT_TRUE(pump([&] { return disconnects == 1; }));
  EXPECT_FALSE(overlap);
  EXPECT_EQ(server->client_count(), 0u);
}
INSTANTIATE_TEST_SUITE_P(ClientAndServerBatches, UdpCallbackSerializationTest, ::testing::Range(0, 4));
}  // namespace
