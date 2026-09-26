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
#include <boost/asio/ip/udp.hpp>
#include <chrono>
#include <future>
#include <thread>

#include "wirestead/framer/line_framer.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wrapper_contract_test_utils.hpp"
using namespace wirestead;
using namespace wirestead::test::wrapper_support;
using namespace std::chrono_literals;
namespace {
struct Cleanup {
  std::function<void()> f;
  ~Cleanup() { f(); }
};
template <typename Harness>
auto connect(Harness& h) {
  if constexpr (requires { h.connect_client(); })
    return h.connect_client();
  else
    return h.start_sender();
}
template <typename S>
bool send(S& s, ClientId id, int api) {
  if (api == 0) return s.send_to(id, "reply").accepted();
  if (api == 1) return s.send_to_blocking(id, "reply").accepted();
  return s.send_to_line(id, "reply").accepted();
}
// A separate target keeps capacity unavailable while each source callback
// runs. Its watchdog releases the queue on a regression, avoiding deadlock.
template <typename H>
struct PressuredServer {
  H harness;
  decltype(std::declval<H&>().start_server()) server;
  decltype(connect(std::declval<H&>())) peer;
  ClientId id = 0;
  std::promise<ClientId> connected;
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  std::atomic<bool> held{false};
  bool start() {
    server = harness.start_server(1024);
    auto ready = connected.get_future();
    server->on_connect([this](const auto& ctx) { connected.set_value(ctx.client_id()); });
    peer = connect(harness);
    if (!peer->send("hello") || ready.wait_for(3s) != std::future_status::ready) return false;
    id = ready.get();
    server->on_connect(nullptr);
    server->on_backpressure([this](size_t queued) {
      if (queued < 1024 || held.exchange(true)) return;
      released.wait_for(5s);
    });
    if (!server->try_send_to(id, std::string(1024, 'x'))) return false;
    return test::TestUtils::waitForCondition([&] { return held.load(); }, 3000);
  }
  void check_rejected() {
    for (int api = 0; api < 3; ++api) EXPECT_FALSE(send(*server, id, api));
  }
  ~PressuredServer() {
    release.set_value();
    harness.stop_all();
  }
};
template <typename H>
class ServerCallbackBlockingSendTest : public ::testing::Test {};
using Servers = ::testing::Types<TcpServerLoopbackHarness, UdsServerLoopbackHarness, UdpServerLoopbackHarness>;
TYPED_TEST_SUITE(ServerCallbackBlockingSendTest, Servers);
TYPED_TEST(ServerCallbackBlockingSendTest, ConnectionDataMessageAndBatchDispatchesAcceptReadySends) {
  for (int kind = 0; kind < 6; ++kind) {
    SCOPED_TRACE(kind);
    PressuredServer<TypeParam> target;
    ASSERT_TRUE(target.start());
    TypeParam h;
    auto s = h.start_server();
    std::promise<void> delivered, disconnected;
    auto done = delivered.get_future(), closed = disconnected.get_future();
    std::atomic<bool> once{false}, closed_once{false};
    Cleanup cleanup{[&] { h.stop_all(); }};
    s->framer([] { return std::make_unique<framer::LineFramer>(); });
    s->batch_size(kind >= 4 ? 10 : 1).batch_latency(5ms);
    if constexpr (requires { h.start_sender(); }) s->idle_timeout(100ms);
    s->on_connect([&](const auto& ctx) {
      EXPECT_TRUE(wrapper::detail::in_data_callback());
      target.check_rejected();
      for (int api = 0; api < 3; ++api) EXPECT_TRUE(send(*s, ctx.client_id(), api));
    });
    auto end_handler = [&](const auto& ctx) {
      EXPECT_TRUE(wrapper::detail::in_data_callback());
      target.check_rejected();
      for (int api = 0; api < 3; ++api) EXPECT_FALSE(send(*s, ctx.client_id(), api));
      if (!closed_once.exchange(true)) disconnected.set_value();
    };
    if constexpr (requires { s->on_session_expired(end_handler); })
      s->on_session_expired(end_handler);
    else
      s->on_disconnect(end_handler);
    auto check = [&](ClientId id) {
      if (once.exchange(true)) return;
      EXPECT_TRUE(wrapper::detail::in_data_callback());
      target.check_rejected();
      for (int api = 0; api < 3; ++api) EXPECT_TRUE(send(*s, id, api));
      delivered.set_value();
    };
    if (kind == 0)
      s->on_data([&](const auto& ctx) { check(ctx.client_id()); });
    else if (kind == 1)
      s->on_message([&](const auto& ctx) { check(ctx.client_id()); });
    else if (kind == 2 || kind == 4)
      s->on_data_batch([&](const auto& batch) { check(batch.front().client_id()); });
    else
      s->on_message_batch([&](const auto& batch) { check(batch.front().client_id()); });
    auto peer = connect(h);
    ASSERT_TRUE(peer->send("hello\n"));
    EXPECT_EQ(done.wait_for(3s), std::future_status::ready);
    peer->stop();
    EXPECT_EQ(closed.wait_for(3s), std::future_status::ready);
    h.stop_all();
  }
}
// on_backpressure runs before the queued write can complete on this executor.
// Keep that callback parked after its nonwaiting sends, then verify an outside
// caller really waits until the callback returns and permits I/O to resume.
TYPED_TEST(ServerCallbackBlockingSendTest, PressureCallbackRejectsAndOutsideCallerWaits) {
  for (int api = 0; api < 3; ++api) {
    SCOPED_TRACE(api);
    TypeParam h;
    auto s = h.start_server(1024);
    std::promise<ClientId> connected;
    auto connection = connected.get_future();
    std::atomic<bool> connected_once{false}, pressure_once{false};
    std::promise<void> checked, release;
    auto checked_future = checked.get_future();
    auto release_future = release.get_future().share();
    Cleanup cleanup{[&] {
      try {
        release.set_value();
      } catch (const std::future_error&) {
      }
      h.stop_all();
    }};
    s->on_connect([&](const auto& ctx) {
      if (!connected_once.exchange(true)) connected.set_value(ctx.client_id());
    });
    auto peer = connect(h);
    ASSERT_TRUE(peer->send("hello"));
    ASSERT_EQ(connection.wait_for(3s), std::future_status::ready);
    const auto id = connection.get();
    s->on_backpressure([&](size_t queued) {
      if (queued < 1024 || pressure_once.exchange(true)) return;
      EXPECT_TRUE(wrapper::detail::in_data_callback());
      for (int inside = 0; inside < 3; ++inside) EXPECT_FALSE(send(*s, id, inside));
      checked.set_value();
      release_future.wait_for(3s);
    });
    ASSERT_TRUE(s->try_send_to(id, std::string(1024, 'x')));
    const auto status = checked_future.wait_for(1s);
    EXPECT_EQ(status, std::future_status::ready);
    if (status != std::future_status::ready) {
      release.set_value();
      s->stop();  // unblocks a regressed callback before joining its executor
      continue;
    }
    auto outside = std::async(std::launch::async, [&] { return send(*s, id, api); });
    EXPECT_EQ(outside.wait_for(100ms), std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(outside.get());
    for (int healthy = 0; healthy < 3; ++healthy) EXPECT_TRUE(send(*s, id, healthy));
    h.stop_all();
  }
}
template <typename S, typename H>
void error_callback_is_guarded() {
  PressuredServer<H> target;
  ASSERT_TRUE(target.start());
  auto c = std::make_shared<FakeChannel>();
  S s(c);
  bool called = false;
  s.on_error([&](const auto&) {
    called = true;
    EXPECT_TRUE(wrapper::detail::in_data_callback());
    target.check_rejected();
  });
  Cleanup cleanup{[&] { s.stop(); }};
  auto started = s.start();
  c->emit_state(base::LinkState::Error);
  EXPECT_TRUE(called);
  s.stop();
  EXPECT_FALSE(wrapper::detail::in_data_callback());
}
TEST(ServerCallbackErrorTest, Tcp) { error_callback_is_guarded<wrapper::TcpServer, TcpServerLoopbackHarness>(); }
TEST(ServerCallbackErrorTest, Uds) { error_callback_is_guarded<wrapper::UdsServer, UdsServerLoopbackHarness>(); }
TEST(ServerCallbackErrorTest, Udp) {
  PressuredServer<UdpServerLoopbackHarness> target;
  ASSERT_TRUE(target.start());
  boost::asio::io_context io;
  // Bind the exact address used below: Windows may allow a specific-address
  // bind alongside a wildcard bind on the same port.
  boost::asio::ip::udp::socket occupied(io, {boost::asio::ip::make_address("127.0.0.1"), 0});
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = occupied.local_endpoint().port();
  wrapper::UdpServer server(cfg);
  std::promise<void> called;
  auto done = called.get_future();
  std::atomic<bool> once{false};
  Cleanup cleanup{[&] { server.stop(); }};
  server.on_error([&](const auto&) {
    EXPECT_TRUE(wrapper::detail::in_data_callback());
    target.check_rejected();
    if (!once.exchange(true)) called.set_value();
  });
  auto started = server.start();
  EXPECT_EQ(done.wait_for(3s), std::future_status::ready);
  ASSERT_EQ(started.wait_for(3s), std::future_status::ready);
  EXPECT_FALSE(started.get());
}
}  // namespace
