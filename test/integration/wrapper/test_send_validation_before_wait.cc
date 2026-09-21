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
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "wirestead/interface/channel.hpp"
#include "wrapper_contract_test_utils.hpp"
using namespace wirestead;
using namespace wirestead::test::wrapper_support;
using namespace std::chrono_literals;
namespace {
constexpr size_t kMax = base::constants::MAX_BUFFER_SIZE;
class ValidationChannel : public interface::Channel {
 public:
  std::atomic<bool> pressure{true}, ready{false};
  mutable std::atomic<int> probes{0};
  std::atomic<int> failures{0};
  boost::asio::io_context io;
  OnState state;
  void start() override { ready = true; }
  void stop() override { ready = false; }
  bool is_connected() const override { return ready; }
  bool is_backpressure_active() const override {
    ++probes;
    return pressure;
  }
  boost::asio::any_io_executor get_executor() override { return io.get_executor(); }
  bool validate(size_t size) {
    if (!ready || size == 0 || size > kMax) {
      ++failures;
      return false;
    }
    return true;
  }
  bool async_write_copy(memory::ConstByteSpan b) override { return validate(b.size()); }
  bool async_write_move(std::vector<uint8_t>&& b) override { return validate(b.size()); }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> b) override {
    return validate(b ? b->size() : 0);
  }
  bool async_try_write_copy(memory::ConstByteSpan b) override { return async_write_copy(b); }
  bool async_try_write_move(std::vector<uint8_t>&& b) override { return async_write_move(std::move(b)); }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> b) override {
    return async_write_shared(std::move(b));
  }
  void on_state(OnState cb) override { state = std::move(cb); }
  void on_bytes(OnBytes) override {}
  void on_backpressure(OnBackpressure) override {}
};
template <typename W>
class SendValidationBeforeWaitTest : public ::testing::Test {};
using Clients = ::testing::Types<wrapper::TcpClient, wrapper::UdsClient, wrapper::UdpClient, wrapper::Serial>;
TYPED_TEST_SUITE(SendValidationBeforeWaitTest, Clients);
TYPED_TEST(SendValidationBeforeWaitTest, InvalidPayloadsReachRejectionWithoutWaiting) {
  auto c = std::make_shared<ValidationChannel>();
  TypeParam w(c);
  w.backpressure_strategy(base::constants::BackpressureStrategy::Reliable);
  auto started = w.start();
  c->state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());
  auto oversized = std::make_shared<const std::vector<uint8_t>>(kMax + 1, 0);
  const std::string_view view(reinterpret_cast<const char*>(oversized->data()), oversized->size());
  for (int which = 0; which < 11; ++which) {
    SCOPED_TRACE(which);
    c->pressure = true;
    c->failures = 0;
    auto result = std::async(std::launch::async, [&] {
      switch (which) {
        case 0:
          return w.send("");
        case 1:
          return w.send_blocking("");
        case 2:
          return w.send_move({});
        case 3:
          return w.send_shared(nullptr);
        case 4:
          return w.send_shared(std::make_shared<const std::vector<uint8_t>>());
        case 5:
          return w.send(view);
        case 6:
          return w.send_blocking(view);
        case 7:
          return w.send_line(view.substr(0, kMax));
        case 8:
          return w.send_line_blocking(view.substr(0, kMax));
        case 9:
          return w.send_move(std::vector<uint8_t>(oversized->begin(), oversized->end()));
        default:
          return w.send_shared(oversized);
      }
    });
    // Releasing after the bound also makes the old implementation terminate.
    EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
    c->pressure = false;
    EXPECT_FALSE(result.get());
    if (which != 3 && which != 4) {
      EXPECT_GT(c->failures, 0) << "transport failure accounting was bypassed";
    }
  }
  w.stop();
}
TYPED_TEST(SendValidationBeforeWaitTest, ValidAndBoundaryPayloadsStillWait) {
  auto c = std::make_shared<ValidationChannel>();
  TypeParam w(c);
  w.backpressure_strategy(base::constants::BackpressureStrategy::Reliable);
  auto started = w.start();
  c->state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());
  auto maximum = std::make_shared<const std::vector<uint8_t>>(kMax, 0);
  const std::string_view view(reinterpret_cast<const char*>(maximum->data()), maximum->size());
  for (int which = 0; which < 8; ++which) {
    SCOPED_TRACE(which);
    c->pressure = true;
    c->probes = 0;
    auto result = std::async(std::launch::async, [&] {
      switch (which) {
        case 0:
          return w.send("valid");
        case 1:
          return w.send_blocking(view);
        case 2:
          return w.send_line("");
        case 3:
          return w.send_line_blocking("");
        case 4:
          return w.send_move(std::vector<uint8_t>{1});
        case 5:
          return w.send_shared(maximum);
        case 6:
          return w.send_line(view.substr(0, kMax - 1));
        default:
          return w.send_line_blocking(view.substr(0, kMax - 1));
      }
    });
    const auto end = std::chrono::steady_clock::now() + 2s;
    while (c->probes == 0 && std::chrono::steady_clock::now() < end) std::this_thread::yield();
    EXPECT_GT(c->probes, 0);
    EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout);
    c->pressure = false;
    EXPECT_TRUE(result.get());
  }
  w.stop();
}

template <typename H>
auto connect(H& h) {
  if constexpr (requires { h.connect_client(); })
    return h.connect_client();
  else
    return h.start_sender();
}
template <typename H>
struct HeldServer {
  H h;
  decltype(std::declval<H&>().start_server()) server;
  decltype(connect(std::declval<H&>())) peer;
  std::promise<ClientId> connected;
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  std::atomic<bool> held{false}, unblocked{false};
  ClientId id = 0;
  bool start() {
    server = h.start_server(1024);
    auto connection = connected.get_future();
    server->on_connect([this](const auto& ctx) { connected.set_value(ctx.client_id()); });
    peer = connect(h);
    if (!peer->send("hello") || connection.wait_for(3s) != std::future_status::ready) return false;
    id = connection.get();
    server->on_connect(nullptr);
    server->on_backpressure([this](size_t queued) {
      if (queued < 1024 || held.exchange(true)) return;
      released.wait_for(10s);
    });
    if (!server->try_send_to(id, std::string(1024, 'x'))) return false;
    return test::TestUtils::waitForCondition([&] { return held.load(); }, 3000);
  }
  void unblock() {
    if (!unblocked.exchange(true)) release.set_value();
  }
  ~HeldServer() {
    unblock();
    h.stop_all();
  }
};
template <typename H>
class ServerSendValidationBeforeWaitTest : public ::testing::Test {};
using Servers = ::testing::Types<TcpServerLoopbackHarness, UdsServerLoopbackHarness, UdpServerLoopbackHarness>;
TYPED_TEST_SUITE(ServerSendValidationBeforeWaitTest, Servers);
TYPED_TEST(ServerSendValidationBeforeWaitTest, InvalidPayloadsDoNotWaitForSessionCapacity) {
  const std::string oversized(kMax + 1, 'x');
  for (int which = 0; which < 5; ++which) {
    SCOPED_TRACE(which);
    HeldServer<TypeParam> t;
    ASSERT_TRUE(t.start());
    auto result = std::async(std::launch::async, [&] {
      switch (which) {
        case 0:
          return t.server->send_to(t.id, "");
        case 1:
          return t.server->send_to_blocking(t.id, "");
        case 2:
          return t.server->send_to(t.id, oversized);
        case 3:
          return t.server->send_to_blocking(t.id, oversized);
        default:
          return t.server->send_to_line(t.id, std::string_view(oversized).substr(0, kMax));
      }
    });
    EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
    t.unblock();
    EXPECT_FALSE(result.get());
  }
}
TYPED_TEST(ServerSendValidationBeforeWaitTest, ValidPayloadAndEmptyLineStillWait) {
  for (int which = 0; which < 3; ++which) {
    SCOPED_TRACE(which);
    HeldServer<TypeParam> t;
    ASSERT_TRUE(t.start());
    std::promise<void> entered;
    auto entry = entered.get_future();
    auto result = std::async(std::launch::async, [&] {
      entered.set_value();
      if (which == 0) return t.server->send_to(t.id, "valid");
      if (which == 1) return t.server->send_to_blocking(t.id, "valid");
      return t.server->send_to_line(t.id, "");
    });
    EXPECT_EQ(entry.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(result.wait_for(100ms), std::future_status::timeout);
    t.unblock();
    EXPECT_TRUE(result.get());
  }
}

template <typename W>
bool send_valid(W& w, int api) {
  switch (api) {
    case 0:
      return w.send("valid");
    case 1:
      return w.send_blocking("valid");
    case 2:
      return w.send_line("valid");
    case 3:
      return w.send_line_blocking("valid");
    case 4:
      return w.send_move(std::vector<uint8_t>{1});
    default:
      return w.send_shared(std::make_shared<const std::vector<uint8_t>>(1, 1));
  }
}
TYPED_TEST(SendValidationBeforeWaitTest, NotReadyDoesNotWaitForCapacity) {
  auto c = std::make_shared<ValidationChannel>();
  TypeParam w(c);
  w.backpressure_strategy(base::constants::BackpressureStrategy::Reliable);
  auto started = w.start();
  c->state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());
  c->ready = false;
  for (int api = 0; api < 6; ++api) {
    SCOPED_TRACE(api);
    c->pressure = true;
    auto result = std::async(std::launch::async, [&] { return send_valid(w, api); });
    EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
    c->pressure = false;
    EXPECT_FALSE(result.get());
    EXPECT_EQ(c->failures, 0) << "not-ready wrapper should not submit a write";
  }
  w.stop();
}
TYPED_TEST(SendValidationBeforeWaitTest, ReadinessLossReleasesCapacityWait) {
  auto c = std::make_shared<ValidationChannel>();
  TypeParam w(c);
  w.backpressure_strategy(base::constants::BackpressureStrategy::Reliable);
  auto started = w.start();
  c->state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());
  for (int api = 0; api < 6; ++api) {
    SCOPED_TRACE(api);
    c->ready = true;
    c->pressure = true;
    c->probes = 0;
    auto result = std::async(std::launch::async, [&] { return send_valid(w, api); });
    const auto end = std::chrono::steady_clock::now() + 2s;
    while (c->probes == 0 && std::chrono::steady_clock::now() < end) std::this_thread::yield();
    EXPECT_GT(c->probes, 0);
    EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout);
    c->ready = false;
    // No backpressure notification: the periodic predicate must see readiness loss.
    EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
    c->pressure = false;
    EXPECT_FALSE(result.get());
    EXPECT_EQ(c->failures, 0);
  }
  w.stop();
}
TYPED_TEST(ServerSendValidationBeforeWaitTest, UnknownClientDoesNotWaitForCapacity) {
  for (int api = 0; api < 3; ++api) {
    SCOPED_TRACE(api);
    HeldServer<TypeParam> t;
    ASSERT_TRUE(t.start());
    // This fixture has exactly one session. Its next ID has never existed.
    const ClientId missing = t.id + 1;
    auto result = std::async(std::launch::async, [&] {
      if (api == 0) return t.server->send_to(missing, "valid");
      if (api == 1) return t.server->send_to_blocking(missing, "valid");
      return t.server->send_to_line(missing, "");
    });
    EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
    t.unblock();
    EXPECT_FALSE(result.get());
  }
}

}  // namespace
