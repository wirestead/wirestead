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
#include <thread>

#include "test_connection_channel.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
using namespace wirestead;
using namespace std::chrono_literals;
namespace {
class CallbackChannel : public wirestead::test::TestConnectionChannel {
 public:
  boost::asio::io_context io;
  bool ready = false;
  OnState state;
  OnBytes bytes;
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void emit_bytes() {
    const uint8_t data[] = {120, 10};
    if (bytes) bytes(data);
  }
  void start() override {}
  void stop() override {
    ready = false;
    connection_lost();
  }
  bool is_connected() const override { return ready; }
  bool is_backpressure_active() const override { return false; }
  void on_backpressure(OnBackpressure) override {}
  SendResult async_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan b) override { return async_write_copy_result(b); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&& b) override {
    return async_write_move_result(std::move(b));
  }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> b) override {
    return async_write_shared_result(std::move(b));
  }

  boost::asio::any_io_executor get_executor() override { return io.get_executor(); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void emit_state(base::LinkState s) {
    ready = s == base::LinkState::Connected;
    if (ready)
      connection_opened();
    else
      connection_lost();
    if (state) state(s);
  }
};
template <class W>
class LifecycleEventsTest : public ::testing::Test {};
using Clients = ::testing::Types<wrapper::TcpClient, wrapper::UdsClient, wrapper::UdpClient, wrapper::Serial>;
TYPED_TEST_SUITE(LifecycleEventsTest, Clients);

template <class W>
void observe(W& w, std::vector<char>& events) {
  w.on_connect([&](const auto&) { events.push_back('C'); });
  w.on_disconnect([&](const auto&) { events.push_back('D'); });
  w.on_error([&](const auto&) { events.push_back('E'); });
}
TYPED_TEST(LifecycleEventsTest, RecoveredLossNotifiesOnceWithoutError) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_state(base::LinkState::Connecting);
  ch->emit_state(base::LinkState::Connecting);
  ch->emit_state(base::LinkState::Connected);
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'C'}));
  w.stop();
  EXPECT_EQ(events.size(), 3u);
}
TYPED_TEST(LifecycleEventsTest, TerminalFailureAfterLossIsDistinctAndDeduplicated) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_state(base::LinkState::Error);
  ch->emit_state(base::LinkState::Error);
  ch->emit_state(base::LinkState::Idle);
  ch->emit_state(base::LinkState::Closed);
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'E'}));
  w.stop();
}
TYPED_TEST(LifecycleEventsTest, InitialRetriesDoNotNotifyDisconnectOrTerminalError) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  auto start = w.start();
  ch->emit_state(base::LinkState::Connecting);
  ch->emit_state(base::LinkState::Connecting);
  EXPECT_TRUE(events.empty());
  EXPECT_EQ(start.wait_for(0ms), std::future_status::timeout);
  ch->emit_state(base::LinkState::Error);
  ASSERT_FALSE(start.get());
  ch->emit_state(base::LinkState::Error);
  EXPECT_EQ(events, (std::vector<char>{'E'}));
  w.stop();
}
TYPED_TEST(LifecycleEventsTest, CompletedStopSuppressesDelayedOldState) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  auto old = ch->state;
  w.stop();
  old(base::LinkState::Error);
  old(base::LinkState::Closed);
  EXPECT_EQ(events, (std::vector<char>{'C'}));
}
TYPED_TEST(LifecycleEventsTest, StopInsideLossSuppressesFollowingTerminalError) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  w.on_disconnect([&](const auto&) {
    events.push_back('D');
    w.stop();
  });
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_state(base::LinkState::Error);
  EXPECT_EQ(events, (std::vector<char>{'C', 'D'}));
  w.stop();
}
TYPED_TEST(LifecycleEventsTest, NewRunResetsTerminalNotification) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  for (int run = 0; run < 2; ++run) {
    auto start = w.start();
    ch->emit_state(base::LinkState::Error);
    EXPECT_FALSE(start.get());
    w.stop();
  }
  EXPECT_EQ(events, (std::vector<char>{'E', 'E'}));
}
TYPED_TEST(LifecycleEventsTest, PendingBatchPrecedesDisconnect) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  w.batch_size(100).batch_latency(1h);
  w.on_data_batch([&](const auto&) { events.push_back('R'); });
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_bytes();
  ch->emit_state(base::LinkState::Error);
  EXPECT_EQ(events, (std::vector<char>{'C', 'R', 'D', 'E'}));
  w.stop();
}
TYPED_TEST(LifecycleEventsTest, StopInsideLossBatchSuppressesMessageBatchAndNotifications) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  std::vector<char> events;
  observe(w, events);
  w.batch_size(100).batch_latency(1h);
  w.framer(std::make_unique<framer::LineFramer>());
  w.on_data_batch([&](const auto&) {
    events.push_back('R');
    w.stop();
  });
  w.on_message_batch([&](const auto&) { events.push_back('M'); });
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_bytes();
  ch->emit_state(base::LinkState::Error);
  EXPECT_EQ(events, (std::vector<char>{'C', 'R'}));
  w.stop();
}
}  // namespace
