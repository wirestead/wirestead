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
  int executor_form = 0;
  std::atomic<bool> reject_admission{false};
  std::atomic<int> admissions{0};
  std::atomic<bool> pressure{false}, ready{false};
  mutable std::atomic<int> probes{0};
  OnBytes bytes;
  OnState state;
  OnBackpressure backpressure;
  void start() override {
    ready = true;
    connection_opened();
  }
  void stop() override {
    ready = false;
    connection_lost();
  }
  bool is_connected() const override { return ready; }
  bool is_backpressure_active() const override {
    probes++;
    return pressure;
  }
  boost::asio::any_io_executor get_executor() override {
    if (executor_form == 1) return boost::asio::make_strand(io);
    if (executor_form == 2) return boost::asio::make_strand(boost::asio::any_io_executor(io.get_executor()));
    return io.get_executor();
  }
  SendResult admission() {
    ++admissions;
    if (reject_admission) return SendResult::reject(SendRejection::WouldBlock);
    return ready ? SendResult::accept() : SendResult::reject(SendRejection::NotReady);
  }
  SendResult async_write_copy_result(memory::ConstByteSpan) override { return admission(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return admission(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override { return admission(); }
  SendResult async_try_write_copy_result(memory::ConstByteSpan b) override { return async_write_copy_result(b); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&& b) override {
    return async_write_move_result(std::move(b));
  }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> b) override {
    return async_write_shared_result(std::move(b));
  }
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { backpressure = std::move(cb); }
  void emit_state(base::LinkState s) {
    ready = s == base::LinkState::Connected;
    if (ready)
      connection_opened();
    else
      connection_lost();
    if (state) state(s);
  }
  void emit_bytes() {
    const std::string text = "message\n";
    if (bytes) bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
  }
};
template <typename W>
wrapper::SendResult send(W& w, int api) {
  switch (api) {
    case 0:
      return w.send("reply");
    case 1:
      return w.send_line("reply");
    case 2:
      return w.send_blocking("reply");
    case 3:
      return w.send_line_blocking("reply");
    case 4:
      return w.send_move(std::vector<uint8_t>{1, 2});
    default:
      return w.send_shared(std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2}));
  }
}
template <typename W>
void start(W& w, CallbackChannel& channel) {
  auto started = w.start();
  channel.emit_state(base::LinkState::Connected);
  ASSERT_EQ(started.wait_for(1s), std::future_status::ready);
  ASSERT_TRUE(started.get());
  w.backpressure_strategy(base::constants::BackpressureStrategy::Reliable);
}
template <typename W>
class CallbackBlockingSendTest : public ::testing::Test {};
using Clients = ::testing::Types<wrapper::TcpClient, wrapper::UdsClient, wrapper::UdpClient, wrapper::Serial>;
TYPED_TEST_SUITE(CallbackBlockingSendTest, Clients);

// Every dispatch kind exercises all six APIs, both with capacity and under
// pressure. The target is distinct so disconnect/error cannot hide a missing
// guard behind a not-connected rejection.
TYPED_TEST(CallbackBlockingSendTest, AllCallbacksPreserveAcceptanceAndNeverWaitForCapacity) {
  for (int kind = 0; kind < 10; ++kind) {
    SCOPED_TRACE(kind);
    auto source_channel = std::make_shared<CallbackChannel>();
    auto target_channel = std::make_shared<CallbackChannel>();
    TypeParam source(source_channel), target(target_channel);
    start(source, *source_channel);
    start(target, *target_channel);
    std::promise<void> done;
    auto finished = done.get_future();
    std::atomic<bool> invoked{false};
    auto callback = [&] {
      SCOPED_TRACE(kind);
      if (invoked.exchange(true)) return;
      for (int api = 0; api < 6; ++api) {
        SCOPED_TRACE(api);
        target_channel->pressure = false;
        EXPECT_TRUE(send(target, api));
        target_channel->pressure = true;
        EXPECT_FALSE(send(target, api));
      }
      target_channel->pressure = false;
      done.set_value();
    };
    source.batch_size(kind >= 8 ? 10 : 1).batch_latency(1ms);
    source.framer(std::make_unique<framer::LineFramer>());
    switch (kind) {
      case 0:
        source.on_data([&](const auto&) { callback(); });
        break;
      case 1:
        source.on_message([&](const auto&) { callback(); });
        break;
      case 2:
      case 8:
        source.on_data_batch([&](const auto&) { callback(); });
        break;
      case 3:
      case 9:
        source.on_message_batch([&](const auto&) { callback(); });
        break;
      case 4:
        source.on_connect([&](const auto&) { callback(); });
        break;
      case 5:
        source.on_disconnect([&](const auto&) { callback(); });
        break;
      case 6:
        source.on_error([&](const auto&) { callback(); });
        break;
      case 7:
        source.on_backpressure([&](size_t) { callback(); });
        break;
    }
    auto emitter = std::async(std::launch::async, [&] {
      if (kind == 4)
        source_channel->emit_state(base::LinkState::Connected);
      else if (kind == 5)
        source_channel->emit_state(base::LinkState::Closed);
      else if (kind == 6)
        source_channel->emit_state(base::LinkState::Error);
      else if (kind == 7)
        source_channel->backpressure(1024);
      else {
        source_channel->emit_bytes();
        if (kind >= 8) source_channel->io.run_for(100ms);
      }
      EXPECT_FALSE(wrapper::detail::in_data_callback());
    });
    // On the old implementation release pressure repeatedly until all sends
    // leave, so a regression reports failures rather than hanging teardown.
    const auto status = finished.wait_for(1s);
    EXPECT_EQ(status, std::future_status::ready);
    while (emitter.wait_for(10ms) != std::future_status::ready) target_channel->pressure = false;
    emitter.get();
    EXPECT_TRUE(invoked);
    if (kind == 5 || kind == 6) {
      for (int api = 0; api < 6; ++api) EXPECT_FALSE(send(source, api));
    }
    source.stop();
    target.stop();
  }
}

TYPED_TEST(CallbackBlockingSendTest, OrdinaryExecutorTaskNeverWaitsOrRetries) {
  for (int executor_form = 0; executor_form < 3; ++executor_form) {
    auto channel = std::make_shared<CallbackChannel>();
    channel->executor_form = executor_form;
    TypeParam target(channel);
    start(target, *channel);
    for (int api = 0; api < 6; ++api) {
      for (bool admission_race : {false, true}) {
        SCOPED_TRACE(::testing::Message() << executor_form << "/" << api << "/" << admission_race);
        channel->pressure = !admission_race;
        channel->reject_admission = admission_race;
        channel->admissions = 0;
        channel->io.restart();
        auto result = std::make_shared<std::promise<SendResult>>();
        auto done = result->get_future();
        // Deliberately post to the raw context, outside any wrapper callback or
        // target strand. A sole runner must remain available for socket work.
        boost::asio::post(channel->io, [&, result] {
          EXPECT_FALSE(wrapper::detail::in_data_callback());
          result->set_value(send(target, api));
        });
        auto runner = std::async(std::launch::async, [&] { channel->io.run(); });
        EXPECT_EQ(done.wait_for(1s), std::future_status::ready);
        // Release either old wait path so regressions fail without hanging.
        channel->pressure = false;
        channel->reject_admission = false;
        runner.get();
        const auto outcome = done.get();
        EXPECT_FALSE(outcome.accepted());
        if (!outcome.accepted()) EXPECT_EQ(outcome.reason(), SendRejection::WouldBlock);
        EXPECT_EQ(channel->admissions, admission_race ? 1 : 0);
        EXPECT_TRUE(send(target, api));
      }
    }
    target.stop();
  }
}

TYPED_TEST(CallbackBlockingSendTest, UnrelatedExecutorStillWaitsForTargetCapacity) {
  auto channel = std::make_shared<CallbackChannel>();
  TypeParam target(channel);
  start(target, *channel);
  boost::asio::io_context unrelated;
  channel->pressure = true;
  auto promise = std::make_shared<std::promise<SendResult>>();
  auto result = promise->get_future();
  boost::asio::post(unrelated, [&] { promise->set_value(target.send_blocking("waiting")); });
  auto runner = std::async(std::launch::async, [&] { unrelated.run(); });
  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (channel->probes == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
  EXPECT_GT(channel->probes, 0);
  EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout);
  channel->pressure = false;
  runner.get();
  EXPECT_TRUE(result.get());
  target.stop();
}

TYPED_TEST(CallbackBlockingSendTest, OutsideCallerStillWaitsAfterCallbackReturnsOrThrows) {
  auto channel = std::make_shared<CallbackChannel>();
  TypeParam target(channel);
  start(target, *channel);
  std::function<void()> throwing = [] { throw std::runtime_error("callback"); };
  wrapper::detail::invoke_user_callback("test", "throw", throwing);
  for (int api = 0; api < 6; ++api) {
    SCOPED_TRACE(api);
    channel->pressure = true;
    channel->probes = 0;
    auto result = std::async(std::launch::async, [&] { return send(target, api); });
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (channel->probes == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    EXPECT_GT(channel->probes, 0);
    EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout);
    channel->pressure = false;
    EXPECT_TRUE(result.get());
  }
  target.stop();
}
TEST(CallbackInvocationTest, NestedSharedAndThrowingCallbacksRestoreThreadDepth) {
  EXPECT_FALSE(wrapper::detail::in_data_callback());
  std::function<void()> outer = [] {
    EXPECT_TRUE(wrapper::detail::in_data_callback());
    auto inner = std::make_shared<const std::function<void()>>([] {
      EXPECT_TRUE(wrapper::detail::in_data_callback());
      throw 7;
    });
    wrapper::detail::invoke_user_callback("test", "inner", inner);
    EXPECT_TRUE(wrapper::detail::in_data_callback());
    std::thread unrelated([] { EXPECT_FALSE(wrapper::detail::in_data_callback()); });
    unrelated.join();
  };
  wrapper::detail::invoke_user_callback("test", "outer", outer);
  EXPECT_FALSE(wrapper::detail::in_data_callback());
  std::function<void()> empty;
  wrapper::detail::invoke_user_callback("test", "empty", empty);
  std::shared_ptr<const std::function<void()>> null;
  wrapper::detail::invoke_user_callback("test", "null", null);
  EXPECT_FALSE(wrapper::detail::in_data_callback());
}
}  // namespace
