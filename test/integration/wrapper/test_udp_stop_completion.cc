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
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead;
using namespace std::chrono_literals;
namespace {
class Signal {
 public:
  void notify() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_ = true;
    cv_.notify_all();
  }
  bool wait(std::chrono::milliseconds limit = 5s) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, limit, [this] { return set_; });
  }
  void hold() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return set_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool set_ = false;
};
struct OnExit {
  std::function<void()> action;
  ~OnExit() { action(); }
};
struct Context {
  boost::asio::io_context io;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work{io.get_executor()};
  std::jthread first{[this] { io.run(); }}, second{[this] { io.run(); }};
  ~Context() {
    work.reset();
    io.stop();
  }
};
struct Target {
  std::shared_ptr<transport::UdpChannel> channel;
  std::shared_ptr<wrapper::UdpClient> client;
  std::shared_ptr<wrapper::UdpServer> server;
  Target(bool is_server, bool external, boost::asio::io_context& io) {
    config::UdpConfig cfg;
    cfg.bind_address = "127.0.0.1";
    cfg.local_port = test::TestUtils::getAvailableTestPort();
    channel = external ? transport::UdpChannel::create(cfg, io) : transport::UdpChannel::create(cfg);
    if (is_server)
      server = std::make_shared<wrapper::UdpServer>(channel);
    else
      client = std::make_shared<wrapper::UdpClient>(channel);
  }
  void on_data(std::function<void(const wrapper::MessageContext&)> cb) {
    if (client)
      client->on_data(std::move(cb));
    else
      server->on_data(std::move(cb));
  }
  bool start() {
    auto ready = client ? client->start() : server->start();
    return ready.wait_for(5s) == std::future_status::ready && ready.get();
  }
  void stop() {
    if (client)
      client->stop();
    else
      server->stop();
  }
  ~Target() { stop(); }
};
void send(Target& target, boost::asio::ip::udp::socket& peer) {
  peer.send_to(boost::asio::buffer("hello", 5), target.channel->local_endpoint());
}
// The same public contract applies to client/server and owned/external executors.
class UdpStopCompletionTest : public ::testing::TestWithParam<std::tuple<bool, bool>> {};
TEST_P(UdpStopCompletionTest, ConcurrentOutsideStopsWaitForCallback) {
  Context context;
  Target target(std::get<0>(GetParam()), std::get<1>(GetParam()), context.io);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal entered, release;
  std::atomic<bool> finished{false};
  std::future<void> first, second;
  OnExit cleanup{[&] {
    release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    target.stop();
  }};
  target.on_data([&](const auto&) {
    entered.notify();
    release.hold();
    finished = true;
  });
  ASSERT_TRUE(target.start());
  send(target, peer);
  ASSERT_TRUE(entered.wait());
  first = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  second = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  EXPECT_EQ(first.wait_for(150ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(150ms), std::future_status::timeout);
  release.notify();
  first.get();
  second.get();
  target.stop();
}
TEST_P(UdpStopCompletionTest, CallbackStopReturnsAndOutsideStopsWaitThenRestart) {
  Context context;
  Target target(std::get<0>(GetParam()), std::get<1>(GetParam()), context.io);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal returned, release;
  std::atomic<bool> finished{false};
  std::future<void> first, second;
  OnExit cleanup{[&] {
    release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    target.stop();
  }};
  target.on_data([&](const auto&) {
    target.stop();
    returned.notify();
    release.hold();
    finished = true;
  });
  ASSERT_TRUE(target.start());
  send(target, peer);
  ASSERT_TRUE(returned.wait());
  first = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  second = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  EXPECT_EQ(first.wait_for(150ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(150ms), std::future_status::timeout);
  release.notify();
  first.get();
  second.get();
  // A different endpoint must be learned on the same injected transport's new run.
  Signal fresh;
  target.on_data([&](const auto&) { fresh.notify(); });
  ASSERT_TRUE(target.start());
  boost::asio::ip::udp::socket new_peer(context.io, {boost::asio::ip::udp::v4(), 0});
  send(target, new_peer);
  EXPECT_TRUE(fresh.wait());
  target.stop();
}
TEST_P(UdpStopCompletionTest, UnrelatedExecutorStillWaitsForCallback) {
  Context context, unrelated;
  Target target(std::get<0>(GetParam()), std::get<1>(GetParam()), context.io);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal entered, release, attempted, returned;
  std::atomic<bool> finished{false};
  OnExit cleanup{[&] {
    release.notify();
    target.stop();
    returned.wait();
  }};
  target.on_data([&](const auto&) {
    entered.notify();
    release.hold();
    finished = true;
  });
  ASSERT_TRUE(target.start());
  send(target, peer);
  ASSERT_TRUE(entered.wait());
  boost::asio::post(unrelated.io, [&] {
    attempted.notify();
    target.stop();
    EXPECT_TRUE(finished.load());
    returned.notify();
  });
  ASSERT_TRUE(attempted.wait());
  EXPECT_FALSE(returned.wait(150ms));
  release.notify();
  EXPECT_TRUE(returned.wait());
  target.stop();
}

TEST_P(UdpStopCompletionTest, SameExecutorRequestsStopWithoutWaitingForOtherCallback) {
  Context context;
  Target target(std::get<0>(GetParam()), std::get<1>(GetParam()), context.io);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal entered, release, returned;
  std::future<void> outside;
  OnExit cleanup{[&] {
    release.notify();
    if (outside.valid()) outside.wait();
    target.stop();
  }};
  target.on_data([&](const auto&) {
    entered.notify();
    release.hold();
  });
  ASSERT_TRUE(target.start());
  // An external context has two runners; on an owned single runner the
  // executor call itself stays open after requesting stop.
  if (std::get<1>(GetParam())) {
    send(target, peer);
    ASSERT_TRUE(entered.wait());
    // Exercise a bare task on the shared io_context, outside the channel's
    // occupied strand. get_executor() now correctly serializes channel work.
    boost::asio::post(context.io, [&] {
      target.stop();
      returned.notify();
    });
    ASSERT_TRUE(returned.wait());
  } else {
    boost::asio::post(target.channel->get_executor(), [&] {
      target.stop();
      returned.notify();
      release.hold();
    });
    ASSERT_TRUE(returned.wait());
  }
  outside = std::async(std::launch::async, [&] { target.stop(); });
  EXPECT_EQ(outside.wait_for(150ms), std::future_status::timeout);
  release.notify();
  outside.get();
}
TEST_P(UdpStopCompletionTest, BatchTimerCallbackStopIsRequestOnlyAndOutsideWaits) {
  Context context;
  Target target(std::get<0>(GetParam()), std::get<1>(GetParam()), context.io);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal requested, release;
  std::atomic<bool> finished{false};
  std::future<void> outside;
  OnExit cleanup{[&] {
    release.notify();
    if (outside.valid()) outside.wait();
    target.stop();
  }};
  auto callback = [&](const auto&) {
    target.stop();
    requested.notify();
    release.hold();
    finished = true;
  };
  if (target.client)
    target.client->batch_size(10).batch_latency(20ms).on_data_batch(callback);
  else
    target.server->batch_size(10).batch_latency(20ms).on_data_batch(callback);
  ASSERT_TRUE(target.start());
  send(target, peer);
  ASSERT_TRUE(requested.wait());
  outside = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  EXPECT_EQ(outside.wait_for(150ms), std::future_status::timeout);
  release.notify();
  outside.get();
}
TEST(UdpServerReaperStopTest, AdmittedExpiryCallbackMustFinishBeforeOutsideStopReturns) {
  Context context;
  Target target(true, true, context.io);
  target.server->idle_timeout(100ms);
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  Signal requested, release;
  std::atomic<bool> finished{false};
  std::future<void> stopper;
  OnExit cleanup{[&] {
    release.notify();
    if (stopper.valid()) stopper.wait();
    target.stop();
  }};
  target.server->on_session_expired([&](const auto&) {
    target.stop();
    requested.notify();
    release.hold();
    finished = true;
  });
  ASSERT_TRUE(target.start());
  send(target, peer);
  ASSERT_TRUE(requested.wait());
  stopper = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  EXPECT_EQ(stopper.wait_for(200ms), std::future_status::timeout);
  release.notify();
  stopper.get();
}
struct Park {
  Signal entered, release, exited;
  std::atomic<bool> armed{true};
};
std::atomic<Park*> parked{nullptr};
void admission_hook() {
  auto* p = parked.load();
  if (p && p->armed.exchange(false)) {
    p->entered.notify();
    p->release.hold();
    p->exited.notify();
  }
}
TEST(UdpServerReaperStopTest, ExternalStopWaitsForParkedReaperBeforeRestart) {
  Context context;
  Target target(true, true, context.io);
  target.server->idle_timeout(100ms);
  Park park;
  std::future<void> stopper;
  OnExit cleanup{[&] {
    park.release.notify();
    if (!park.armed.load()) park.exited.wait();
    if (stopper.valid()) stopper.wait();
    wrapper::detail::g_pre_admission_hook = nullptr;
    parked = nullptr;
    target.stop();
  }};
  ASSERT_TRUE(target.start());
  // With no peer traffic, the only subsequent wrapper admission is the reaper.
  parked = &park;
  wrapper::detail::g_pre_admission_hook = &admission_hook;
  ASSERT_TRUE(park.entered.wait());
  // Timer and native cleanup share the strand. Even before callback admission,
  // an external stop cannot finish until the old timer releases the strand.
  stopper = std::async(std::launch::async, [&] { target.stop(); });
  EXPECT_EQ(stopper.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  ASSERT_TRUE(park.exited.wait());
  ASSERT_EQ(stopper.wait_for(5s), std::future_status::ready);
  stopper.get();
  target.server->idle_timeout(0ms);
  std::atomic<int> expired{0};
  target.server->on_session_expired([&](const auto&) { ++expired; });
  Signal received;
  target.on_data([&](const auto&) { received.notify(); });
  ASSERT_TRUE(target.start());
  boost::asio::ip::udp::socket peer(context.io, {boost::asio::ip::udp::v4(), 0});
  send(target, peer);
  ASSERT_TRUE(received.wait());
  std::this_thread::sleep_for(250ms);
  EXPECT_EQ(expired.load(), 0);
  EXPECT_EQ(target.server->client_count(), 1u);
  target.stop();
}
INSTANTIATE_TEST_SUITE_P(ClientServerOwnedExternal, UdpStopCompletionTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));
}  // namespace
