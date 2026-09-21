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
#ifndef _WIN32
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

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
#include "wirestead/transport/serial/serial.hpp"
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
  int master = -1;
  std::string slave;
  std::shared_ptr<transport::Serial> channel;
  std::shared_ptr<wrapper::Serial> client;
  Target(bool external, boost::asio::io_context& io) {
    master = posix_openpt(O_RDWR | O_NOCTTY);
    if (master < 0 || grantpt(master) != 0 || unlockpt(master) != 0)
      throw std::runtime_error("pseudo-terminal setup failed");
    slave = ptsname(master);
    config::SerialConfig cfg;
    cfg.device = slave;
    cfg.baud_rate = 115200;
    cfg.reopen_on_error = false;
    channel = external ? transport::Serial::create(cfg, io) : transport::Serial::create(cfg);
    client = std::make_shared<wrapper::Serial>(channel);
  }
  void on_data(std::function<void(const wrapper::MessageContext&)> cb) { client->on_data(std::move(cb)); }
  bool start() {
    auto ready = client->start();
    return ready.wait_for(5s) == std::future_status::ready && ready.get();
  }
  void stop() {
    if (client) client->stop();
  }
  ~Target() {
    stop();
    if (master >= 0) close(master);
  }
};
void send(Target& target) { ASSERT_EQ(write(target.master, "hello", 5), 5); }
// PTYs exercise the same injected transport across shutdown and restart.
class SerialStopCompletionTest : public ::testing::TestWithParam<bool> {};
TEST_P(SerialStopCompletionTest, ConcurrentOutsideStopsWaitForCallback) {
  Context context;
  Target target(GetParam(), context.io);
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
  send(target);
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
TEST_P(SerialStopCompletionTest, CallbackStopReturnsAndOutsideStopsWaitThenRestart) {
  Context context;
  Target target(GetParam(), context.io);
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
  send(target);
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
  // Fresh reads must run on the same injected transport after completion.
  Signal fresh;
  target.on_data([&](const auto&) { fresh.notify(); });
  ASSERT_TRUE(target.start());
  send(target);
  EXPECT_TRUE(fresh.wait());
  target.stop();
}
TEST_P(SerialStopCompletionTest, UnrelatedExecutorStillWaitsForCallback) {
  Context context, unrelated;
  Target target(GetParam(), context.io);
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
  send(target);
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

TEST_P(SerialStopCompletionTest, SameExecutorRequestsStopWithoutWaitingForOtherCallback) {
  Context context;
  Target target(GetParam(), context.io);
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
  if (GetParam()) {
    send(target);
    ASSERT_TRUE(entered.wait());
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
TEST_P(SerialStopCompletionTest, BatchTimerCallbackStopIsRequestOnlyAndOutsideWaits) {
  Context context;
  Target target(GetParam(), context.io);
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
  target.client->batch_size(10).batch_latency(20ms).on_data_batch(callback);
  ASSERT_TRUE(target.start());
  send(target);
  ASSERT_TRUE(requested.wait());
  outside = std::async(std::launch::async, [&] {
    target.stop();
    EXPECT_TRUE(finished.load());
  });
  EXPECT_EQ(outside.wait_for(150ms), std::future_status::timeout);
  release.notify();
  outside.get();
}
TEST_P(SerialStopCompletionTest, ConnectCallbackCanRequestStopThenRestart) {
  Context context;
  Target target(GetParam(), context.io);
  Signal requested;
  OnExit cleanup{[&] { target.stop(); }};
  target.client->on_connect([&](const auto&) {
    target.stop();
    requested.notify();
  });
  auto first = target.client->start();
  ASSERT_TRUE(requested.wait());
  target.stop();
  target.client->on_connect(nullptr);
  Signal data;
  target.on_data([&](const auto&) { data.notify(); });
  ASSERT_TRUE(target.start());
  send(target);
  EXPECT_TRUE(data.wait());
  target.stop();
}

// The factory-managed and global-manager paths must retain the channel until
// an outside caller has observed completion, just like injected channels.
TEST_P(SerialStopCompletionTest, ManagedOrSharedContextCallbackStopCompletesOutside) {
  Context context;
  Target target(false, context.io);
  auto managed_io = std::make_shared<boost::asio::io_context>();
  if (GetParam()) {
    target.client = std::make_shared<wrapper::Serial>(target.slave, 115200, managed_io);
    target.client->manage_external_context(true);
  } else {
    target.client = std::make_shared<wrapper::Serial>(target.slave, 115200);
    target.client->shared_context(true);
  }
  Signal requested, release;
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
    requested.notify();
    release.hold();
    finished = true;
  });
  ASSERT_TRUE(target.start());
  send(target);
  ASSERT_TRUE(requested.wait());
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
  Signal fresh;
  target.on_data([&](const auto&) { fresh.notify(); });
  ASSERT_TRUE(target.start());
  send(target);
  EXPECT_TRUE(fresh.wait());
  target.stop();
}
INSTANTIATE_TEST_SUITE_P(OwnedAndExternal, SerialStopCompletionTest, ::testing::Bool());
}  // namespace
#endif
