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
#include "wirestead/transport/base/stop_test_hook.hpp"
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
  std::shared_ptr<boost::asio::io_context> io = std::make_shared<boost::asio::io_context>();
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work{io->get_executor()};
  std::thread runner{[this] { io->run(); }};
  ~Context() {
    work.reset();
    io->stop();
    runner.join();
  }
};

// This channel preserves snapshots without borrowing an actual UDP strand.
// A parked wrapper admission must not also prevent transport cleanup.
class SavedChannel : public interface::Channel {
 public:
  void start() override { connected_ = true; }
  void stop() override { connected_ = false; }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override { return io_.get_executor(); }
  bool async_write_copy(memory::ConstByteSpan) override { return true; }
  bool async_write_move(std::vector<uint8_t>&&) override { return true; }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override { return true; }
  bool async_try_write_copy(memory::ConstByteSpan) override { return true; }
  bool async_try_write_move(std::vector<uint8_t>&&) override { return true; }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override { return true; }
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { bp = std::move(cb); }
  OnBytes bytes;
  OnState state;
  OnBackpressure bp;

 private:
  boost::asio::io_context io_;
  std::atomic<bool> connected_{false};
};

struct AdmissionPark {
  Signal entered, release;
  std::atomic<bool> armed{true};
};
std::atomic<AdmissionPark*> admission_park{nullptr};
void park_admission() {
  auto* park = admission_park.load();
  if (park && park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

class UdpStopAdmissionTest : public ::testing::Test {};

TEST_F(UdpStopAdmissionTest, ParkedOldHandlerIsRefusedAfterStopAndRestart) {
  auto channel = std::make_shared<SavedChannel>();
  auto client = std::make_shared<wrapper::UdpClient>(channel);
  std::atomic<int> delivered{0};
  if (client) client->on_data([&](const auto&) { ++delivered; });
  auto start = [&] {
    auto ready = client->start();
    channel->state(base::LinkState::Connected);
    EXPECT_EQ(ready.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(ready.get());
  };
  auto stop = [&] { client->stop(); };
  auto snapshot = [&]() -> std::function<void()> {
    auto bytes = channel->bytes;
    return [bytes] {
      const uint8_t data[] = {42};
      bytes(memory::ConstByteSpan(data, 1));
    };
  };
  AdmissionPark park;
  std::future<void> pending;
  OnExit cleanup{[&] {
    park.release.notify();
    if (pending.valid()) pending.wait();
    wrapper::detail::g_pre_admission_hook.store(nullptr);
    admission_park.store(nullptr);
    stop();
  }};
  start();
  auto old = snapshot();
  admission_park.store(&park);
  wrapper::detail::g_pre_admission_hook.store(&park_admission);
  pending = std::async(std::launch::async, old);
  ASSERT_TRUE(park.entered.wait());
  stop();
  // Stop completed while the old handler retained a liveness reference.
  start();
  auto fresh = snapshot();
  park.release.notify();
  pending.get();
  EXPECT_EQ(delivered.load(), 0);
  old();
  EXPECT_EQ(delivered.load(), 0);
  fresh();
  EXPECT_EQ(delivered.load(), 1);
}

struct CleanupPark {
  Signal entered, release;
  std::atomic<int> waiters{0};
  std::atomic<bool> armed{true};
};
std::atomic<CleanupPark*> cleanup_park{nullptr};
void park_cleanup(const void*, bool completing) {
  auto* park = cleanup_park.load();
  if (!park) return;
  if (!completing) {
    ++park->waiters;
  } else if (park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

std::shared_ptr<transport::UdpChannel> make_transport(boost::asio::io_context& io) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  return transport::UdpChannel::create(cfg, io);
}

TEST_F(UdpStopAdmissionTest, BothOutsideStopsWaitUntilCleanupActuallyCompletes) {
  Context context;
  auto channel = make_transport(*context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  Signal started;
  boost::asio::post(channel->get_executor(), [&] { started.notify(); });
  ASSERT_TRUE(started.wait());
  cleanup_park.store(&park);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return park.waiters.load() >= 2; }, 1000));
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
  channel->stop();
  // A completed stop also permits the SAME transport instance to restart.
  channel->start();
  Signal restarted;
  boost::asio::post(channel->get_executor(), [&] { restarted.notify(); });
  ASSERT_TRUE(restarted.wait());
  channel->stop();
}

TEST_F(UdpStopAdmissionTest, StopDoesNotRunUnrelatedReadyHandlers) {
  Context context;
  auto channel = make_transport(*context.io);
  Signal occupied, release, marker;
  CleanupPark observed;
  observed.armed = false;
  std::atomic<std::thread::id> ran_on{};
  std::future<void> stopper;
  OnExit cleanup{[&] {
    release.notify();
    if (stopper.valid()) stopper.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  boost::asio::post(channel->get_executor(), [&] {
    occupied.notify();
    release.hold();
  });
  ASSERT_TRUE(occupied.wait());
  boost::asio::post(*context.io, [&] {
    ran_on = std::this_thread::get_id();
    marker.notify();
  });
  cleanup_park.store(&observed);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  stopper = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return observed.waiters.load() == 1; }, 1000));
  EXPECT_FALSE(marker.wait(100ms));
  EXPECT_EQ(stopper.wait_for(100ms), std::future_status::timeout);
  release.notify();
  stopper.get();
  ASSERT_TRUE(marker.wait());
  EXPECT_EQ(ran_on.load(), context.runner.get_id());
}

TEST_F(UdpStopAdmissionTest, NeverStartedStopNeedsNoExecutorAndRetainsNoWork) {
  boost::asio::io_context io;
  auto channel = make_transport(io);
  std::weak_ptr<interface::Channel> weak = channel;
  channel->stop();
  channel->stop();
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}

TEST(UdpWriteStopTest, UnstartedWritesRetainNoWorkAndReentrantStopIsSafe) {
  boost::asio::io_context io;
  config::UdpConfig cfg;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  auto channel = transport::UdpChannel::create(cfg, io);
  const uint8_t byte = 1;
  EXPECT_FALSE(channel->async_write_copy(memory::ConstByteSpan(&byte, 1)));
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>{byte}));
  EXPECT_FALSE(channel->async_write_shared(std::make_shared<const std::vector<uint8_t>>(1, byte)));
  EXPECT_FALSE(
      channel->async_write_to(memory::ConstByteSpan(&byte, 1), {boost::asio::ip::make_address("127.0.0.1"), 12345}));
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Closed) channel->stop();
  });
  channel->stop();
  std::weak_ptr<transport::UdpChannel> weak = channel;
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}
TEST(UdpWriteStopTest, OversizedWriteErrorCallbackCanRequestStop) {
  Context context;
  config::UdpConfig cfg;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  auto channel = transport::UdpChannel::create(cfg, *context.io);
  Signal ready, requested;
  OnExit cleanup{[&] { channel->stop(); }};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) ready.notify();
    if (state == base::LinkState::Error) {
      channel->stop();
      requested.notify();
    }
  });
  channel->start();
  ASSERT_TRUE(ready.wait());
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>(base::constants::MAX_BUFFER_SIZE + 1)));
  EXPECT_TRUE(requested.wait());
  channel->stop();
}
TEST(UdpFailedStartStopTest, ValidationFailureNeedsNoExecutorForCleanup) {
  for (bool external : {false, true}) {
    boost::asio::io_context io;
    config::UdpConfig cfg;
    cfg.bind_address = "not-an-address";
    auto channel = external ? transport::UdpChannel::create(cfg, io) : transport::UdpChannel::create(cfg);
    std::weak_ptr<transport::UdpChannel> weak = channel;
    for (int cycle = 0; cycle < 2; ++cycle) {
      EXPECT_THROW(channel->start(), std::runtime_error);
      channel->stop();
    }
    channel.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(io.poll(), 0u);
  }
}

TEST(UdpCancelledIoCompletionTest, OutsideStopsWaitForTheLastCancelledHandler) {
  Context context;
  auto channel = make_transport(*context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_udp_io_completion_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  Signal listening;
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) listening.notify();
  });
  channel->start();
  ASSERT_TRUE(listening.wait());
  cleanup_park.store(&park);
  transport::detail::g_udp_io_completion_hook.store([] { park_cleanup(nullptr, true); });
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
}

}  // namespace
