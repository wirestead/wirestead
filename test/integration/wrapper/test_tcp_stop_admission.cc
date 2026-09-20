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

// The parts of D-1 that a race cannot be waited for: callback admission,
// restart isolation, teardown completion and who is allowed to run the
// caller's executor. Each one forces the order it needs instead of hoping the
// window occurs - through the gate's testing seam, through a channel the test
// drives itself, or by occupying the executor.

#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

namespace {

class Signal {
 public:
  void notify() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      set_ = true;
    }
    cv_.notify_all();
  }
  bool wait(std::chrono::milliseconds bound = 5s) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, bound, [this] { return set_; });
  }
  bool is_set() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return set_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool set_ = false;
};

// State the gate's testing seam reaches. A free function is used because the
// seam is a plain function pointer, so it cannot capture.
struct ParkState {
  Signal parked;   // a callback has reached the admission point
  Signal release;  // the test lets it continue
  std::atomic<bool> arm{false};
  std::atomic<int> parked_count{0};
};
ParkState& park_state() {
  static ParkState state;
  return state;
}

void park_before_admission() {
  if (!park_state().arm.exchange(false)) return;  // park the first one only
  park_state().parked_count.fetch_add(1);
  park_state().parked.notify();
  park_state().release.wait(10s);
}

class ScopedAdmissionHook {
 public:
  ScopedAdmissionHook() { wrapper::detail::g_pre_admission_hook.store(&park_before_admission); }
  ~ScopedAdmissionHook() {
    wrapper::detail::g_pre_admission_hook.store(nullptr);
    park_state().arm.store(false);
    park_state().release.notify();
  }
};

class TcpStopAdmissionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = TestUtils::getAvailableTestPort();
    server_ = wirestead::tcp_server(port_).on_error([](auto&&) {}).build();
    ASSERT_TRUE(server_->start_sync());
  }
  void TearDown() override {
    if (server_) server_->stop();
  }

  uint16_t port_ = 0;
  std::shared_ptr<wrapper::TcpServer> server_;
};

// A callback that has passed its liveness check but has not registered yet is
// parked there and resumes while a stop() is in flight. Without the gate
// deciding admission and registration together, that callback would register
// after the stop had looked at the count and would then run the user handler.
TEST_F(TcpStopAdmissionTest, CallbackParkedAtAdmissionIsRefusedAfterStopCompletes) {
  ScopedAdmissionHook hook_guard;

  // An external context with two threads: one carries the parked callback,
  // the other stays free to run the teardown. On an owned single thread the
  // same scenario would say nothing - stop() would be waiting for that
  // thread, which is the parked one.
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::vector<std::thread> ioc_threads;
  for (int i = 0; i < 2; ++i) ioc_threads.emplace_back([&] { ioc->run(); });

  auto client = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, ioc);
  std::atomic<int> user_callbacks{0};
  client->on_data([&](const wrapper::MessageContext&) { user_callbacks.fetch_add(1); });

  ASSERT_TRUE(client->start_sync());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->client_count() > 0; }, 3000));

  park_state().arm.store(true);
  server_->broadcast("parked\n");
  ASSERT_TRUE(park_state().parked.wait()) << "no callback reached the admission point";

  std::atomic<bool> stop_returned{false};
  std::thread stopper([&] {
    client->stop();
    stop_returned = true;
  });
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return stop_returned.load(); }, 5000))
      << "stop() did not complete while a callback was parked before admission";
  stopper.join();

  // The parked callback resumes after stop() has returned. It had already
  // passed its liveness check, so without admission and registration being
  // one decision it would register itself here and run the user handler -
  // after the stop that was meant to exclude it.
  park_state().release.notify();
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(user_callbacks.load(), 0) << "a callback parked before admission still reached the user handler";

  client.reset();
  work.reset();
  ioc->stop();
  for (auto& thread : ioc_threads) thread.join();
}

// With a callback running on an occupied external executor, both outside
// callers stay inside stop() until it finishes. The teardown queued behind it
// is not what they wait for - contract section 1 lets outstanding internal
// work remain when it holds its own lifetime - so this test is about the
// callback, which is the part the contract does define.
TEST_F(TcpStopAdmissionTest, OutsideStopsWaitForARunningCallbackOnAnOccupiedExecutor) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::vector<std::thread> ioc_threads;
  for (int i = 0; i < 2; ++i) ioc_threads.emplace_back([&] { ioc->run(); });

  auto client = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, ioc);
  Signal in_callback, release_callback, occupied, release_occupier;
  std::atomic<bool> callback_finished{false};
  client->on_data([&](const wrapper::MessageContext&) {
    if (in_callback.is_set()) return;
    in_callback.notify();
    release_callback.wait(10s);
    callback_finished = true;
  });

  ASSERT_TRUE(client->start_sync());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->client_count() > 0; }, 3000));
  server_->broadcast("hold\n");
  ASSERT_TRUE(in_callback.wait()) << "the callback never ran";

  // Occupy the second thread too, so nothing else can make progress either.
  boost::asio::post(*ioc, [&] {
    occupied.notify();
    release_occupier.wait(10s);
  });
  ASSERT_TRUE(occupied.wait());

  std::atomic<int> entered{0};
  std::atomic<int> returns{0};
  std::vector<std::thread> stoppers;
  for (int i = 0; i < 2; ++i) {
    stoppers.emplace_back([&] {
      entered.fetch_add(1);
      client->stop();
      returns.fetch_add(1);
    });
  }
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load() == 2; }, 3000));
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(returns.load(), 0) << "a stop() returned while a callback was running";

  release_callback.notify();
  release_occupier.notify();
  for (auto& stopper : stoppers) stopper.join();
  EXPECT_EQ(returns.load(), 2);
  EXPECT_TRUE(callback_finished.load());

  client.reset();
  work.reset();
  ioc->stop();
  for (auto& thread : ioc_threads) thread.join();
}

// A stopping thread waits for the caller's executor; it does not run it. A
// handler queued on that executor must run on the executor's own thread, not
// on the thread that called stop().
TEST_F(TcpStopAdmissionTest, StopDoesNotRunTheCallersExecutor) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::atomic<std::thread::id> ran_on{};
  std::thread ioc_thread([&] { ioc->run(); });
  const auto executor_thread = ioc_thread.get_id();

  auto client = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, ioc);
  ASSERT_TRUE(client->start_sync());

  Signal occupied, release_occupier, marker_ran;
  boost::asio::post(*ioc, [&] {
    occupied.notify();
    release_occupier.wait(10s);
  });
  ASSERT_TRUE(occupied.wait());

  // Queued behind the occupier, so it is still pending when stop() is called.
  boost::asio::post(*ioc, [&] {
    ran_on.store(std::this_thread::get_id());
    marker_ran.notify();
  });

  std::thread stopper([&] { client->stop(); });
  std::this_thread::sleep_for(200ms);
  EXPECT_FALSE(marker_ran.is_set()) << "a queued handler ran while the executor was occupied";

  release_occupier.notify();
  stopper.join();
  ASSERT_TRUE(marker_ran.wait()) << "the queued handler never ran";
  EXPECT_EQ(ran_on.load(), executor_thread) << "stop() ran the caller's executor on the stopping thread";

  client.reset();
  work.reset();
  ioc->stop();
  if (ioc_thread.joinable()) ioc_thread.join();
}

}  // namespace
