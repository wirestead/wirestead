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

// D-1 for the TCP client: concurrent and callback-initiated stop()
// (docs/communication_contract_v0.10_decisions.md).
//
//   1. stop() from a thread the shutdown does not need returns only when
//      shutdown is complete - every concurrent caller, including one that
//      finds a shutdown already requested;
//   2. stop() from a thread the shutdown does need requests it and returns;
//   3. stop() after completion returns immediately;
//   4. after a waiting stop() returns, a restart works and receives data.
//
// Every wait is bounded and every assertion is on observed order, not on
// sleeps: a regression fails rather than hanging.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

namespace {

using Clock = std::chrono::steady_clock;

// A latch that records when it was released, so a test can assert that a
// stop() returned *after* a callback finished rather than guessing with a
// sleep.
class Gate {
 public:
  void wait_until_open(std::chrono::milliseconds bound = 5s) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, bound, [this] { return open_; });
  }
  void open() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      open_ = true;
    }
    cv_.notify_all();
  }
  bool is_open() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return open_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool open_ = false;
};

class TcpClientStopCompletionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = TestUtils::getAvailableTestPort();
    server_ = wirestead::tcp_server(port_).on_error([](auto&&) {}).build();
    ASSERT_TRUE(server_->start_sync());
  }

  void TearDown() override {
    if (server_) server_->stop();
  }

  std::shared_ptr<wrapper::TcpClient> connected_client() {
    auto client = wirestead::tcp_client("127.0.0.1", port_).on_error([](auto&&) {}).build();
    return client;
  }

  void push_to_client(const std::string& payload) {
    ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->client_count() > 0; }, 3000));
    server_->broadcast(payload);
  }

  uint16_t port_ = 0;
  std::shared_ptr<wrapper::TcpServer> server_;
};

// Rule 1: two concurrent outside callers both return only after the callback
// they overlap with has finished.
TEST_F(TcpClientStopCompletionTest, ConcurrentOutsideStopsBothWaitForACallbackToFinish) {
  auto client = connected_client();

  Gate in_callback;
  std::atomic<bool> callback_finished{false};
  std::atomic<int> callbacks_after_stop{0};
  std::atomic<bool> stops_returned{false};

  client->on_data([&](const wrapper::MessageContext&) {
    if (stops_returned.load()) {
      callbacks_after_stop.fetch_add(1);
      return;
    }
    in_callback.open();
    std::this_thread::sleep_for(300ms);
    callback_finished = true;
  });

  ASSERT_TRUE(client->start_sync());
  push_to_client("hello\n");
  in_callback.wait_until_open();
  ASSERT_TRUE(in_callback.is_open()) << "the slow callback never ran";

  std::atomic<bool> first_done{false};
  std::atomic<bool> second_done{false};
  std::thread first([&] {
    client->stop();
    EXPECT_TRUE(callback_finished.load()) << "the first stop() returned while the callback was still running";
    first_done = true;
  });
  std::thread second([&] {
    client->stop();
    EXPECT_TRUE(callback_finished.load()) << "the second stop() returned while the callback was still running";
    second_done = true;
  });

  first.join();
  second.join();
  stops_returned = true;
  EXPECT_TRUE(first_done.load());
  EXPECT_TRUE(second_done.load());

  server_->broadcast("after-stop\n");
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(callbacks_after_stop.load(), 0) << "a callback ran after stop() returned";
}

// Rules 1 and 2 together, which testing them separately does not cover: the
// callback requests the shutdown and keeps running while two outside callers
// wait for it.
TEST_F(TcpClientStopCompletionTest, CallbackRequestsStopWhileOutsideCallersWaitForCompletion) {
  auto client = connected_client();

  Gate in_callback;
  std::atomic<bool> callback_finished{false};
  std::atomic<bool> inner_stop_returned_early{false};

  client->on_data([&](const wrapper::MessageContext&) {
    in_callback.open();
    const auto before = Clock::now();
    client->stop();  // rule 2: requests, does not wait for itself
    const auto elapsed = Clock::now() - before;
    inner_stop_returned_early = elapsed < 200ms;
    std::this_thread::sleep_for(300ms);
    callback_finished = true;
  });

  ASSERT_TRUE(client->start_sync());
  push_to_client("hello\n");
  in_callback.wait_until_open();
  ASSERT_TRUE(in_callback.is_open()) << "the callback never ran";

  std::vector<std::thread> waiters;
  std::atomic<int> returned_after_callback{0};
  for (int i = 0; i < 2; ++i) {
    waiters.emplace_back([&] {
      client->stop();
      if (callback_finished.load()) returned_after_callback.fetch_add(1);
    });
  }
  for (auto& waiter : waiters) waiter.join();

  EXPECT_TRUE(inner_stop_returned_early.load()) << "stop() from inside the callback waited instead of requesting";
  EXPECT_EQ(returned_after_callback.load(), 2) << "an outside stop() returned before the callback had finished";
}

// Rule 2 for the callback kinds this test can reach deterministically, and
// rule 3 for the call that follows them.
TEST_F(TcpClientStopCompletionTest, StopFromConnectCallbackReturnsAndDoesNotThrow) {
  auto client = wirestead::tcp_client("127.0.0.1", port_).on_error([](auto&&) {}).build();

  std::atomic<bool> returned{false};
  std::atomic<bool> threw{false};
  client->on_connect([&](const wrapper::ConnectionContext&) {
    try {
      client->stop();
      returned = true;
    } catch (...) {
      threw = true;
    }
  });

  client->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return returned.load() || threw.load(); }, 5000));
  EXPECT_FALSE(threw.load());
  EXPECT_TRUE(returned.load());

  client->stop();  // completes the shutdown the callback requested

  const auto before = Clock::now();
  client->stop();  // rule 3
  EXPECT_LT(Clock::now() - before, 500ms) << "a stop() after completion did not return immediately";
}

// Rule 4: after a waiting stop() returns, the channel restarts and receives.
TEST_F(TcpClientStopCompletionTest, RestartAfterStopReceivesDataAgain) {
  auto client = connected_client();

  std::atomic<int> received{0};
  client->on_data([&](const wrapper::MessageContext&) { received.fetch_add(1); });

  ASSERT_TRUE(client->start_sync());
  push_to_client("first\n");
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000));

  client->stop();
  received = 0;

  ASSERT_TRUE(client->start_sync()) << "restart after a completed stop() failed";
  push_to_client("second\n");
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000))
      << "the restarted channel received nothing";
  client->stop();
}

// Rule 1 again, on an externally run io_context: there is no io thread to
// join, so completion has to come from somewhere else.
TEST_F(TcpClientStopCompletionTest, OutsideStopWaitsOnAnExternallyRunContext) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::thread ioc_thread([&] { ioc->run(); });

  auto client = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, ioc);
  Gate in_callback;
  std::atomic<bool> callback_finished{false};

  client->on_data([&](const wrapper::MessageContext&) {
    in_callback.open();
    std::this_thread::sleep_for(300ms);
    callback_finished = true;
  });

  ASSERT_TRUE(client->start_sync());
  push_to_client("hello\n");
  in_callback.wait_until_open();
  ASSERT_TRUE(in_callback.is_open()) << "the slow callback never ran";

  client->stop();
  EXPECT_TRUE(callback_finished.load()) << "stop() returned while a callback was running on the external context";

  client.reset();
  work.reset();
  ioc->stop();
  if (ioc_thread.joinable()) ioc_thread.join();
}

// The cross-channel case: a callback of one channel stopping another.
// Sharing the executor makes the second channel's shutdown need this thread,
// so it is request-only; with its own executor the same call waits.
TEST_F(TcpClientStopCompletionTest, StoppingAnotherChannelFromACallback) {
  auto shared_ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*shared_ioc);
  std::thread ioc_thread([&] { shared_ioc->run(); });

  auto driver = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, shared_ioc);
  auto shared_peer = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_, shared_ioc);
  auto independent_peer = wirestead::tcp_client("127.0.0.1", port_).on_error([](auto&&) {}).build();

  Gate stopped_both;
  std::atomic<bool> shared_stop_fast{false};
  std::atomic<bool> independent_stop_waited{false};

  driver->on_data([&](const wrapper::MessageContext&) {
    if (stopped_both.is_open()) return;
    auto before = Clock::now();
    shared_peer->stop();  // shares this executor: request only
    shared_stop_fast = (Clock::now() - before) < 500ms;

    before = Clock::now();
    independent_peer->stop();  // its own io thread: waits for completion
    independent_stop_waited = !independent_peer->connected();

    stopped_both.open();
  });

  ASSERT_TRUE(shared_peer->start_sync());
  ASSERT_TRUE(independent_peer->start_sync());
  ASSERT_TRUE(driver->start_sync());
  push_to_client("hello\n");
  stopped_both.wait_until_open();
  ASSERT_TRUE(stopped_both.is_open()) << "the driver callback never completed";

  EXPECT_TRUE(shared_stop_fast.load()) << "stopping a channel that shares this executor blocked the callback";
  EXPECT_TRUE(independent_stop_waited.load()) << "stopping an independent channel did not complete";

  driver->stop();
  shared_peer->stop();
  independent_peer->stop();
  driver.reset();
  shared_peer.reset();
  work.reset();
  shared_ioc->stop();
  if (ioc_thread.joinable()) ioc_thread.join();
}

}  // namespace
