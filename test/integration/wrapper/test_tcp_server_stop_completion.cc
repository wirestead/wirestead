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

// D-1 for the TCP server (docs/communication_contract_v0.10_decisions.md).
// The server-specific part is that a session is one serial scope among
// several: an outside stop() has to wait for the whole object, not only for
// the session it happened to notice.
//
// Every wait is bounded; a regression fails rather than hanging.

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

// One-shot event. The tests fix their order with these rather than with
// sleeps; a sleep appears only as the window in which a caller must *not*
// have returned, where a longer wait can only weaken the test, not flake it.
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

constexpr auto kNotYetWindow = 300ms;

class TcpServerStopCompletionTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }

  std::shared_ptr<wrapper::TcpClient> connect_client() {
    auto client = wirestead::tcp_client("127.0.0.1", port_).on_error([](auto&&) {}).build();
    if (!client->start_sync()) return nullptr;
    return client;
  }

  uint16_t port_ = 0;
};

// Rule 1 with more than one session: the callback of one session is running
// while both outside callers stop the whole server.
TEST_F(TcpServerStopCompletionTest, OutsideStopsWaitForASessionCallbackToFinish) {
  auto server = wirestead::tcp_server(port_).on_error([](auto&&) {}).build();

  Signal in_callback, release_callback;
  std::atomic<bool> callback_finished{false};
  std::atomic<int> callbacks_after_stop{0};
  std::atomic<bool> stops_returned{false};

  server->on_data([&](const wrapper::MessageContext&) {
    if (stops_returned.load()) {
      callbacks_after_stop.fetch_add(1);
      return;
    }
    if (in_callback.is_set()) return;
    in_callback.notify();
    release_callback.wait();
    callback_finished = true;
  });

  ASSERT_TRUE(server->start_sync());
  auto slow_client = connect_client();
  ASSERT_NE(slow_client, nullptr);
  auto quiet_client = connect_client();
  ASSERT_NE(quiet_client, nullptr);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 2; }, 3000));

  ASSERT_TRUE(slow_client->send("busy\n"));
  ASSERT_TRUE(in_callback.wait()) << "the session callback never ran";

  std::atomic<int> entered{0};
  std::atomic<int> returns{0};
  std::atomic<int> returns_before_callback_finished{0};
  std::vector<std::thread> stoppers;
  for (int i = 0; i < 2; ++i) {
    stoppers.emplace_back([&] {
      entered.fetch_add(1);
      server->stop();
      if (!callback_finished.load()) returns_before_callback_finished.fetch_add(1);
      returns.fetch_add(1);
    });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load() == 2; }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_EQ(returns.load(), 0) << "an outside stop() returned while a session callback was running";

  release_callback.notify();
  for (auto& stopper : stoppers) stopper.join();
  stops_returned = true;

  EXPECT_EQ(returns.load(), 2);
  EXPECT_EQ(returns_before_callback_finished.load(), 0);
  EXPECT_EQ(callbacks_after_stop.load(), 0) << "a callback ran after stop() returned";

  slow_client->stop();
  quiet_client->stop();
}

// Rules 1 and 2 together on the server: the session callback requests the
// shutdown and keeps running while outside callers wait for it.
TEST_F(TcpServerStopCompletionTest, SessionCallbackRequestsStopWhileOutsideCallersWait) {
  auto server = wirestead::tcp_server(port_).on_error([](auto&&) {}).build();

  Signal in_callback, inner_stop_returned, release_callback;
  std::atomic<bool> callback_finished{false};

  server->on_data([&](const wrapper::MessageContext&) {
    if (in_callback.is_set()) return;
    in_callback.notify();
    server->stop();  // rule 2: requests, does not wait for itself
    inner_stop_returned.notify();
    release_callback.wait();
    callback_finished = true;
  });

  ASSERT_TRUE(server->start_sync());
  auto client = connect_client();
  ASSERT_NE(client, nullptr);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 1; }, 3000));
  ASSERT_TRUE(client->send("busy\n"));

  ASSERT_TRUE(in_callback.wait()) << "the session callback never ran";
  ASSERT_TRUE(inner_stop_returned.wait()) << "stop() from inside the session callback did not return";
  ASSERT_FALSE(callback_finished.load()) << "the callback ended before its stop() was observed";

  std::atomic<int> entered{0};
  std::atomic<int> returns{0};
  std::atomic<int> returns_before_callback_finished{0};
  std::vector<std::thread> stoppers;
  for (int i = 0; i < 2; ++i) {
    stoppers.emplace_back([&] {
      entered.fetch_add(1);
      server->stop();
      if (!callback_finished.load()) returns_before_callback_finished.fetch_add(1);
      returns.fetch_add(1);
    });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load() == 2; }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_EQ(returns.load(), 0) << "an outside stop() returned while the session callback was still running";

  release_callback.notify();
  for (auto& stopper : stoppers) stopper.join();

  EXPECT_EQ(returns.load(), 2);
  EXPECT_EQ(returns_before_callback_finished.load(), 0);

  client->stop();
}

// Rule 3, then rule 4: a further stop() is immediate, and the server restarts
// and serves a new client afterwards.
TEST_F(TcpServerStopCompletionTest, RestartAfterStopAcceptsAndReceivesAgain) {
  auto server = wirestead::tcp_server(port_).on_error([](auto&&) {}).build();
  std::atomic<int> received{0};
  server->on_data([&](const wrapper::MessageContext&) { received.fetch_add(1); });

  ASSERT_TRUE(server->start_sync());
  auto first_client = connect_client();
  ASSERT_NE(first_client, nullptr);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 1; }, 3000));
  ASSERT_TRUE(first_client->send("first\n"));
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000));

  server->stop();
  first_client->stop();

  const auto before = Clock::now();
  server->stop();
  EXPECT_LT(Clock::now() - before, 500ms) << "a stop() after completion did not return immediately";

  received = 0;
  ASSERT_TRUE(server->start_sync()) << "restart after a completed stop() failed";
  auto second_client = connect_client();
  ASSERT_NE(second_client, nullptr);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 1; }, 3000));
  ASSERT_TRUE(second_client->send("second\n"));
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000))
      << "the restarted server received nothing";

  second_client->stop();
  server->stop();
}

// Rule 1 on an externally run io_context, where there is no owned thread to
// join and completion has to come from the callbacks themselves.
TEST_F(TcpServerStopCompletionTest, OutsideStopWaitsOnAnExternallyRunContext) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::thread ioc_thread([&] { ioc->run(); });

  auto server = std::make_shared<wrapper::TcpServer>(port_, ioc);
  Signal in_callback, release_callback;
  std::atomic<bool> callback_finished{false};

  server->on_data([&](const wrapper::MessageContext&) {
    if (in_callback.is_set()) return;
    in_callback.notify();
    release_callback.wait();
    callback_finished = true;
  });

  ASSERT_TRUE(server->start_sync());
  auto client = connect_client();
  ASSERT_NE(client, nullptr);
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->client_count() == 1; }, 3000));
  ASSERT_TRUE(client->send("busy\n"));

  ASSERT_TRUE(in_callback.wait()) << "the session callback never ran";

  std::atomic<bool> entered{false};
  std::atomic<bool> returned{false};
  std::thread stopper([&] {
    entered = true;
    server->stop();
    returned = true;
  });

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load(); }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_FALSE(returned.load()) << "stop() returned while a session callback was running on the external context";

  release_callback.notify();
  stopper.join();
  EXPECT_TRUE(callback_finished.load());

  client->stop();
  server.reset();
  work.reset();
  ioc->stop();
  if (ioc_thread.joinable()) ioc_thread.join();
}

}  // namespace
