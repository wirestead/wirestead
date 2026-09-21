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

// D-1 for the UDS client: concurrent and callback-initiated stop()
// (docs/communication_contract_v0.10_decisions.md).
//
// The order these tests depend on is fixed with explicit signals rather than
// sleeps: a callback is held open by a gate, the callers announce that they
// have entered stop(), and the assertions are on what had happened by the time
// the gate was opened. Sleeps appear only as the "has not returned yet"
// observation window, where a longer sleep can only make the test weaker, not
// flaky.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
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

struct StopTestCleanup {
  std::function<void()> cleanup;
  ~StopTestCleanup() { cleanup(); }
};

// Joins a thread however the test leaves the scope, so a failed assertion
// does not terminate the process on a still-joinable std::jthread.
class JoinOnExit {
 public:
  explicit JoinOnExit(std::jthread& thread) : thread_(thread) {}
  ~JoinOnExit() {
    if (thread_.joinable()) thread_.join();
  }

 private:
  std::jthread& thread_;
};

// One-shot event.
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

// How long a caller that must not return yet is given to prove it.
constexpr auto kNotYetWindow = 300ms;

class UdsClientStopCompletionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    socket_path_ = TestUtils::makeUniqueUdsSocketPath("uds-d1").string();
    server_ = wirestead::uds_server(socket_path_).on_error([](auto&&) {}).build();
    ASSERT_TRUE(server_->start_sync());
  }

  void TearDown() override {
    if (server_) server_->stop();
  }

  std::shared_ptr<wrapper::UdsClient> make_client() {
    return wirestead::uds_client(socket_path_).on_error([](auto&&) {}).build();
  }

  void push(const std::string& payload) {
    ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->client_count() > 0; }, 3000));
    server_->broadcast(payload);
  }

  std::string socket_path_;
  std::shared_ptr<wrapper::UdsServer> server_;
};

// Rule 1: both concurrent callers return only after the callback they overlap
// with has finished, and nothing is delivered afterwards.
TEST_F(UdsClientStopCompletionTest, ConcurrentOutsideStopsBothWaitForACallbackToFinish) {
  auto client = make_client();

  Signal in_callback, release_callback;
  std::atomic<bool> callback_finished{false};
  std::atomic<int> callbacks_after_return{0};
  std::atomic<bool> returned{false};

  client->on_data([&](const wrapper::MessageContext&) {
    if (returned.load()) {
      callbacks_after_return.fetch_add(1);
      return;
    }
    if (in_callback.is_set()) return;
    in_callback.notify();
    release_callback.wait();
    callback_finished = true;
  });

  StopTestCleanup cleanup{[&] {
    release_callback.notify();
    client->stop();
  }};

  ASSERT_TRUE(client->start_sync());
  push("hello\n");
  ASSERT_TRUE(in_callback.wait()) << "the callback never ran";

  std::atomic<int> entered{0};
  std::atomic<int> returns{0};
  std::atomic<int> returns_before_callback_finished{0};
  std::vector<std::jthread> stoppers;
  for (int i = 0; i < 2; ++i) {
    stoppers.emplace_back([&] {
      entered.fetch_add(1);
      client->stop();
      if (!callback_finished.load()) returns_before_callback_finished.fetch_add(1);
      returns.fetch_add(1);
    });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load() == 2; }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_EQ(returns.load(), 0) << "a stop() returned while the callback was still running";

  release_callback.notify();
  for (auto& stopper : stoppers) stopper.join();
  returned = true;

  EXPECT_EQ(returns.load(), 2);
  EXPECT_EQ(returns_before_callback_finished.load(), 0);

  server_->broadcast("after-stop\n");
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(callbacks_after_return.load(), 0) << "a callback ran after stop() returned";
}

// Rules 1 and 2 in one run, in a fixed order: the callback's own stop() is
// observed to return *while the callback is still running*, and only then do
// the outside callers start.
TEST_F(UdsClientStopCompletionTest, CallbackRequestsStopWhileOutsideCallersWaitForCompletion) {
  auto client = make_client();

  Signal in_callback, inner_stop_returned, release_callback;
  std::atomic<bool> callback_finished{false};

  client->on_data([&](const wrapper::MessageContext&) {
    if (in_callback.is_set()) return;
    in_callback.notify();
    client->stop();  // rule 2: requests the shutdown, does not wait for itself
    inner_stop_returned.notify();
    release_callback.wait();
    callback_finished = true;
  });

  StopTestCleanup cleanup{[&] {
    release_callback.notify();
    client->stop();
  }};

  ASSERT_TRUE(client->start_sync());
  push("hello\n");
  ASSERT_TRUE(in_callback.wait()) << "the callback never ran";
  ASSERT_TRUE(inner_stop_returned.wait()) << "stop() from inside the callback did not return";
  ASSERT_FALSE(callback_finished.load()) << "the callback ended before its stop() was observed";

  std::atomic<int> entered{0};
  std::atomic<int> returns{0};
  std::atomic<int> returns_before_callback_finished{0};
  std::vector<std::jthread> stoppers;
  for (int i = 0; i < 2; ++i) {
    stoppers.emplace_back([&] {
      entered.fetch_add(1);
      client->stop();
      if (!callback_finished.load()) returns_before_callback_finished.fetch_add(1);
      returns.fetch_add(1);
    });
  }

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load() == 2; }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_EQ(returns.load(), 0) << "an outside stop() returned while the callback was still running";

  release_callback.notify();
  for (auto& stopper : stoppers) stopper.join();

  EXPECT_EQ(returns.load(), 2);
  EXPECT_EQ(returns_before_callback_finished.load(), 0);

  // Rule 4, after a shutdown that was requested from a callback: restart and
  // receive again.
  std::atomic<int> received{0};
  StopTestCleanup restart_cleanup{[&] {
    if (client) client->stop();
  }};
  client->on_data([&](const wrapper::MessageContext&) { received.fetch_add(1); });
  ASSERT_TRUE(client->start_sync()) << "restart after a callback-initiated stop failed";
  push("again\n");
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000))
      << "the restarted channel received nothing";
  client->stop();
}

// Rule 2 from the connect callback, then rule 3 for the call that follows.
TEST_F(UdsClientStopCompletionTest, StopFromConnectCallbackReturnsAndDoesNotThrow) {
  auto client = make_client();

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

  StopTestCleanup cleanup{[&] { client->stop(); }};

  client->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return returned.load() || threw.load(); }, 5000));
  EXPECT_FALSE(threw.load());
  EXPECT_TRUE(returned.load());

  client->stop();  // completes the shutdown the callback requested

  const auto before = Clock::now();
  client->stop();  // rule 3
  EXPECT_LT(Clock::now() - before, 500ms) << "a stop() after completion did not return immediately";
}

// Rule 4 on its own: restart after a completed stop() receives again.
TEST_F(UdsClientStopCompletionTest, RestartAfterStopReceivesDataAgain) {
  auto client = make_client();

  std::atomic<int> received{0};
  StopTestCleanup restart_cleanup{[&] {
    if (client) client->stop();
  }};
  client->on_data([&](const wrapper::MessageContext&) { received.fetch_add(1); });

  StopTestCleanup cleanup{[&] { client->stop(); }};

  ASSERT_TRUE(client->start_sync());
  push("first\n");
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000));

  client->stop();
  received = 0;

  ASSERT_TRUE(client->start_sync()) << "restart after a completed stop() failed";
  push("second\n");
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load() > 0; }, 3000))
      << "the restarted channel received nothing";
  client->stop();
}

// Rule 1 on an externally run io_context: no thread of ours to join, so
// completion has to come from the callbacks and the transport's own teardown.
TEST_F(UdsClientStopCompletionTest, OutsideStopWaitsOnAnExternallyRunContext) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);
  std::jthread ioc_thread([&] { ioc->run(); });

  auto client = std::make_shared<wrapper::UdsClient>(socket_path_, ioc);
  Signal in_callback, release_callback;
  std::atomic<bool> callback_finished{false};

  client->on_data([&](const wrapper::MessageContext&) {
    if (in_callback.is_set()) return;
    in_callback.notify();
    release_callback.wait();
    callback_finished = true;
  });

  StopTestCleanup cleanup{[&] {
    release_callback.notify();
    if (client) client->stop();
    work.reset();
    ioc->stop();
  }};

  ASSERT_TRUE(client->start_sync());
  push("hello\n");
  ASSERT_TRUE(in_callback.wait()) << "the callback never ran";

  std::atomic<bool> entered{false};
  std::atomic<bool> returned{false};
  std::jthread stopper([&] {
    entered = true;
    client->stop();
    returned = true;
  });

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return entered.load(); }, 3000));
  std::this_thread::sleep_for(kNotYetWindow);
  EXPECT_FALSE(returned.load()) << "stop() returned while a callback was running on the external context";

  release_callback.notify();
  stopper.join();
  EXPECT_TRUE(callback_finished.load());

  client.reset();
  work.reset();
  ioc->stop();
  if (ioc_thread.joinable()) ioc_thread.join();
}

// The cross-channel case. Stopping a channel whose shutdown needs the thread
// this callback is running on is request-only; stopping one with its own
// executor waits for that channel's callback to finish.
TEST_F(UdsClientStopCompletionTest, StoppingAnotherChannelFromACallback) {
  auto shared_ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*shared_ioc);
  std::vector<std::jthread> ioc_threads;
  for (int i = 0; i < 2; ++i) ioc_threads.emplace_back([&] { shared_ioc->run(); });
  // Released on every exit path, so a failed assertion cannot leave the gates
  // shut or the io threads running.
  struct SharedContextCleanup {
    std::vector<std::jthread>& threads;
    std::shared_ptr<boost::asio::io_context> ioc;
    std::vector<Signal*> gates;
    std::function<void()> finish;
    ~SharedContextCleanup() {
      for (auto* gate : gates) gate->notify();
      finish();
      ioc->stop();
      for (auto& thread : threads) {
        if (thread.joinable()) thread.join();
      }
    }
  };

  auto driver = std::make_shared<wrapper::UdsClient>(socket_path_, shared_ioc);
  auto shared_peer = std::make_shared<wrapper::UdsClient>(socket_path_, shared_ioc);
  auto independent_peer = make_client();

  Signal peer_in_callback, release_peer, driver_in_callback, driver_may_proceed, driver_done;
  Signal independent_in_callback, release_independent;
  std::atomic<bool> peer_callback_finished{false};
  std::atomic<bool> independent_callback_finished{false};
  std::atomic<bool> shared_stop_returned_while_peer_running{false};
  std::atomic<bool> independent_stop_waited{false};

  SharedContextCleanup cleanup{
      ioc_threads, shared_ioc, {&release_peer, &release_independent, &driver_may_proceed}, [&] {
        if (driver) driver->stop();
        if (shared_peer) shared_peer->stop();
        if (independent_peer) independent_peer->stop();
      }};

  shared_peer->on_data([&](const wrapper::MessageContext&) {
    if (peer_in_callback.is_set()) return;
    peer_in_callback.notify();
    release_peer.wait();
    peer_callback_finished = true;
  });
  independent_peer->on_data([&](const wrapper::MessageContext&) {
    if (independent_in_callback.is_set()) return;
    independent_in_callback.notify();
    release_independent.wait();
    independent_callback_finished = true;
  });
  driver->on_data([&](const wrapper::MessageContext&) {
    if (driver_in_callback.is_set()) return;
    driver_in_callback.notify();
    // The other two callbacks have to be running before this one stops them,
    // otherwise a stop that refuses their admission is indistinguishable from
    // one that waited for them.
    driver_may_proceed.wait();

    // Shares this io_context, so its shutdown needs this thread: request only.
    shared_peer->stop();
    shared_stop_returned_while_peer_running = !peer_callback_finished.load();

    // Its own executor: this waits until that channel's callback has finished,
    // which another thread releases below.
    independent_peer->stop();
    independent_stop_waited = independent_callback_finished.load();

    driver_done.notify();
  });

  ASSERT_TRUE(shared_peer->start_sync());
  ASSERT_TRUE(independent_peer->start_sync());
  ASSERT_TRUE(driver->start_sync());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->client_count() == 3; }, 5000))
      << "not every client connected";

  server_->broadcast("hold\n");
  ASSERT_TRUE(driver_in_callback.wait()) << "the driver's callback never ran";
  ASSERT_TRUE(peer_in_callback.wait()) << "the shared peer's callback never ran";
  ASSERT_TRUE(independent_in_callback.wait()) << "the independent peer's callback never ran";
  driver_may_proceed.notify();

  std::jthread releaser([&] {
    std::this_thread::sleep_for(kNotYetWindow);
    release_independent.notify();
  });
  JoinOnExit join_releaser(releaser);

  ASSERT_TRUE(driver_done.wait(10s)) << "the driver callback did not finish";
  EXPECT_TRUE(shared_stop_returned_while_peer_running.load())
      << "stopping a channel that shares this executor waited instead of requesting";
  EXPECT_TRUE(independent_stop_waited.load()) << "stopping an independent channel returned before its callback ended";

  release_peer.notify();

  driver->stop();
  shared_peer->stop();
  independent_peer->stop();
  driver.reset();
  shared_peer.reset();
  work.reset();
}

}  // namespace
