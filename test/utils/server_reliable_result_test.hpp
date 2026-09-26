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

#pragma once

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
#include "wirestead/base/constants.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/send_result.hpp"

namespace wirestead::test::server_wait {
using namespace std::chrono_literals;
struct Signal {
  std::mutex mutex;
  std::condition_variable cv;
  bool set = false;
  void notify() {
    std::lock_guard<std::mutex> lock(mutex);
    set = true;
    cv.notify_all();
  }
  bool wait() {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, 5s, [&] { return set; });
  }
  void hold() {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return set; });
  }
};
struct Park {
  Signal entered, release;
  std::atomic<bool> once{true};
};
struct Observation {
  Park entry, selected;
  std::atomic<int> send{-1}, wait{-1}, sends{0};
  bool park_selected = false;
};
inline std::atomic<Observation*> observation{nullptr};
inline int code(const wrapper::SendResult& result) {
  return result.accepted() ? 100 : static_cast<int>(result.reason());
}
inline void observe_send(const wrapper::SendResult& result) {
  if (auto* state = observation.load()) {
    state->send = code(result);
    ++state->sends;
  }
}
inline void park_entry() {
  if (auto* state = observation.load(); state && state->entry.once.exchange(false)) {
    state->entry.entered.notify();
    state->entry.release.hold();
  }
}
inline void observe_wait(const wrapper::SendResult& result) {
  if (auto* state = observation.load()) {
    state->wait = code(result);
    if (state->park_selected) {
      state->selected.entered.notify();
      state->selected.release.hold();
    }
  }
}
struct Hooks {
  std::atomic<void (*)(const wrapper::SendResult&)>& send;
  std::atomic<void (*)()>& wait;
  std::atomic<void (*)(const wrapper::SendResult&)>& wait_result;
  std::atomic<void (*)()>& pinned;
};
struct OnExit {
  std::function<void()> action;
  ~OnExit() { action(); }
};

// The executor stays paused until a writer is parked. A single admitted 1 KiB
// write publishes pressure without depending on platform socket-buffer sizes.
template <typename Wrapper, typename Native, typename Socket, typename Connect>
void run_case(int event, int form, boost::asio::io_context& io, const std::shared_ptr<Native>& native, Socket& peer,
              Connect connect, Hooks hooks) {
  auto work = boost::asio::make_work_guard(io);
  Wrapper server(native);
  std::atomic<int> connections{0};
  server.on_connect([&](const auto&) { ++connections; });
  if (form == 3) server.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);
  Observation state;
  state.park_selected = event == 6 || event == 9;
  observation = &state;
  hooks.send = observe_send;
  hooks.wait_result = observe_wait;
  std::thread runner;
  std::future<wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    state.entry.release.notify();
    state.selected.release.notify();
    if (!runner.joinable())
      runner = std::thread([&] {
        io.restart();
        io.run();
      });
    server.stop();
    if (writer.valid()) writer.wait();
    work.reset();
    io.stop();
    runner.join();
    hooks.send = nullptr;
    hooks.wait = nullptr;
    hooks.wait_result = nullptr;
    hooks.pinned = nullptr;
    observation = nullptr;
  }};
  auto pump_until = [&](auto predicate) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  };
  auto ready = server.start();
  ASSERT_TRUE(pump_until([&] { return ready.wait_for(0s) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  connect(peer);
  // Finish connection initialization before parking a send while it holds the
  // wrapper lock; the session must be free to observe the later peer close.
  ASSERT_TRUE(pump_until([&] { return native->client_count() == 1 && connections == 1; }));
  const auto id = native->connected_clients().front();
  if (event != 8) {
    ASSERT_TRUE(native->send_to_client(id, std::string(1024, 'p')));
    ASSERT_TRUE(pump_until([&] { return native->is_backpressure_active(id); }));
  }
  hooks.wait = event == 8 ? nullptr : park_entry;
  hooks.pinned = event == 8 ? park_entry : nullptr;
  auto send = [&] {
    if (form == 0) return server.send_to(id, "old");
    if (form == 1) return server.send_to_line(id, "old");
    return server.send_to_blocking(id, "old");
  };
  using Rejection = wrapper::SendRejection;
  if (event == 7) {
    wrapper::detail::CallbackGuard callback;
    const auto result = send();
    ASSERT_FALSE(result.accepted());
    EXPECT_EQ(result.reason(), Rejection::WouldBlock);
    EXPECT_EQ(state.send, static_cast<int>(Rejection::WouldBlock));
    EXPECT_EQ(state.wait, -1);
    EXPECT_EQ(state.sends, 1);
    return;
  }
  writer = std::async(std::launch::async, send);
  ASSERT_TRUE(state.entry.entered.wait());
  runner = std::thread([&] { io.run(); });
  auto until = [&](auto predicate) { return TestUtils::waitForCondition(predicate, 5000); };
  if (event == 0 || event == 1 || event == 8 || event == 10) {
    peer.close();
    ASSERT_TRUE(until([&] { return native->client_count() == 0; }));
    if (event == 1) server.stop();
    if (event == 10) {
      connect(peer);
      ASSERT_TRUE(until([&] { return native->client_count() == 1; }));
    }
  } else if (event == 2) {
    native->stop();
  } else if (event == 3 || event == 4) {
    server.stop();
    if (event == 4) {
      peer.close();
      ASSERT_TRUE(server.start().get());
      connect(peer);
      ASSERT_TRUE(until([&] { return native->client_count() == 1; }));
    }
  } else {
    ASSERT_TRUE(until([&] { return !native->is_backpressure_active(id); }));
  }
  state.entry.release.notify();
  if (event == 6 || event == 9) {
    ASSERT_TRUE(state.selected.entered.wait());
    server.stop();
    if (event == 9) {
      peer.close();
      ASSERT_TRUE(server.start().get());
      connect(peer);
      ASSERT_TRUE(until([&] { return native->client_count() == 1; }));
    }
    state.selected.release.notify();
  }
  ASSERT_EQ(writer.wait_for(3s), std::future_status::ready);
  const auto result = writer.get();
  EXPECT_EQ(result.accepted(), event == 5);
  const int expected = event == 5 ? 100
                                  : static_cast<int>(event >= 2 && event <= 4 ? Rejection::CancelledWhileWaiting
                                                     : event == 6             ? Rejection::NotStarted
                                                                              : Rejection::NotReady);
  EXPECT_EQ(code(result), expected);
  EXPECT_EQ(state.send, expected);
  EXPECT_EQ(state.sends, 1);
  if (event == 4 || event == 9 || event == 10) {
    const auto replacement = native->connected_clients().front();
    auto stats = native->client_stats(replacement);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->messages_accepted, 0u);
  }
  const int wait_expected = event == 8 ? -1 : (event == 5 || event == 6 || event == 9 ? 100 : expected);
  EXPECT_EQ(state.wait, wait_expected);
}
}  // namespace wirestead::test::server_wait
