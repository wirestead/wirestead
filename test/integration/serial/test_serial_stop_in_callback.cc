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

// Regression tests for stop() called from inside a serial callback.
//
// The contract being held here is narrow and deliberately smaller than the
// still-open C-5.4-3 and C-1-1 decisions in
// docs/communication_contract_v0.10.md:
//
//   1. stop() from inside a callback requests the shutdown and returns,
//      without joining the thread it is running on;
//   2. a later stop() from outside completes the shutdown, even though one was
//      already requested;
//   3. after that stop() returns, the object can be restarted and destroyed;
//   4. restarting from inside a callback, before shutdown is complete, is not
//      supported and is not exercised here.
//
// Every wait is bounded, so a regression fails rather than hanging.

#include <gtest/gtest.h>

#ifndef _WIN32

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "wirestead/wirestead.hpp"

using namespace std::chrono_literals;

namespace {

class SerialStopInCallbackTest : public ::testing::Test {
 protected:
  void SetUp() override {
    master_ = posix_openpt(O_RDWR | O_NOCTTY);
    ASSERT_GE(master_, 0) << "no pseudo-terminal available";
    ASSERT_EQ(grantpt(master_), 0);
    ASSERT_EQ(unlockpt(master_), 0);
    const char* name = ptsname(master_);
    ASSERT_NE(name, nullptr);
    slave_ = name;
  }

  void TearDown() override {
    if (master_ >= 0) close(master_);
  }

  void write_line(const char* text) { ASSERT_GT(write(master_, text, std::strlen(text)), 0); }

  template <typename Predicate>
  static bool wait_for(Predicate predicate, std::chrono::milliseconds bound = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate()) return true;
      std::this_thread::sleep_for(10ms);
    }
    return predicate();
  }

  // Bounded start, so a regression fails instead of blocking the suite.
  static bool start_within(wirestead::wrapper::Serial& port, std::chrono::seconds bound = 5s) {
    auto future = port.start();
    if (future.wait_for(bound) != std::future_status::ready) return false;
    return future.get();
  }

  int master_ = -1;
  std::string slave_;
};

TEST_F(SerialStopInCallbackTest, StopFromCallbackReturnsWithoutThrowing) {
  auto port = wirestead::serial(slave_, 115200).on_error([](auto&&) {}).build();

  std::atomic<bool> entered{false};
  std::atomic<bool> returned{false};
  std::atomic<bool> threw{false};

  port->on_data([&](const wirestead::wrapper::MessageContext&) {
    if (entered.exchange(true)) return;
    try {
      port->stop();
      returned = true;
    } catch (...) {
      threw = true;
    }
  });

  ASSERT_TRUE(start_within(*port));
  write_line("ping\n");

  ASSERT_TRUE(wait_for([&] { return returned.load() || threw.load(); })) << "the callback never reached stop()";
  EXPECT_FALSE(threw.load()) << "stop() from a callback threw instead of requesting the shutdown";
  EXPECT_TRUE(returned.load());

  // Point 2 and 3: an outside stop() completes the shutdown, and the object is
  // then destroyable - the destructor runs at the end of this test.
  port->stop();
}

TEST_F(SerialStopInCallbackTest, RestartAfterCallbackStopReceivesDataAgain) {
  auto port = wirestead::serial(slave_, 115200).on_error([](auto&&) {}).build();

  std::atomic<bool> entered{false};
  std::atomic<bool> stopped_from_callback{false};

  port->on_data([&](const wirestead::wrapper::MessageContext&) {
    if (entered.exchange(true)) return;
    port->stop();
    stopped_from_callback = true;
  });

  ASSERT_TRUE(start_within(*port));
  write_line("ping\n");
  ASSERT_TRUE(wait_for([&] { return stopped_from_callback.load(); })) << "the callback never completed stop()";

  port->stop();

  // Point 3: a restart works, and the restarted channel actually receives.
  std::atomic<int> received{0};
  port->on_data([&](const wirestead::wrapper::MessageContext&) { received.fetch_add(1); });

  ASSERT_TRUE(start_within(*port)) << "restart after a callback-initiated stop did not complete";
  write_line("pong\n");
  EXPECT_TRUE(wait_for([&] { return received.load() > 0; })) << "the restarted channel received nothing";

  port->stop();
}

}  // namespace

#endif  // !_WIN32
