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

// Hardware-in-the-loop serial tests against a real device.
//
// They run only when WIRESTEAD_HIL_SERIAL names a serial port with an Arduino
// running test/hil/wirestead_hil/wirestead_hil.ino at 115200 baud; otherwise
// every test skips. The pty-based tests never reach a real tty driver, a real
// UART or a device that reboots when the port opens - these do.
//
// Opening the port resets an Arduino through DTR (#710), and a reset may print
// its banner late, so every test synchronises with "!ping" -> "PONG" instead
// of trusting the first banner it sees.

#include <gtest/gtest.h>

#ifndef _WIN32

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "wirestead/wirestead.hpp"

using namespace std::chrono_literals;

namespace {

constexpr uint32_t kBaud = 115200;

const char* hil_device() { return std::getenv("WIRESTEAD_HIL_SERIAL"); }

// Everything the device sent, in order.
class Inbox {
 public:
  void append(const wirestead::wrapper::MessageContext& ctx) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.append(ctx.data());
  }

  std::string snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_;
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.clear();
  }

  template <typename Predicate>
  bool wait_for(Predicate predicate, std::chrono::milliseconds bound) const {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate(snapshot())) return true;
      std::this_thread::sleep_for(1ms);
    }
    return predicate(snapshot());
  }

 private:
  mutable std::mutex mutex_;
  std::string data_;
};

bool contains(const std::string& haystack, const char* needle) { return haystack.find(needle) != std::string::npos; }

// Wait out the reset that opening the port triggered, then empty the inbox. A
// send can be refused while the link is still coming up, so that is retried
// rather than treated as failure.
bool sync(wirestead::wrapper::Serial& port, Inbox& inbox) {
  const auto deadline = std::chrono::steady_clock::now() + 8s;
  while (std::chrono::steady_clock::now() < deadline) {
    inbox.clear();
    if (!port.send("!ping\n")) {
      std::this_thread::sleep_for(50ms);
      continue;
    }
    if (inbox.wait_for([](const std::string& s) { return contains(s, "PONG\n"); }, 500ms)) {
      // A late reset banner can still follow the first PONG; one more quiet
      // round trip proves the sketch is past setup().
      std::this_thread::sleep_for(300ms);
      inbox.clear();
      if (!port.send("!ping\n")) continue;
      if (inbox.wait_for([](const std::string& s) { return s == "PONG\n"; }, 500ms)) {
        inbox.clear();
        return true;
      }
    }
  }
  return false;
}

long query_boots(wirestead::wrapper::Serial& port, Inbox& inbox) {
  inbox.clear();
  if (!port.send("!boots\n")) return -1;
  if (!inbox.wait_for([](const std::string& s) { return contains(s, "\n"); }, 1s)) return -1;
  const auto reply = inbox.snapshot();
  const auto at = reply.find("BOOTS ");
  return at == std::string::npos ? -1 : std::stol(reply.substr(at + 6));
}

class SerialHilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (hil_device() == nullptr) GTEST_SKIP() << "WIRESTEAD_HIL_SERIAL is not set";
  }

  static bool start_within(wirestead::wrapper::Serial& port, std::chrono::seconds bound = 5s) {
    auto future = port.start();
    return future.wait_for(bound) == std::future_status::ready && future.get();
  }
};

TEST_F(SerialHilTest, EchoRoundTripsAreIntact) {
  Inbox inbox;
  auto port = wirestead::serial(hil_device(), kBaud).on_error([](auto&&) {}).build();
  port->on_data([&](const wirestead::wrapper::MessageContext& ctx) { inbox.append(ctx); });
  ASSERT_TRUE(start_within(*port));
  ASSERT_TRUE(sync(*port, inbox)) << "the device never answered !ping";

  constexpr int kRounds = 200;
  std::vector<double> rtt_us;
  rtt_us.reserve(kRounds);
  for (int i = 0; i < kRounds; ++i) {
    std::string line(31, static_cast<char>('a' + i % 26));
    line += '\n';
    inbox.clear();
    const auto sent_at = std::chrono::steady_clock::now();
    ASSERT_TRUE(port->send(line));
    ASSERT_TRUE(inbox.wait_for([&](const std::string& s) { return s.size() >= line.size(); }, 1s))
        << "round " << i << " got " << inbox.snapshot().size() << " of " << line.size() << " bytes";
    rtt_us.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - sent_at).count());
    ASSERT_EQ(inbox.snapshot(), line) << "round " << i;
  }

  std::sort(rtt_us.begin(), rtt_us.end());
  const auto p50 = rtt_us[rtt_us.size() / 2];
  const auto p99 = rtt_us[rtt_us.size() * 99 / 100];
  std::cout << "32-byte echo round trip at " << kBaud << " baud: p50 " << p50 << " us, p99 " << p99 << " us\n";
  RecordProperty("rtt_p50_us", static_cast<int>(p50));
  RecordProperty("rtt_p99_us", static_cast<int>(p99));
  port->stop();
}

TEST_F(SerialHilTest, RxIdleTimeoutReopensWhenTheDeviceGoesQuiet) {
  Inbox inbox;
  std::atomic<int> connects{0};
  auto port = wirestead::serial(hil_device(), kBaud)
                  .reopen_on_error(true)
                  .retry_interval(100ms)
                  // Longer than the ~1.7 s the board is silent while it reboots
                  // on open, or the timeout fires before the sketch can answer.
                  .rx_idle_timeout(2500ms)
                  .on_error([](auto&&) {})
                  .build();
  port->on_data([&](const wirestead::wrapper::MessageContext& ctx) { inbox.append(ctx); });
  port->on_connect([&](const wirestead::wrapper::ConnectionContext&) { connects.fetch_add(1); });
  ASSERT_TRUE(start_within(*port));
  ASSERT_TRUE(sync(*port, inbox)) << "the device never answered !ping";
  const int connects_after_sync = connects.load();

  // Steady traffic below the timeout must not trip it.
  ASSERT_TRUE(port->send("!tick 50\n"));
  std::this_thread::sleep_for(1500ms);
  EXPECT_TRUE(contains(inbox.snapshot(), "T20\n")) << "ticks did not arrive: " << inbox.snapshot();
  EXPECT_EQ(connects.load(), connects_after_sync) << "a port receiving every 50 ms was reopened";

  // Silence longer than the timeout must reopen the port.
  ASSERT_TRUE(port->send("!mute 6000\n"));
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (connects.load() == connects_after_sync && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_GT(connects.load(), connects_after_sync) << "the silent port was never reopened";
  port->stop();
}

// Characterises #710 rather than endorsing it: dtr(false) is applied after
// open() has already raised DTR, so the board still resets. Revisit this test
// with whatever #710 decides.
TEST_F(SerialHilTest, DtrFalseDoesNotPreventTheResetAtOpen) {
  long before = -1;
  {
    Inbox inbox;
    auto port = wirestead::serial(hil_device(), kBaud).on_error([](auto&&) {}).build();
    port->on_data([&](const wirestead::wrapper::MessageContext& ctx) { inbox.append(ctx); });
    ASSERT_TRUE(start_within(*port));
    ASSERT_TRUE(sync(*port, inbox));
    before = query_boots(*port, inbox);
    port->stop();
  }
  ASSERT_GE(before, 0) << "!boots got no answer";

  Inbox inbox;
  auto port = wirestead::serial(hil_device(), kBaud).dtr(false).on_error([](auto&&) {}).build();
  port->on_data([&](const wirestead::wrapper::MessageContext& ctx) { inbox.append(ctx); });
  ASSERT_TRUE(start_within(*port));
  ASSERT_TRUE(sync(*port, inbox));
  const long after = query_boots(*port, inbox);
  EXPECT_GT(after, before) << "the board did not reset at open; #710 may be fixed";
  port->stop();
}

}  // namespace

#endif  // !_WIN32
