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
#include <chrono>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/transport/udp/udp.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::config;
using namespace wirestead::test;
using namespace std::chrono_literals;

// Regression/stress test for jwsung91/wirestead#436: UdpChannel's on_bytes_/
// on_state_/on_bp_ callback slots used to be assigned directly with no
// synchronization at all, while the io thread concurrently read the same
// members on every received datagram. Uses a separate sender/receiver pair
// (the same pattern as TransportUdpTest.LoopbackSendReceive) rather than a
// self-addressed socket - a self-send loopback proved unreliable on Windows
// ARM64 CI. Drives real I/O on the receiver's io thread while another
// thread concurrently replaces every one of the receiver's callbacks for
// ~1.5s. Under ThreadSanitizer this reliably reported a data race before
// the #436 fix and is clean after it.
TEST(UdpCallbackRaceStressTest, ConcurrentCallbackReplacementDuringActiveIo) {
  const uint16_t receiver_port = TestUtils::getAvailableTestPort();

  UdpConfig receiver_cfg;
  receiver_cfg.local_port = receiver_port;
  auto receiver = UdpChannel::create(receiver_cfg);
  receiver->on_bytes([](memory::ConstByteSpan) {});
  receiver->on_state([](base::LinkState) {});
  receiver->on_backpressure([](size_t) {});

  UdpConfig sender_cfg;
  sender_cfg.local_port = 0;
  sender_cfg.remote_address = "127.0.0.1";
  sender_cfg.remote_port = receiver_port;
  auto sender = UdpChannel::create(sender_cfg);

  std::atomic<bool> receiver_ready{false};
  receiver->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Listening) receiver_ready = true;
  });
  std::atomic<bool> sender_ready{false};
  sender->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Connected) sender_ready = true;
  });

  receiver->start();
  sender->start();
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return receiver_ready.load() && sender_ready.load(); }, 1000));

  std::atomic<bool> stop{false};

  // Thread A: continuously send so the receiver's io thread keeps reading
  // on_bytes_/on_state_/on_bp_ on every receive-completion.
  std::thread sender_thread([&] {
    std::vector<uint8_t> payload{1};
    while (!stop.load()) {
      sender->async_try_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));
    }
  });

  // Thread B: hammer every callback setter concurrently, directly on the
  // transport (bypassing the wrapper's own already-synchronized handlers).
  std::thread setter_thread([&] {
    while (!stop.load()) {
      receiver->on_bytes([](memory::ConstByteSpan) {});
      receiver->on_state([](base::LinkState) {});
      receiver->on_backpressure([](size_t) {});
    }
  });

  std::this_thread::sleep_for(1500ms);
  stop.store(true);
  sender_thread.join();
  setter_thread.join();

  // Sanity check: if this is 0, the test isn't exercising the io thread's
  // callback read path at all and any TSan result would be meaningless.
  EXPECT_GT(receiver->stats().messages_received, 0u);

  sender->stop();
  receiver->stop();
}
