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
#include "unilink/config/udp_config.hpp"
#include "unilink/transport/udp/udp.hpp"

using namespace unilink;
using namespace unilink::transport;
using namespace unilink::config;
using namespace unilink::test;
using namespace std::chrono_literals;

// Regression/stress test for jwsung91/unilink#436: UdpChannel's on_bytes_/
// on_state_/on_bp_ callback slots used to be assigned directly with no
// synchronization at all, while the io thread concurrently read the same
// members on every received datagram. This drives real I/O directly
// against the transport (not through the wrapper, which already
// synchronizes its own handler fields separately) so the io thread is
// actively reading the callback slots, while another thread concurrently
// replaces every callback for ~300ms. Under ThreadSanitizer this reliably
// reported a data race before the #436 fix and is clean after it.
TEST(UdpCallbackRaceStressTest, ConcurrentCallbackReplacementDuringActiveIo) {
  const uint16_t port = TestUtils::getAvailableTestPort();

  UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = port;  // self-addressed: every send arrives back as a receive

  auto channel = UdpChannel::create(cfg);
  channel->on_bytes([](memory::ConstByteSpan) {});
  channel->on_state([](base::LinkState) {});
  channel->on_backpressure([](size_t) {});
  channel->start();

  std::this_thread::sleep_for(50ms);
  ASSERT_TRUE(channel->is_connected());

  std::atomic<bool> stop{false};

  // Thread A: continuously send so the io thread keeps reading on_bytes_/
  // on_state_/on_bp_ on every receive-completion.
  std::thread sender([&] {
    std::vector<uint8_t> payload{1};
    while (!stop.load()) {
      channel->async_try_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));
    }
  });

  // Thread B: hammer every callback setter concurrently, directly on the
  // transport (bypassing the wrapper's own already-synchronized handlers).
  std::thread setter([&] {
    while (!stop.load()) {
      channel->on_bytes([](memory::ConstByteSpan) {});
      channel->on_state([](base::LinkState) {});
      channel->on_backpressure([](size_t) {});
    }
  });

  std::this_thread::sleep_for(1500ms);
  stop.store(true);
  sender.join();
  setter.join();

  // Sanity check: if this is 0, the test isn't exercising the io thread's
  // callback read path at all and any TSan result would be meaningless.
  EXPECT_GT(channel->stats().messages_received, 0u);

  channel->stop();
}
