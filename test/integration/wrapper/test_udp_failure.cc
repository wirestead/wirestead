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

#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <vector>

#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;

// Test wrapper behavior when not started
TEST(UdpFailureTest, WrapperSendWithoutStart) {
  config::UdpConfig cfg;
  cfg.local_port = 0;  // Ephemeral
  wrapper::UdpClient udp(cfg);

  // Not started
  EXPECT_FALSE(udp.connected());

  // Send should be safe (no-op)
  EXPECT_NO_THROW(udp.send("test"));
  EXPECT_NO_THROW(udp.send_line("test line"));
}

// Test transport behavior when not started / stopped
TEST(UdpFailureTest, TransportUninitializedAndStopped) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  // Use external ioc to control execution
  boost::asio::io_context ioc;
  auto channel = transport::UdpChannel::create(cfg, ioc);

  // 1. Test write before start (Idle state)
  // Logic might allow enqueue but do_write might check remote endpoint or
  // socket open state
  EXPECT_FALSE(channel->is_connected());

  std::vector<uint8_t> data = {0x01, 0x02, 0x03};
  channel->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));

  // Run ioc to process the posted write task
  ioc.run_for(std::chrono::milliseconds(10));

  // 2. Test write after stop (Stopping/Closed state)
  channel->stop();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(10));  // Process stop

  // Now write should be rejected immediately (enqueue_buffer returns false)
  channel->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));

  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(10));

  // Verify no crash
  SUCCEED();
}

// Test bad config options
TEST(UdpFailureTest, BadOptions) {
  // Test invalid config (though InputValidator handles most)
  // This ensures we cover the "return false" or error paths if any runtime
  // checks exist
  config::UdpConfig cfg;
  cfg.local_port = 0;
  // Invalid remote address to trigger set_remote_from_config error or similar?
  // InputValidator should catch it before.
  // But if we bypass validation?
  // transport::UdpChannel constructor calls cfg_.validate_and_clamp().
}
