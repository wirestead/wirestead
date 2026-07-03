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
#include <boost/asio.hpp>
#include <chrono>
#include <thread>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/config/udp_config.hpp"
#include "unilink/memory/safe_span.hpp"
#include "unilink/transport/udp/udp.hpp"

using namespace unilink;
using namespace unilink::transport;
namespace net = boost::asio;
using namespace std::chrono_literals;

TEST(TransportUdpErrorTest, SendOversizedPacket) {
  net::io_context ioc;

  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;  // Ephemeral
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  cfg.backpressure_threshold = 1024 * 1024;  // 1MB

  auto channel = UdpChannel::create(cfg, ioc);

  std::atomic<bool> error_occurred{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_occurred = true;
    }
  });

  channel->start();

  // UDP payload limit is 65535. Sending 100KB should definitely fail.
  std::vector<uint8_t> huge_packet(100000, 0xDD);
  channel->async_write_copy(memory::ConstByteSpan(huge_packet.data(), huge_packet.size()));

  // Run loop to process send - give it more time and poll multiple times
  for (int i = 0; i < 10 && !error_occurred.load(); ++i) {
    ioc.poll();
    if (!error_occurred.load()) {
      std::this_thread::sleep_for(10ms);
    }
  }

  EXPECT_TRUE(error_occurred.load()) << "Sending >65535 bytes via UDP should trigger Error state";

  // #445: last_error_info() should now report detail for this transport too.
  ASSERT_TRUE(channel->last_error_info().has_value());
  EXPECT_EQ(channel->last_error_info()->component, "udp");

  channel->stop();
}

TEST(TransportUdpErrorTest, BackpressureClearsAfterWriteErrorWithQueuedWrites) {
  net::io_context ioc;

  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;  // Ephemeral
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  cfg.backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  cfg.backpressure_threshold = 100000;  // Low enough that one oversized payload alone activates it

  auto channel = UdpChannel::create(cfg, ioc);

  std::atomic<bool> error_occurred{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_occurred = true;
    }
  });

  channel->start();

  // Enqueue two oversized (guaranteed EMSGSIZE) writes back-to-back before the io_context
  // ever runs, so the first is in flight and the second is still queued/pending when the
  // first write's failure is delivered. This reproduces the scenario where more than one
  // large payload was queued at the moment a UDP write errors out.
  std::vector<uint8_t> huge_packet(100000, 0xDD);
  channel->async_write_copy(memory::ConstByteSpan(huge_packet.data(), huge_packet.size()));
  channel->async_write_copy(memory::ConstByteSpan(huge_packet.data(), huge_packet.size()));

  for (int i = 0; i < 20 && !error_occurred.load(); ++i) {
    ioc.poll();
    if (!error_occurred.load()) {
      std::this_thread::sleep_for(10ms);
    }
  }

  ASSERT_TRUE(error_occurred.load());

  EXPECT_FALSE(channel->is_backpressure_active())
      << "Backpressure must clear once the channel errors out, otherwise a Reliable-mode "
         "sender blocked waiting on it deadlocks forever (see unilink#427)";

  channel->stop();
}

TEST(TransportUdpErrorTest, BackpressureClearsAfterWriteErrorWithPendingOverflow) {
  net::io_context ioc;

  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;  // Ephemeral
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  cfg.backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  cfg.backpressure_threshold = 100000;  // Low enough that one oversized payload alone activates it

  auto channel = UdpChannel::create(cfg, ioc);

  std::atomic<bool> error_occurred{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_occurred = true;
    }
  });

  channel->start();

  // Enqueue many oversized (guaranteed EMSGSIZE) writes back-to-back before the io_context ever
  // runs. The first activates backpressure and starts writing; in Reliable mode, all the rest
  // route into the pending_ overflow queue (not tx_) while backpressure stays active. When the
  // first write's failure is delivered, report_backpressure()'s internal cleanup path flushes
  // pending_ back into tx_ and can re-arm backpressure_active_ on its own if that flush alone
  // exceeds the high watermark again - reproducing the real-world hang seen on a Jetson Orin
  // Nano Super (unilink#427), where enough Reliable-mode sends had queued up that the naive fix
  // (clearing only tx_) still left backpressure stuck active.
  std::vector<uint8_t> huge_packet(100000, 0xDD);
  for (int i = 0; i < 20; ++i) {
    channel->async_write_copy(memory::ConstByteSpan(huge_packet.data(), huge_packet.size()));
  }

  for (int i = 0; i < 20 && !error_occurred.load(); ++i) {
    ioc.poll();
    if (!error_occurred.load()) {
      std::this_thread::sleep_for(10ms);
    }
  }

  ASSERT_TRUE(error_occurred.load());

  EXPECT_FALSE(channel->is_backpressure_active())
      << "Backpressure must clear once the channel errors out even when many Reliable-mode "
         "writes had overflowed into the pending_ queue (see unilink#427)";

  channel->stop();
}
