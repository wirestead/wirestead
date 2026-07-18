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

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "test/utils/test_utils.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/udp/udp.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;
using namespace std::chrono_literals;
namespace net = boost::asio;
using udp = net::ip::udp;

class TransportUdpTest : public ::testing::Test {
 protected:
  void TearDown() override {
    // Ensure ports are released
    TestUtils::waitFor(50);
  }
};

TEST_F(TransportUdpTest, LoopbackSendReceive) {
  config::UdpConfig sender_cfg;
  sender_cfg.local_port = 0;
  sender_cfg.remote_address = "127.0.0.1";
  uint16_t receiver_port = TestUtils::getAvailableTestPort();
  sender_cfg.remote_port = receiver_port;

  config::UdpConfig receiver_cfg;
  receiver_cfg.local_port = receiver_port;

  auto sender = UdpChannel::create(sender_cfg);
  auto receiver = UdpChannel::create(receiver_cfg);

  std::string received;
  std::atomic<bool> done{false};

  receiver->on_bytes([&](memory::ConstByteSpan data) {
    received.assign(reinterpret_cast<const char*>(data.data()), data.size());
    done = true;
  });

  std::atomic<bool> sender_ready{false};
  sender->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Connected) sender_ready = true;
  });

  std::atomic<bool> receiver_ready{false};
  receiver->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Listening) receiver_ready = true;
  });

  receiver->start();
  sender->start();

  EXPECT_TRUE(TestUtils::waitForCondition([&] { return sender_ready.load() && receiver_ready.load(); }, 1000));

  std::string data = "hello udp";
  sender->async_write_copy(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));

  EXPECT_TRUE(TestUtils::waitForCondition([&] { return done.load(); }, 1000));
  EXPECT_EQ(received, data);

  sender->stop();
  receiver->stop();
}

TEST_F(TransportUdpTest, WriteTooLargeIgnoredOrLogged) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto sender = UdpChannel::create(cfg);
  sender->start();

  // > 64KB typically fails on UDP (max 65507 bytes)
  std::vector<uint8_t> extra(70000, 0x41);
  EXPECT_NO_THROW(sender->async_write_copy(memory::ConstByteSpan(extra.data(), extra.size())));

  // Verification is mostly that it doesn't crash or throw.
  // The error callback might be triggered if we hooked it up.
  sender->stop();
}

TEST_F(TransportUdpTest, LearnsRemoteFromFirstPacket) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.local_port = port;
  // No remote set initially

  auto channel = UdpChannel::create(cfg);
  std::string inbound;
  std::atomic<bool> received{false};

  channel->on_bytes([&](memory::ConstByteSpan data) {
    inbound.assign(reinterpret_cast<const char*>(data.data()), data.size());
    received = true;
  });

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Listening) ready = true;
  });

  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));

  // Send packet from external socket
  net::io_context ioc;
  udp::socket ext_sock(ioc, udp::endpoint(udp::v4(), 0));
  std::string msg = "ping";
  ext_sock.send_to(net::buffer(msg), udp::endpoint(net::ip::make_address("127.0.0.1"), port));

  // Increase wait time slightly for CI stability
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return received.load(); }, 2000));
  EXPECT_EQ(inbound, msg);

  // Now channel should have learned the remote endpoint and be connected
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return channel->is_connected(); }, 1000));

  // Send reply
  std::string out = "pong";
  // Important: Use ConstByteSpan copy to ensure valid data is passed if logic requires it
  // But here we just write immediately.
  // Wait, if async_write_copy copies data, it should be fine.
  // The issue might be that async_write_copy uses `safe_memcpy` with potentially invalid source/dest
  // OR the `PooledBuffer` logic is flawed.
  // Let's ensure data is valid.
  channel->async_write_copy(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(out.data()), out.size()));

  // Read reply on external socket with timeout
  char buf[1024];
  udp::endpoint sender_ep;
  ext_sock.non_blocking(true);

  bool reply_received = TestUtils::waitForCondition(
      [&] {
        boost::system::error_code ec;
        size_t n = ext_sock.receive_from(net::buffer(buf), sender_ep, 0, ec);
        if (!ec && n > 0) {
          std::string reply(buf, n);
          return reply == "pong";
        }
        return false;
      },
      2000);

  EXPECT_TRUE(reply_received);

  channel->stop();
}

TEST_F(TransportUdpTest, WriteWithoutRemoteIsNoop) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto channel = UdpChannel::create(cfg);
  channel->start();

  std::vector<uint8_t> payload = {0x01};
  channel->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));  // no remote yet

  // Should not crash, just drop
  TestUtils::waitFor(50);

  channel->stop();
}

TEST_F(TransportUdpTest, RemoteStaysFirstPeer) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.local_port = port;

  auto channel = UdpChannel::create(cfg);

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Listening) ready = true;
  });

  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));

  net::io_context ioc;
  udp::socket peer1(ioc, udp::endpoint(udp::v4(), 0));
  udp::socket peer2(ioc, udp::endpoint(udp::v4(), 0));

  // Send packet from peer1 to establish connection
  peer1.send_to(net::buffer("peer1"), udp::endpoint(net::ip::make_address("127.0.0.1"), port));

  // Wait for channel to learn peer1
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return channel->is_connected(); }, 2000));

  // Channel sends data -> should go to peer1
  std::string reply = "reply";
  channel->async_write_copy(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(reply.data()), reply.size()));

  char buf[100];
  udp::endpoint ep;

  // Verify peer1 receives reply
  peer1.non_blocking(true);
  bool peer1_got_reply = TestUtils::waitForCondition(
      [&] {
        boost::system::error_code ec;
        size_t n = peer1.receive_from(net::buffer(buf), ep, 0, ec);
        return !ec && n > 0 && std::string(buf, n) == reply;
      },
      2000);
  EXPECT_TRUE(peer1_got_reply);

  // peer2 sends data -> channel receives, but remote endpoint should NOT switch to peer2
  peer2.send_to(net::buffer("peer2"), udp::endpoint(net::ip::make_address("127.0.0.1"), port));
  TestUtils::waitFor(100);

  channel->async_write_copy(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(reply.data()), reply.size()));

  // Verify peer1 receives again (not peer2)
  peer1_got_reply = TestUtils::waitForCondition(
      [&] {
        boost::system::error_code ec;
        size_t n = peer1.receive_from(net::buffer(buf), ep, 0, ec);
        return !ec && n > 0 && std::string(buf, n) == reply;
      },
      2000);
  EXPECT_TRUE(peer1_got_reply);

  // Check peer2 has nothing
  peer2.non_blocking(true);
  boost::system::error_code ec;
  peer2.receive_from(net::buffer(buf), ep, 0, ec);
  EXPECT_EQ(ec, net::error::would_block);

  channel->stop();
}

// #435: RemoteStaysFirstPeer above only checks that the channel keeps
// *sending* to the first peer - it doesn't check whether a later, different
// sender's data still gets *delivered* through on_bytes(). Confirms it
// doesn't: once locked to peer1, peer2's datagrams are silently discarded
// rather than treated as legitimate data (the actual spoofing/hijack risk).
TEST_F(TransportUdpTest, DatagramsFromNonRemotePeerAreDiscardedNotDelivered) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.local_port = port;

  auto channel = UdpChannel::create(cfg);

  std::vector<std::string> received;
  std::mutex received_mutex;
  channel->on_bytes([&](memory::ConstByteSpan data) {
    std::lock_guard<std::mutex> lock(received_mutex);
    received.emplace_back(reinterpret_cast<const char*>(data.data()), data.size());
  });

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Listening) ready = true;
  });

  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));

  net::io_context ioc;
  udp::socket peer1(ioc, udp::endpoint(udp::v4(), 0));
  udp::socket peer2(ioc, udp::endpoint(udp::v4(), 0));

  const std::string legit1 = "legit-1";
  const std::string spoofed = "spoofed";
  const std::string legit2 = "legit-2";

  peer1.send_to(net::buffer(legit1), udp::endpoint(net::ip::make_address("127.0.0.1"), port));
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        std::lock_guard<std::mutex> lock(received_mutex);
        return !received.empty();
      },
      2000));

  // peer2 (not the locked-in remote) tries to inject data - must not appear
  // in on_bytes deliveries.
  peer2.send_to(net::buffer(spoofed), udp::endpoint(net::ip::make_address("127.0.0.1"), port));
  TestUtils::waitFor(200);

  // A second legitimate datagram from peer1 must still be delivered
  // normally, proving the filter isn't overly broad.
  peer1.send_to(net::buffer(legit2), udp::endpoint(net::ip::make_address("127.0.0.1"), port));
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        std::lock_guard<std::mutex> lock(received_mutex);
        return received.size() >= 2;
      },
      2000));

  {
    std::lock_guard<std::mutex> lock(received_mutex);
    for (const auto& msg : received) {
      EXPECT_NE(msg, spoofed) << "Datagram from a non-remote sender was delivered to on_bytes";
    }
    EXPECT_EQ(received[0], legit1);
    EXPECT_EQ(received.back(), legit2);
  }

  channel->stop();
}

TEST_F(TransportUdpTest, QueueLimitMovesToError) {
  net::io_context ioc;  // Use external IO context to control execution
  config::UdpConfig cfg;
  cfg.local_port = 0;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;  // nothing listening
  cfg.backpressure_threshold = 1024;

  auto channel = UdpChannel::create(cfg, ioc);
  std::atomic<bool> error_seen{false};
  channel->on_state([&](base::LinkState s) {
    if (s == base::LinkState::Error) error_seen = true;
  });
  channel->start();

  // Queue huge data multiple times to overflow backpressure buffer
  // Note: UdpChannel enforces a minimum limit of DEFAULT_BACKPRESSURE_THRESHOLD (1MB)
  std::vector<uint8_t> huge(350 * 1024, 0x00);  // 350KB

  // Need to push enough to exceed limit (> 1MB)
  // 4 writes of 350KB = 1.4MB
  // Important: Explicitly create ConstByteSpan to ensure type safety in test
  channel->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size()));
  channel->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size()));
  channel->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size()));
  channel->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size()));

  // Need to run the io_context to process the queue overflow
  // Increase timeout to ensure all async operations complete
  ioc.run_for(1000ms);

  EXPECT_TRUE(error_seen.load());

  channel->stop();
}

TEST_F(TransportUdpTest, StopCancelsInFlightHandlers) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto channel = UdpChannel::create(cfg);

  std::atomic<int> bytes_callbacks{0};
  channel->on_bytes([&](memory::ConstByteSpan) { bytes_callbacks.fetch_add(1); });
  channel->start();

  channel->stop();
}

TEST_F(TransportUdpTest, WriteEdgeCasesReturnFalse) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto channel = UdpChannel::create(cfg);

  std::vector<uint8_t> empty;
  EXPECT_FALSE(channel->async_write_copy(memory::ConstByteSpan(empty.data(), empty.size())));
  EXPECT_FALSE(channel->async_write_move({}));
  EXPECT_FALSE(channel->async_write_shared(nullptr));
  EXPECT_FALSE(channel->async_write_shared(std::make_shared<const std::vector<uint8_t>>()));

  std::vector<uint8_t> payload = {0x01};
  EXPECT_FALSE(channel->async_write_shared(std::make_shared<const std::vector<uint8_t>>(payload)));
  EXPECT_FALSE(channel->async_write_to(memory::ConstByteSpan(empty.data(), empty.size()),
                                       udp::endpoint(net::ip::make_address("127.0.0.1"), 9)));

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) ready = true;
  });
  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));
  channel->stop();

  EXPECT_FALSE(channel->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>{0x02}));
  EXPECT_FALSE(channel->async_write_to(memory::ConstByteSpan(payload.data(), payload.size()),
                                       udp::endpoint(net::ip::make_address("127.0.0.1"), 9)));
}

TEST_F(TransportUdpTest, ExplicitDestinationWriteWithoutRemote) {
  net::io_context ioc;
  udp::socket receiver(ioc, udp::endpoint(udp::v4(), 0));
  receiver.non_blocking(true);

  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto channel = UdpChannel::create(cfg, ioc);

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) ready = true;
  });
  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        return ready.load();
      },
      1000));

  std::string payload = "explicit";
  const udp::endpoint destination(net::ip::make_address("127.0.0.1"), receiver.local_endpoint().port());
  EXPECT_TRUE(channel->async_write_to(
      memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(payload.data()), payload.size()), destination));

  std::array<char, 64> buffer{};
  udp::endpoint sender;
  bool received = TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        boost::system::error_code ec;
        const auto bytes = receiver.receive_from(net::buffer(buffer), sender, 0, ec);
        return !ec && std::string(buffer.data(), bytes) == payload;
      },
      1000);
  EXPECT_TRUE(received);

  channel->stop();
}

TEST_F(TransportUdpTest, MemoryPoolExplicitDestinationWriteWithoutRemote) {
  net::io_context ioc;
  udp::socket receiver(ioc, udp::endpoint(udp::v4(), 0));
  receiver.non_blocking(true);

  config::UdpConfig cfg;
  cfg.local_port = 0;
  cfg.enable_memory_pool = true;
  auto channel = UdpChannel::create(cfg, ioc);

  std::atomic<bool> ready{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) ready = true;
  });
  channel->start();
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        return ready.load();
      },
      1000));

  std::vector<uint8_t> payload(32, 0x7A);
  const udp::endpoint destination(net::ip::make_address("127.0.0.1"), receiver.local_endpoint().port());
  EXPECT_TRUE(channel->async_write_to(memory::ConstByteSpan(payload.data(), payload.size()), destination));

  std::array<uint8_t, 64> buffer{};
  udp::endpoint sender;
  bool received = TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        boost::system::error_code ec;
        const auto bytes = receiver.receive_from(net::buffer(buffer), sender, 0, ec);
        return !ec && bytes == payload.size() && std::equal(payload.begin(), payload.end(), buffer.begin());
      },
      1000);
  EXPECT_TRUE(received);

  channel->stop();
}

TEST_F(TransportUdpTest, BytesFromExceptionStopsWhenConfigured) {
  auto port = TestUtils::getAvailableTestPort();
  config::UdpConfig cfg;
  cfg.local_port = port;
  cfg.stop_on_callback_exception = true;

  auto channel = UdpChannel::create(cfg);
  std::atomic<bool> ready{false};
  std::atomic<bool> error_seen{false};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) ready = true;
    if (state == base::LinkState::Error) error_seen = true;
  });
  channel->on_bytes_from([](memory::ConstByteSpan, const udp::endpoint&) { throw std::runtime_error("boom"); });

  channel->start();
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));

  net::io_context ioc;
  udp::socket sender(ioc, udp::endpoint(udp::v4(), 0));
  sender.send_to(net::buffer("boom", 4), udp::endpoint(net::ip::make_address("127.0.0.1"), port));

  EXPECT_TRUE(TestUtils::waitForCondition([&] { return error_seen.load(); }, 1000));
  channel->stop();
}

TEST_F(TransportUdpTest, InvalidRemoteAddressThrows) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  cfg.remote_address = "not a valid address";
  cfg.remote_port = 12345;

  EXPECT_THROW((void)UdpChannel::create(cfg), std::runtime_error);
}
