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

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using udp = net::ip::udp;
std::function<void()> after_start;
std::function<void()> before_receive_error;
void started() {
  auto action = std::move(after_start);
  after_start = {};
  if (action) action();
}
class UdpSendAccountingTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  udp::socket peer{io, udp::endpoint(net::ip::address_v4::loopback(), 0)};
  std::shared_ptr<transport::UdpChannel> channel;
  bool ready = false;
  void drain() {
    io.restart();
    io.poll();
  }
  bool open(bool pressure = false, bool reliable = false) {
    config::UdpConfig cfg;
    cfg.bind_address = "127.0.0.1";
    if (GetParam() < 7) {
      cfg.remote_address = "127.0.0.1";
      cfg.remote_port = peer.local_endpoint().port();
    }
    cfg.enable_memory_pool = GetParam() != 1 && GetParam() != 8;
    cfg.backpressure_threshold = pressure ? 1024 : 4 * 1024 * 1024;
    if (pressure && !reliable) cfg.backpressure_strategy = base::constants::BackpressureStrategy::BestEffort;
    channel = transport::UdpChannel::create(cfg, io);
    channel->on_state([this](base::LinkState state) {
      ready = state == base::LinkState::Connected || state == base::LinkState::Listening;
    });
    channel->start();
    drain();
    return ready;
  }
  void TearDown() override {
    transport::detail::g_udp_write_started_hook = nullptr;
    transport::detail::g_udp_write_initiation_hook = nullptr;
    transport::detail::g_udp_pinned_write_hook = nullptr;
    transport::detail::g_udp_write_completion_hook = nullptr;
    after_start = {};
    before_receive_error = {};
    transport::detail::g_udp_receive_result_hook = nullptr;
    if (channel) test::stop_with_context(channel, io);
  }
  wrapper::SendAccounting stats() { return *channel->stats().send_accounting; }
  bool send(size_t n = 8) {
    std::vector<uint8_t> b(n, 42);
    auto span = memory::ConstByteSpan(b.data(), b.size());
    switch (GetParam()) {
      case 0:
      case 1:
        return channel->async_write_copy(span);
      case 2:
        return channel->async_write_move(std::move(b));
      case 3:
        return channel->async_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
      case 4:
        return channel->async_try_write_copy(span);
      case 5:
        return channel->async_try_write_move(std::move(b));
      case 6:
        return channel->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
      case 7:
      case 8:
        return channel->async_write_to(span, peer.local_endpoint());
      default:
        return channel->async_try_write_to(span, peer.local_endpoint());
    }
  }
  void hook(std::function<void()> action) {
    after_start = std::move(action);
    transport::detail::g_udp_write_started_hook = started;
  }
  template <class F>
  bool pump(F ready) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready()) {
      if (std::chrono::steady_clock::now() >= until) return false;
      io.restart();
      io.run_one_for(std::chrono::milliseconds(5));
    }
    return true;
  }
  void conserved() {
    const auto s = stats();
    EXPECT_EQ(s.accepted.requests,
              s.written.requests + s.outstanding.requests + s.explicit_stop.discarded_before_write.requests +
                  s.explicit_stop.aborted_during_write.requests + s.connection_loss.discarded_before_write.requests +
                  s.connection_loss.aborted_during_write.requests + s.queue_pressure.discarded_before_write.requests +
                  s.queue_pressure.aborted_during_write.requests);
    EXPECT_EQ(s.accepted.bytes,
              s.written.bytes + s.outstanding.bytes + s.explicit_stop.discarded_before_write.bytes +
                  s.explicit_stop.aborted_during_write.bytes + s.connection_loss.discarded_before_write.bytes +
                  s.connection_loss.aborted_during_write.bytes + s.queue_pressure.discarded_before_write.bytes +
                  s.queue_pressure.aborted_during_write.bytes);
  }
};
TEST_P(UdpSendAccountingTest, SuccessfulDatagramsRemainSeparateRequests) {
  ASSERT_TRUE(open());
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().outstanding.requests == 0; }));
  EXPECT_EQ(stats().accepted.requests, 3u);
  EXPECT_EQ(stats().written.requests, 3u);
  EXPECT_EQ(stats().confirmed_written_bytes, 24u);
  conserved();
}
TEST_P(UdpSendAccountingTest, StopBeforeEnqueueDiscardsAcceptedPosts) {
  ASSERT_TRUE(open());
  net::post(channel->get_executor(), [&] {
    EXPECT_TRUE(send());
    channel->stop();
  });
  drain();
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 8u);
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 0u);
  conserved();
}
TEST_P(UdpSendAccountingTest, StopAfterHandoffOwnsLateCompletion) {
  ASSERT_TRUE(open());
  hook([&] { channel->stop(); });
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  test::stop_with_context(channel, io);
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.requests, 2u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(stats().confirmed_written_bytes, 0u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  conserved();
}
TEST_P(UdpSendAccountingTest, InitiationFailureOwnsActiveAndPostedWork) {
  ASSERT_TRUE(open());
  transport::detail::g_udp_write_initiation_hook = +[] { throw std::runtime_error("initiation failure"); };
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 2u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  EXPECT_FALSE(channel->is_connected());
  EXPECT_FALSE(send());
  EXPECT_EQ(stats().accepted.requests, 3u);
  conserved();
}
TEST_P(UdpSendAccountingTest, DatagramTooLargeTerminatesSocketRun) {
  ASSERT_TRUE(open());
  ASSERT_TRUE(send(65536));
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().outstanding.requests == 0; }));
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.bytes, 65536u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 2u);
  EXPECT_EQ(stats().confirmed_written_bytes, 0u);
  EXPECT_FALSE(send());
  conserved();
}
TEST_P(UdpSendAccountingTest, ResetBeforeEnqueueExcludesOldEpoch) {
  ASSERT_TRUE(open());
  ASSERT_TRUE(send());
  channel->reset_stats();
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().confirmed_written_bytes, 8u);
  conserved();
}
TEST_P(UdpSendAccountingTest, ResetDuringActiveWriteExcludesOldCompletion) {
  ASSERT_TRUE(open());
  hook([&] {
    channel->reset_stats();
    EXPECT_TRUE(send());
  });
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().confirmed_written_bytes, 8u);
  conserved();
}
TEST_P(UdpSendAccountingTest, RestartDoesNotReplayOldRequests) {
  ASSERT_TRUE(open());
  hook([&] { channel->stop(); });
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  test::stop_with_context(channel, io);
  channel->reset_stats();
  channel->start();
  drain();
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(stats().confirmed_written_bytes, 8u);
  conserved();
}
TEST_P(UdpSendAccountingTest, RejectedInputsDoNotBecomeAcceptedLoss) {
  ASSERT_TRUE(open(true));
  EXPECT_FALSE(send(0));
  ASSERT_TRUE(send(800));
  if (GetParam() == 4 || GetParam() == 5 || GetParam() == 6 || GetParam() == 9)
    EXPECT_FALSE(send(800));
  else
    EXPECT_FALSE(send(*channel->write_queue_limit()));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().queue_pressure.discarded_before_write.requests, 0u);
  conserved();
}
TEST_P(UdpSendAccountingTest, BestEffortPreservesAllAcceptedDatagrams) {
  ASSERT_TRUE(open(true));
  ASSERT_TRUE(send(800));
  std::vector<uint8_t> b(400);
  for (int i = 0; i != 3; ++i)
    ASSERT_TRUE(channel->async_write_to(memory::ConstByteSpan(b.data(), b.size()), peer.local_endpoint()));
  ASSERT_TRUE(pump([&] { return stats().outstanding.requests == 0; }));
  EXPECT_EQ(stats().queue_pressure.discarded_before_write.requests, 0u);
  EXPECT_EQ(stats().written.requests, 4u);
  EXPECT_EQ(stats().written.bytes, 2000u);
  EXPECT_EQ(stats().queue_pressure.aborted_during_write.requests, 0u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 0u);
  conserved();
}
TEST_P(UdpSendAccountingTest, ReliablePendingDatagramsCompleteWithoutLoss) {
  ASSERT_TRUE(open(true, true));
  ASSERT_TRUE(send(800));
  std::vector<uint8_t> b(800);
  for (int i = 0; i != 3; ++i)
    ASSERT_TRUE(channel->async_write_to(memory::ConstByteSpan(b.data(), b.size()), peer.local_endpoint()));
  ASSERT_TRUE(pump([&] { return stats().outstanding.requests == 0; }));
  EXPECT_EQ(stats().written.requests, 4u);
  EXPECT_EQ(stats().confirmed_written_bytes, 3200u);
  conserved();
}

TEST_P(UdpSendAccountingTest, ReceiveFailureDiscardsAcceptedPostsBeforeEnqueue) {
  ASSERT_TRUE(open());
  ASSERT_TRUE(send());
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  // Learn the actual ephemeral source port from the datagram.
  peer.non_blocking(true);
  udp::endpoint source;
  std::array<uint8_t, 32> data{};
  ASSERT_TRUE(pump([&] {
    boost::system::error_code ec;
    const auto n = peer.receive_from(net::buffer(data), source, 0, ec);
    return !ec && n == 8;
  }));
  channel->reset_stats();
  before_receive_error = [&] {
    EXPECT_TRUE(send());
    EXPECT_TRUE(send());
    EXPECT_TRUE(send());
  };
  transport::detail::g_udp_receive_result_hook = +[](boost::system::error_code& ec) {
    if (before_receive_error) {
      auto action = std::move(before_receive_error);
      before_receive_error = {};
      action();
      ec = net::error::connection_reset;
    }
  };
  peer.send_to(net::buffer(data.data(), 8), source);
  ASSERT_TRUE(pump([&] { return stats().connection_loss.discarded_before_write.requests == 3; }));
  drain();
  EXPECT_EQ(stats().accepted.requests, 3u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 0u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  EXPECT_FALSE(send());
  conserved();
}
INSTANTIATE_TEST_SUITE_P(AllInputs, UdpSendAccountingTest, ::testing::Range(0, 10));
}  // namespace

namespace {
using namespace std::chrono_literals;
auto virtual_now = std::chrono::steady_clock::time_point(1s);
class UdpSessionAccountingTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  udp::socket first{io, udp::endpoint(net::ip::address_v4::loopback(), 0)};
  udp::socket second{io, udp::endpoint(net::ip::address_v4::loopback(), 0)};
  udp::endpoint destination;
  std::shared_ptr<transport::UdpChannel> channel;
  std::unique_ptr<wrapper::UdpServer> server;
  wirestead::ClientId a{}, b{};
  template <class F>
  bool pump(F ready) {
    const auto end = std::chrono::steady_clock::now() + 5s;
    while (!ready()) {
      if (std::chrono::steady_clock::now() >= end) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  }
  bool open(bool pressure = false, bool factory = false) {
    udp::socket reservation(io, udp::endpoint(net::ip::address_v4::loopback(), 0));
    destination = reservation.local_endpoint();
    reservation.close();
    config::UdpConfig cfg;
    cfg.bind_address = "127.0.0.1";
    cfg.local_port = destination.port();
    cfg.enable_memory_pool = GetParam() != 2;
    cfg.backpressure_threshold = pressure ? 1024 : 4 * 1024 * 1024;
    if (factory) {
      auto borrowed = std::shared_ptr<net::io_context>(&io, [](auto*) {});
      server = std::make_unique<wrapper::UdpServer>(cfg, borrowed);
    } else {
      channel = transport::UdpChannel::create(cfg, io);
      server = std::make_unique<wrapper::UdpServer>(channel);
    }
    virtual_now = std::chrono::steady_clock::time_point(1s);
    transport::detail::g_udp_session_clock_hook = +[] { return virtual_now; };
    auto ready = server->start();
    return pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }) && ready.get();
  }
  wirestead::ClientId connect(udp::socket& peer) {
    const auto before = server->client_count();
    peer.send_to(net::buffer("hello", 5), destination);
    if (!pump([&] { return server->client_count() > before; })) return 0;
    const auto ids = server->connected_clients();
    return *std::max_element(ids.begin(), ids.end());
  }
  wrapper::SendResult send(wirestead::ClientId id, std::string_view data = "1234567") {
    if (GetParam() == 0) return server->try_send_to(id, data);
    return server->send_to_blocking(id, data);
  }
  wrapper::SendAccounting total() { return *server->stats().send_accounting; }
  wrapper::SendAccounting peer(wirestead::ClientId id) { return *server->client_stats(id)->send_accounting; }
  void settle() {
    ASSERT_TRUE(pump([&] { return total().outstanding.requests == 0; }));
  }
  void stop() { test::stop_wrapper_with_context(*server, io); }
  void TearDown() override {
    transport::detail::g_udp_write_started_hook = nullptr;
    transport::detail::g_udp_write_initiation_hook = nullptr;
    transport::detail::g_udp_pinned_write_hook = nullptr;
    transport::detail::g_udp_write_completion_hook = nullptr;
    transport::detail::g_udp_session_clock_hook = nullptr;
    after_start = {};
    if (server) stop();
  }
};

TEST_P(UdpSessionAccountingTest, BestEffortBlockingPreservesEveryAcceptedDatagram) {
  ASSERT_TRUE(open(true));
  a = connect(first);
  ASSERT_NE(a, 0u);
  channel->set_backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);
  ASSERT_TRUE(server->send_to_blocking(a, std::string(800, 'a')));
  for (int i = 0; i < 3; ++i) ASSERT_TRUE(server->send_to_blocking(a, std::string(400, 'b')));
  settle();
  EXPECT_EQ(peer(a).accepted.requests, 4u);
  EXPECT_EQ(peer(a).written.requests, 4u);
  EXPECT_EQ(peer(a).written.bytes, 2000u);
  EXPECT_EQ(peer(a).queue_pressure.discarded_before_write.requests, 0u);
}

TEST_P(UdpSessionAccountingTest, BlockingCapacityRetriesBeyondFiveAndStopReleasesSender) {
  ASSERT_TRUE(open(true));
  a = connect(first);
  ASSERT_NE(a, 0u);
  ASSERT_TRUE(server->send_to_blocking(a, std::string(*channel->write_queue_limit(), 'f')));
  const auto failures = channel->stats().failed_sends;
  auto sender = std::async(std::launch::async, [&] { return server->send_to_blocking(a, "abc"); });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (channel->stats().failed_sends < failures + 12 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(1ms);
  const bool retried = channel->stats().failed_sends >= failures + 12;
  auto stopper = std::async(std::launch::async, [&] { server->stop(); });
  const bool released = sender.wait_for(5s) == std::future_status::ready;
  EXPECT_TRUE(pump([&] { return stopper.wait_for(0ms) == std::future_status::ready; }));
  stopper.get();
  ASSERT_TRUE(released);
  EXPECT_TRUE(retried);
  const auto result = sender.get();
  EXPECT_FALSE(result.accepted());
  EXPECT_TRUE(result.reason() == wrapper::SendRejection::CancelledWhileWaiting ||
              result.reason() == wrapper::SendRejection::Stopping);
}

TEST_P(UdpSessionAccountingTest, PeerTotalsAndBroadcastCountEachAcceptedTargetOnce) {
  ASSERT_TRUE(open());
  a = connect(first);
  b = connect(second);
  ASSERT_NE(a, 0u);
  ASSERT_NE(b, 0u);
  ASSERT_TRUE(send(a).accepted());
  ASSERT_TRUE(send(b, "12345678901").accepted());
  auto result = server->broadcast("abc");
  ASSERT_EQ(result.accepted_count(), 2u);
  settle();
  EXPECT_EQ(peer(a).written.bytes, 10u);
  EXPECT_EQ(peer(b).written.bytes, 14u);
  EXPECT_EQ(total().written.bytes, 24u);
  EXPECT_EQ(total().accepted.requests, 4u);
  EXPECT_EQ(server->client_stats(a)->messages_received, 1u);
  EXPECT_EQ(server->client_stats(b)->bytes_received, 5u);
  EXPECT_EQ(server->stats().bytes_received, 10u);
  EXPECT_FALSE(server->client_stats(99999));
  EXPECT_FALSE(send(99999).accepted());
  EXPECT_EQ(total().accepted.requests, 4u);
}
TEST_P(UdpSessionAccountingTest, ResetExcludesOldPostedAndActiveWritesFromEveryLedger) {
  ASSERT_TRUE(open());
  a = connect(first);
  b = connect(second);
  ASSERT_TRUE(send(a).accepted());
  server->reset_stats();
  EXPECT_EQ(peer(a).accepted.requests, 0u);
  EXPECT_EQ(server->client_stats(a)->messages_received, 0u);
  after_start = [&] {
    server->reset_stats();
    EXPECT_TRUE(send(b, "abc").accepted());
  };
  transport::detail::g_udp_write_started_hook = started;
  ASSERT_TRUE(pump([&] { return total().written.requests == 1; }));
  EXPECT_EQ(total().accepted.requests, 1u);
  EXPECT_EQ(total().written.bytes, 3u);
  EXPECT_EQ(peer(a).accepted.requests, 0u);
  EXPECT_EQ(peer(b).written.bytes, 3u);
}
TEST_P(UdpSessionAccountingTest, ExpiryDiscardsPostedWorkPreservesActiveAndOtherPeer) {
  ASSERT_TRUE(open());
  a = connect(first);
  virtual_now += 100ms;
  b = connect(second);
  server->idle_timeout(100ms);
  after_start = [&] {
    virtual_now += 50ms;
    // A second executor thread advances the reaper while this strand holds
    // the active write. Nested polling on this thread can dispatch reentrantly.
    bool expired = false;
    std::jthread reaper([&] { expired = pump([&] { return !server->client_stats(a).has_value(); }); });
    reaper.join();
    EXPECT_TRUE(expired);
    EXPECT_TRUE(server->client_stats(b).has_value());
    EXPECT_EQ(total().session_expiry.discarded_before_write.requests, 2u);
    EXPECT_EQ(total().outstanding.requests, 2u);
  };
  transport::detail::g_udp_write_started_hook = started;
  ASSERT_TRUE(send(a).accepted());
  ASSERT_TRUE(send(a, "abc").accepted());
  ASSERT_TRUE(send(a, "abcde").accepted());
  ASSERT_TRUE(send(b, "abcdefghi").accepted());
  settle();
  EXPECT_EQ(total().session_expiry.discarded_before_write.bytes, 8u);
  EXPECT_EQ(total().session_expiry.aborted_during_write.requests, 0u);
  EXPECT_EQ(total().written.bytes, 16u);
  EXPECT_EQ(peer(b).written.bytes, 9u);
  EXPECT_FALSE(send(a).accepted());
  auto replacement = connect(first);
  ASSERT_NE(replacement, a);
  EXPECT_EQ(peer(replacement).accepted.requests, 0u);
  ASSERT_TRUE(send(replacement, "ab").accepted());
  settle();
  EXPECT_EQ(peer(replacement).written.bytes, 2u);
  EXPECT_EQ(total().accepted.requests, 5u);
  EXPECT_EQ(total().written.requests, 3u);
}
TEST_P(UdpSessionAccountingTest, ExpiryThenStopKeepsFirstCauseForWaitingWrites) {
  ASSERT_TRUE(open());
  a = connect(first);
  server->idle_timeout(100ms);
  after_start = [&] {
    virtual_now += 101ms;
    // A second executor thread advances the reaper while this strand holds
    // the active write. Nested polling on this thread can dispatch reentrantly.
    bool expired = false;
    std::jthread reaper([&] { expired = pump([&] { return !server->client_stats(a).has_value(); }); });
    reaper.join();
    EXPECT_TRUE(expired);
    server->stop();
  };
  transport::detail::g_udp_write_started_hook = started;
  ASSERT_TRUE(send(a).accepted());
  ASSERT_TRUE(send(a).accepted());
  settle();
  stop();
  EXPECT_EQ(total().session_expiry.discarded_before_write.requests, 1u);
  EXPECT_EQ(total().explicit_stop.aborted_during_write.requests, 1u);
  EXPECT_EQ(total().explicit_stop.discarded_before_write.requests, 0u);
  EXPECT_EQ(total().written.requests, 0u);
}
TEST_P(UdpSessionAccountingTest, SharedCapacityRejectsWithoutAcceptedLoss) {
  ASSERT_TRUE(open(true));
  a = connect(first);
  b = connect(second);
  // Use try admission for deterministic rejection without pumping the executor.
  ASSERT_TRUE(server->try_send_to(a, std::string(800, 'a')).accepted());
  EXPECT_FALSE(server->try_send_to(b, std::string(800, 'b')).accepted());
  EXPECT_EQ(peer(a).accepted.requests, 1u);
  EXPECT_EQ(peer(b).accepted.requests, 0u);
  EXPECT_EQ(peer(b).queue_pressure.discarded_before_write.requests, 0u);
  EXPECT_EQ(total().accepted.requests, 1u);
  settle();
}
TEST_P(UdpSessionAccountingTest, StopRetainsTotalsAndRestartStartsFreshForBothChannelOwners) {
  for (bool factory : {false, true}) {
    ASSERT_TRUE(open(false, factory));
    a = connect(first);
    ASSERT_TRUE(send(a).accepted());
    settle();
    stop();
    EXPECT_EQ(total().written.bytes, 7u);
    EXPECT_FALSE(server->client_stats(a));
    stop();
    EXPECT_EQ(total().written.bytes, 7u);
    auto ready = server->start();
    ASSERT_TRUE(pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
    ASSERT_TRUE(ready.get());
    EXPECT_EQ(total().accepted.requests, 0u);
    a = connect(first);
    ASSERT_TRUE(send(a, "abc").accepted());
    settle();
    stop();
    EXPECT_EQ(total().written.bytes, 3u);
    server->reset_stats();
    EXPECT_EQ(total().accepted.requests, 0u);
    server.reset();
    channel.reset();
  }
}

TEST_P(UdpSessionAccountingTest, ExpiryRemovesQueuedAndReliablePendingStorage) {
  ASSERT_TRUE(open(true));
  a = connect(first);
  virtual_now += 100ms;
  b = connect(second);
  server->idle_timeout(100ms);
  // Plain writes may enter the Reliable pending queue. All admissions precede
  // executor progress, and their enqueue handlers precede the completion gate.
  ASSERT_TRUE(server->send_to_blocking(a, std::string(800, 'a')).accepted());
  ASSERT_TRUE(server->send_to_blocking(a, std::string(800, 'b')).accepted());
  ASSERT_TRUE(server->send_to_blocking(a, std::string(800, 'c')).accepted());
  ASSERT_TRUE(server->send_to_blocking(b, std::string(800, 'd')).accepted());
  after_start = [&] {
    EXPECT_GT(server->client_stats(a)->pending_bytes, 0u);
    virtual_now += 50ms;
    bool expired = false;
    std::jthread reaper([&] { expired = pump([&] { return !server->client_stats(a).has_value(); }); });
    reaper.join();
    EXPECT_TRUE(expired);
    EXPECT_EQ(total().session_expiry.discarded_before_write.requests, 2u);
    EXPECT_EQ(total().outstanding.requests, 2u);
  };
  transport::detail::g_udp_write_completion_hook = started;
  settle();
  EXPECT_EQ(total().written.bytes, 1600u);
  EXPECT_EQ(total().session_expiry.discarded_before_write.bytes, 1600u);
  EXPECT_EQ(peer(b).written.bytes, 800u);
  EXPECT_EQ(server->client_stats(b)->pending_bytes, 0u);
  EXPECT_EQ(server->client_stats(b)->queued_bytes, 0u);
  EXPECT_EQ(server->stats().pending_bytes, 0u);
  EXPECT_EQ(server->stats().queued_bytes, 0u);
}
TEST_P(UdpSessionAccountingTest, SocketFailureAndResetKeepPeerProjectionConsistent) {
  ASSERT_TRUE(open());
  a = connect(first);
  b = connect(second);
  transport::detail::g_udp_write_initiation_hook = +[] { throw std::runtime_error("initiation failure"); };
  ASSERT_TRUE(send(a).accepted());
  ASSERT_TRUE(send(b).accepted());
  settle();
  EXPECT_EQ(peer(a).connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(peer(b).connection_loss.discarded_before_write.requests, 1u);
  EXPECT_EQ(total().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(total().connection_loss.discarded_before_write.requests, 1u);
  server->reset_stats();
  EXPECT_EQ(peer(a).accepted.requests, 0u);
  EXPECT_EQ(peer(b).accepted.requests, 0u);
  EXPECT_EQ(server->client_stats(a)->messages_accepted, 0u);
  EXPECT_EQ(server->client_stats(b)->messages_accepted, 0u);
}
TEST_P(UdpSessionAccountingTest, StopAtFinalAdmissionPreservesStoppingReason) {
  ASSERT_TRUE(open());
  a = connect(first);
  after_start = [&] { channel->stop(); };
  transport::detail::g_udp_pinned_write_hook = started;
  bool done = false;
  net::post(io, [&] {
    const auto result = send(a);
    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reason(), wrapper::SendRejection::Stopping);
    done = true;
  });
  ASSERT_TRUE(pump([&] { return done; }));
  EXPECT_EQ(total().accepted.requests, 0u);
}
INSTANTIATE_TEST_SUITE_P(Admissions, UdpSessionAccountingTest, ::testing::Range(0, 3));
}  // namespace
