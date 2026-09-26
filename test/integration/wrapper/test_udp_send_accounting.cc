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
TEST_P(UdpSendAccountingTest, KeepLatestDisposesOnlyQueuedDatagrams) {
  ASSERT_TRUE(open(true));
  ASSERT_TRUE(send(800));
  std::vector<uint8_t> b(400);
  for (int i = 0; i != 3; ++i)
    ASSERT_TRUE(channel->async_write_to(memory::ConstByteSpan(b.data(), b.size()), peer.local_endpoint()));
  ASSERT_TRUE(pump([&] { return stats().outstanding.requests == 0; }));
  EXPECT_GT(stats().queue_pressure.discarded_before_write.requests, 0u);
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
