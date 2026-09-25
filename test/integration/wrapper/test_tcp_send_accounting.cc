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
#include <functional>
#include <memory>
#include <stdexcept>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"

namespace {
using namespace wirestead;
using namespace std::chrono_literals;
namespace net = boost::asio;
using tcp = net::ip::tcp;
std::function<void()> after_write_start;
void write_started() {
  auto action = after_write_start;
  if (action) action();
}

class TcpSendAccountingTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  tcp::acceptor acceptor{io, tcp::endpoint(tcp::v4(), 0)};
  tcp::socket peer{io};
  std::shared_ptr<transport::TcpClient> client;

  void TearDown() override {
    transport::detail::g_tcp_write_started_hook = nullptr;
    transport::detail::g_tcp_write_initiation_hook = nullptr;
    after_write_start = {};
    if (client) test::stop_with_context(client, io);
  }
  bool connect(bool best_effort = false) {
    // Bound the advertised receive window before handshake. The peer never
    // reads, so the 1 MiB loss probe cannot finish before the close gate.
    acceptor.set_option(net::socket_base::receive_buffer_size(1024));
    auto accepted = std::make_shared<bool>(false);
    acceptor.async_accept(peer, [accepted](auto ec) { *accepted = !ec; });
    config::TcpClientConfig cfg;
    cfg.port = acceptor.local_endpoint().port();
    cfg.enable_memory_pool = GetParam() != 1;
    cfg.max_retries = 0;
    cfg.send_buffer_size = 1024;
    cfg.backpressure_threshold = best_effort ? 1024 : 4 * 1024 * 1024;
    if (best_effort) cfg.backpressure_strategy = base::constants::BackpressureStrategy::BestEffort;
    client = transport::TcpClient::create(cfg, io);
    client->start();
    return pump([&] { return *accepted && client->is_connected(); });
  }
  template <typename Predicate>
  bool pump(Predicate ready) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!ready()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  }
  wrapper::SendAccounting stats() { return *client->stats().send_accounting; }
  bool send(size_t size = 8) {
    std::vector<uint8_t> bytes(size, 42);
    switch (GetParam()) {
      case 0:
      case 1:
        return client->async_write_copy(memory::ConstByteSpan(bytes.data(), bytes.size()));
      case 2:
        return client->async_write_move(std::move(bytes));
      case 3:
        return client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(bytes)));
      case 4:
        return client->async_try_write_copy(memory::ConstByteSpan(bytes.data(), bytes.size()));
      case 5:
        return client->async_try_write_move(std::move(bytes));
      default:
        return client->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(bytes)));
    }
  }
};

TEST_P(TcpSendAccountingTest, StopBeforeEnqueueCountsOneDiscardAndNoAbort) {
  ASSERT_TRUE(connect());
  bool done = false;
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send());
    client->stop();
    done = true;
  });
  ASSERT_TRUE(pump([&] { return done; }));
  test::stop_with_context(client, io);
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 1u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.requests, 1u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.bytes, 8u);
  EXPECT_EQ(s.explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_EQ(s.written.requests, 0u);
}

TEST_P(TcpSendAccountingTest, StopAfterHandoffSeparatesActiveFromPendingAndIgnoresLateCompletion) {
  ASSERT_TRUE(connect());
  after_write_start = [&] {
    after_write_start = {};
    client->stop();
  };
  transport::detail::g_tcp_write_started_hook = &write_started;
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send(11));
    EXPECT_TRUE(send(13));
    EXPECT_TRUE(send(17));
  });
  ASSERT_TRUE(pump([&] { return stats().explicit_stop.aborted_during_write.requests == 1; }));
  test::stop_with_context(client, io);
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 3u);
  EXPECT_EQ(s.explicit_stop.aborted_during_write.bytes, 11u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.requests, 2u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.bytes, 30u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_EQ(s.written.requests, 0u);
  EXPECT_EQ(s.confirmed_written_bytes, 0u);
}

TEST_P(TcpSendAccountingTest, GatherCompletionCountsRequestsRatherThanOperations) {
  ASSERT_TRUE(connect());
  auto shared = std::make_shared<const std::vector<uint8_t>>(8, 42);
  net::post(client->get_executor(), [&, shared] {
    for (int i = 0; i < 7; ++i) {
      if (GetParam() == 3)
        EXPECT_TRUE(client->async_write_shared(shared));
      else if (GetParam() == 6)
        EXPECT_TRUE(client->async_try_write_shared(shared));
      else
        EXPECT_TRUE(send());
    }
  });
  ASSERT_TRUE(pump([&] { return stats().written.requests == 7; }));
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 7u);
  EXPECT_EQ(s.written.bytes, 56u);
  EXPECT_EQ(s.confirmed_written_bytes, 56u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_LT(client->stats().messages_sent, s.written.requests);
}

TEST_P(TcpSendAccountingTest, ResetWithPendingRequestsDoesNotContaminateNewEpoch) {
  ASSERT_TRUE(connect());
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send(8));
    client->reset_stats();
    EXPECT_TRUE(send(13));
  });
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 1u);
  EXPECT_EQ(s.written.bytes, 13u);
  EXPECT_EQ(s.confirmed_written_bytes, 13u);
  EXPECT_EQ(s.outstanding.requests, 0u);
}

TEST_P(TcpSendAccountingTest, ResetDuringActiveWriteExcludesItsLateCompletion) {
  ASSERT_TRUE(connect());
  after_write_start = [&] {
    after_write_start = {};
    client->reset_stats();
    EXPECT_TRUE(send(13));
  };
  transport::detail::g_tcp_write_started_hook = &write_started;
  EXPECT_TRUE(send(8));
  ASSERT_TRUE(pump([&] { return stats().written.requests == 1; }));
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 1u);
  EXPECT_EQ(s.written.bytes, 13u);
  EXPECT_EQ(s.confirmed_written_bytes, 13u);
  EXPECT_EQ(s.outstanding.requests, 0u);
}

TEST_P(TcpSendAccountingTest, RejectedRequestsDoNotEnterAcceptedAccounting) {
  ASSERT_TRUE(connect());
  EXPECT_FALSE(send(0));
  test::stop_with_context(client, io);
  EXPECT_FALSE(send(8));
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 0u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.requests, 0u);
  EXPECT_EQ(s.outstanding.requests, 0u);
}

TEST_P(TcpSendAccountingTest, PeerLossAbortsActiveAndDiscardsQueuedRequests) {
  ASSERT_TRUE(connect());
  after_write_start = [&] {
    after_write_start = {};
    boost::system::error_code ec;
    peer.set_option(net::socket_base::linger(true, 0), ec);
    peer.close(ec);
  };
  transport::detail::g_tcp_write_started_hook = &write_started;
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send(1024 * 1024));
    EXPECT_TRUE(send(13));
    EXPECT_TRUE(send(17));
  });
  ASSERT_TRUE(pump([&] { return stats().connection_loss.aborted_during_write.requests == 1; }));
  test::stop_with_context(client, io);
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 3u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 1024u * 1024u);
  EXPECT_EQ(s.connection_loss.discarded_before_write.requests, 2u);
  EXPECT_EQ(s.connection_loss.discarded_before_write.bytes, 30u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_EQ(s.explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.requests, 0u);
}

TEST_P(TcpSendAccountingTest, KeepLatestLossExcludesRejectedTryAdmission) {
  ASSERT_TRUE(connect(true));
  after_write_start = [&] {
    after_write_start = {};
    // Try admission refuses new input; it is not a post-acceptance loss.
    EXPECT_FALSE(client->async_try_write_move(std::vector<uint8_t>(8, 1)));
  };
  transport::detail::g_tcp_write_started_hook = &write_started;
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(client->async_write_move(std::vector<uint8_t>(1024, 1)));
    EXPECT_TRUE(client->async_write_move(std::vector<uint8_t>(512, 2)));
    EXPECT_TRUE(client->async_write_move(std::vector<uint8_t>(600, 3)));
  });
  ASSERT_TRUE(pump([&] { return stats().written.requests == 2; }));
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 3u);
  EXPECT_EQ(s.queue_pressure.discarded_before_write.requests, 1u);
  EXPECT_EQ(s.queue_pressure.discarded_before_write.bytes, 512u);
  EXPECT_EQ(s.written.bytes, 1624u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_EQ(s.queue_pressure.aborted_during_write.requests, 0u);
}

TEST_P(TcpSendAccountingTest, FailedInitiationTerminatesAcceptedRequestsWithoutPendingIoLeak) {
  ASSERT_TRUE(connect());
  transport::detail::g_tcp_write_initiation_hook = +[] { throw std::runtime_error("injected initiation failure"); };
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send(11));
    EXPECT_TRUE(send(13));
  });
  ASSERT_TRUE(pump([&] { return stats().connection_loss.aborted_during_write.requests == 1; }));
  test::stop_with_context(client, io);
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 2u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 11u);
  EXPECT_EQ(s.connection_loss.discarded_before_write.bytes, 13u);
  EXPECT_EQ(s.outstanding.requests, 0u);
}
INSTANTIATE_TEST_SUITE_P(AllAdmissionFamilies, TcpSendAccountingTest, ::testing::Range(0, 7));
}  // namespace
