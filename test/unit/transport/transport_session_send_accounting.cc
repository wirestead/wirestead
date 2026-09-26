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
#include <functional>
#include <memory>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include "wirestead/transport/tcp_server/tcp_server_session.hpp"
#include "wirestead/transport/uds/uds_server_session.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using Error = boost::system::error_code;
using Handler = std::function<void(const Error&, size_t)>;

struct EndpointState {
  Handler read, write;
  size_t bytes = 0, starts = 0;
  bool inline_write = false, throw_write = false;
  void start(size_t n, Handler h) {
    ++starts;
    bytes = n;
    if (throw_write) throw std::runtime_error("write initiation failure");
    if (inline_write)
      h({}, n);
    else
      write = std::move(h);
  }
  void complete(Error ec, size_t n) {
    auto h = std::move(write);
    ASSERT_TRUE(h);
    h(ec, n);
  }
};

template <bool Tcp>
class Endpoint : public std::conditional_t<Tcp, interface::TcpSocketInterface, interface::UdsSocketInterface>,
                 public EndpointState {
 public:
  using Protocol = std::conditional_t<Tcp, net::ip::tcp, net::local::stream_protocol>;
  void close(Error& ec) override {
    ec.clear();
    auto r = std::move(read);
    auto w = std::move(write);
    if (r) r(net::error::operation_aborted, 0);
    if (w) w(net::error::operation_aborted, 0);
  }
  bool is_open() const { return true; }
  void async_connect(const net::local::stream_protocol::endpoint&, std::function<void(const Error&)> h) { h({}); }
  void shutdown(typename Protocol::socket::shutdown_type, Error& ec) override { ec.clear(); }
  typename Protocol::endpoint remote_endpoint(Error& ec) const override {
    ec.clear();
    return {};
  }
  void async_read_some(const net::mutable_buffer&, Handler h) override { read = std::move(h); }
  void async_write(const net::const_buffer& b, Handler h) override { start(b.size(), std::move(h)); }
  void async_write(const std::vector<net::const_buffer>& buffers, Handler h) override {
    size_t n = 0;
    for (const auto& b : buffers) n += b.size();
    start(n, std::move(h));
  }
};

class SessionSendAccountingTest : public ::testing::TestWithParam<std::tuple<bool, int>> {
 protected:
  net::io_context io;
  EndpointState* endpoint = nullptr;
  using Sessions =
      std::variant<std::shared_ptr<transport::TcpServerSession>, std::shared_ptr<transport::UdsServerSession>>;
  Sessions session;
  bool started = false;
  int input() const { return std::get<1>(GetParam()); }
  template <class F>
  decltype(auto) visit(F&& f) {
    return std::visit([&](auto& s) -> decltype(auto) { return f(*s); }, session);
  }
  void drain() {
    io.restart();
    io.poll();
  }
  template <bool Tcp>
  void create(bool pressure, bool reliable_pressure) {
    using S = std::conditional_t<Tcp, transport::TcpServerSession, transport::UdsServerSession>;
    auto fake = std::make_unique<Endpoint<Tcp>>();
    endpoint = fake.get();
    const size_t high = pressure ? 1024 : 4 * 1024 * 1024;
    const auto strategy = pressure && !reliable_pressure ? base::constants::BackpressureStrategy::BestEffort
                                                         : base::constants::BackpressureStrategy::Reliable;
    session = std::make_shared<S>(io, std::move(fake), high, 0, strategy, input() != 1);
  }
  bool connect(bool pressure = false, bool unused = false, bool reliable_pressure = false) {
    (void)unused;
    if (std::get<0>(GetParam()))
      create<true>(pressure, reliable_pressure);
    else
      create<false>(pressure, reliable_pressure);
    visit([](auto& s) { s.start(); });
    started = true;
    drain();
    return alive();
  }
  bool alive() {
    return visit([](auto& s) { return s.alive(); });
  }
  void stop() {
    visit([](auto& s) { s.stop(); });
  }
  void reset_stats() {
    visit([](auto& s) { s.reset_stats(); });
  }
  wrapper::RuntimeStats runtime_stats() {
    return visit([](auto& s) { return s.stats(); });
  }
  size_t limit() {
    return visit([](auto& s) { return s.write_queue_limit(); });
  }
  bool plain(size_t n) {
    return visit([&](auto& s) { return s.async_write_move(std::vector<uint8_t>(n)); });
  }
  void TearDown() override {
    if (started) {
      stop();
      drain();
    }
  }
  wrapper::SendAccounting stats() { return *runtime_stats().send_accounting; }
  bool send(size_t n = 8) {
    return visit([&](auto& s) {
      std::vector<uint8_t> b(n, 42);
      switch (input()) {
        case 0:
        case 1:
          return s.async_write_copy(memory::ConstByteSpan(b.data(), b.size()));
        case 2:
          return s.async_write_move(std::move(b));
        case 3:
          return s.async_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
        case 4:
          return s.async_try_write_copy(memory::ConstByteSpan(b.data(), b.size()));
        case 5:
          return s.async_try_write_move(std::move(b));
        default:
          return s.async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
      }
    });
  }
  void expect_conserved() {
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

TEST_P(SessionSendAccountingTest, StopBeforeEnqueue) {
  ASSERT_TRUE(connect());
  net::post(io.get_executor(), [&] {
    EXPECT_TRUE(send());
    stop();
  });
  drain();
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 8u);
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(endpoint->starts, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, StopActiveAndQueuedIgnoresLateSuccess) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  auto late = std::move(endpoint->write);
  net::post(io.get_executor(), [&] {
    stop();
    late({}, 8);
    late = {};
  });
  drain();
  const auto s = stats();
  EXPECT_EQ(s.explicit_stop.aborted_during_write.bytes, 8u);
  EXPECT_EQ(s.explicit_stop.discarded_before_write.bytes, 8u);
  EXPECT_EQ(s.written.requests, 0u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, PartialGatherCountsLogicalRequestsAndPrefix) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  endpoint->complete({}, 8);
  drain();
  ASSERT_EQ(endpoint->bytes, 24u);
  endpoint->complete(net::error::connection_reset, 11);
  drain();
  const auto s = stats();
  EXPECT_EQ(s.accepted.requests, 4u);
  EXPECT_EQ(s.written.requests, 2u);
  EXPECT_EQ(s.written.bytes, 16u);
  EXPECT_EQ(s.confirmed_written_bytes, 19u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.requests, 2u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.bytes, 16u);
  EXPECT_EQ(s.outstanding.requests, 0u);
  EXPECT_FALSE(alive());
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, InlineCompletionDoesNotDeadlock) {
  ASSERT_TRUE(connect());
  endpoint->inline_write = true;
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  EXPECT_EQ(stats().written.requests, 2u);
  EXPECT_EQ(stats().confirmed_written_bytes, 16u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, InitiationFailureTerminatesActiveAndPosted) {
  ASSERT_TRUE(connect());
  endpoint->throw_write = true;
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  EXPECT_FALSE(alive());
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, ResetActiveAndQueuedExcludesOldEpoch) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  reset_stats();
  ASSERT_TRUE(send());
  drain();
  endpoint->complete({}, 8);
  drain();
  ASSERT_EQ(endpoint->bytes, 16u);
  endpoint->complete({}, 16);
  drain();
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().written.requests, 1u);
  EXPECT_EQ(stats().confirmed_written_bytes, 8u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, ResetBeforePostedEnqueueExcludesOldEpoch) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  reset_stats();
  drain();
  endpoint->complete({}, 8);
  drain();
  EXPECT_EQ(stats().accepted.requests, 0u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(stats().confirmed_written_bytes, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, ReadLossOwnsOutstandingBeforeLateWrite) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  auto late = std::move(endpoint->write);
  auto read = std::move(endpoint->read);
  read(net::error::connection_reset, 0);
  read = {};
  drain();
  late({}, 8);
  late = {};
  drain();
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(stats().confirmed_written_bytes, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, WriteEofIsTerminal) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  endpoint->complete(net::error::eof, 3);
  drain();
  EXPECT_FALSE(alive());
  EXPECT_EQ(stats().confirmed_written_bytes, 3u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, ShortSuccessIsTerminal) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  endpoint->complete({}, 3);
  drain();
  EXPECT_FALSE(alive());
  EXPECT_EQ(stats().confirmed_written_bytes, 3u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, RejectionDoesNotCreateAcceptedRequest) {
  ASSERT_TRUE(connect(true));
  EXPECT_FALSE(send(0));
  ASSERT_TRUE(send(800));
  drain();
  // Try APIs fail above the high watermark; plain APIs fail above the hard limit.
  EXPECT_FALSE(send(input() >= 4 ? 800 : limit()));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 1u);
  EXPECT_EQ(stats().queue_pressure.discarded_before_write.requests, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, KeepLatestOnlyDiscardsRemovedQueueEntries) {
  ASSERT_TRUE(connect(true));
  ASSERT_TRUE(send(800));
  drain();
  ASSERT_TRUE(plain(400));
  drain();
  ASSERT_TRUE(plain(400));
  drain();
  ASSERT_TRUE(plain(400));
  drain();
  const auto s = stats();
  EXPECT_GT(s.queue_pressure.discarded_before_write.requests, 0u);
  EXPECT_EQ(s.queue_pressure.aborted_during_write.requests, 0u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.requests, 0u);
  expect_conserved();
}

TEST_P(SessionSendAccountingTest, ReliablePendingIsDiscardedAtStop) {
  ASSERT_TRUE(connect(true, false, true));
  ASSERT_TRUE(send(800));
  drain();
  ASSERT_TRUE(plain(800));
  drain();
  ASSERT_TRUE(plain(800));
  drain();
  EXPECT_GT(runtime_stats().pending_bytes, 0u);
  net::post(io.get_executor(), [&] { stop(); });
  drain();
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.bytes, 800u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 1600u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, DiscardedCompletionStillAllowsStop) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  endpoint->write = {};
  drain();
  net::post(io.get_executor(), [&] { stop(); });
  drain();
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  expect_conserved();
}
TEST_P(SessionSendAccountingTest, OffExecutorReadCompletionIsSerialized) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  auto read = std::move(endpoint->read);
  std::thread other([&] { read(net::error::connection_reset, 0); });
  other.join();
  read = {};
  // The external completion must queue session state changes onto its strand.
  EXPECT_TRUE(alive());
  EXPECT_EQ(stats().outstanding.requests, 1u);
  drain();
  EXPECT_FALSE(alive());
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  expect_conserved();
}
INSTANTIATE_TEST_SUITE_P(TcpAndUdsInputs, SessionSendAccountingTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Range(0, 7)));
}  // namespace
