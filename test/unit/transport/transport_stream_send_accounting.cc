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
#include <tuple>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/interface/iuds_socket.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/uds/uds_client.hpp"

namespace {
using namespace wirestead;
namespace net = boost::asio;
using Error = boost::system::error_code;
using Handler = std::function<void(const Error&, size_t)>;

// The same controlled endpoint exercises both adapters, including interfaces
// that invoke completions inline or destroy them without invoking them.
class Endpoint : public interface::SerialPortInterface, public interface::UdsSocketInterface {
 public:
  Handler read, write;
  size_t bytes = 0;
  size_t starts = 0;
  bool opened = false, inline_write = false, throw_write = false;
  void open(const std::string&, Error& ec) override {
    ec.clear();
    opened = true;
  }
  bool is_open() const override { return opened; }
  void close(Error& ec) override {
    ec.clear();
    opened = false;
    auto r = std::move(read);
    auto w = std::move(write);
    if (r) r(net::error::operation_aborted, 0);
    if (w) w(net::error::operation_aborted, 0);
  }
  void set_option(const net::serial_port_base::baud_rate&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::character_size&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::stop_bits&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::parity&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::flow_control&, Error& ec) override { ec.clear(); }
  void async_connect(const net::local::stream_protocol::endpoint&, std::function<void(const Error&)> h) override {
    opened = true;
    h({});
  }
  void shutdown(net::local::stream_protocol::socket::shutdown_type, Error& ec) override { ec.clear(); }
  net::local::stream_protocol::endpoint remote_endpoint(Error& ec) const override {
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

class StreamSendAccountingTest : public ::testing::TestWithParam<std::tuple<bool, int>> {
 protected:
  net::io_context io;
  Endpoint* endpoint = nullptr;
  std::shared_ptr<interface::Channel> client;
  bool serial() const { return std::get<0>(GetParam()); }
  int input() const { return std::get<1>(GetParam()); }
  void drain() {
    io.restart();
    io.poll();
  }
  bool connect(bool pressure = false, bool retry = false, bool reliable_pressure = false) {
    auto fake = std::make_unique<Endpoint>();
    endpoint = fake.get();
    const size_t high = pressure ? 1024 : 4 * 1024 * 1024;
    const auto strategy = pressure && !reliable_pressure ? base::constants::BackpressureStrategy::BestEffort
                                                         : base::constants::BackpressureStrategy::Reliable;
    if (serial()) {
      config::SerialConfig cfg;
      cfg.device = "/fake/serial";
      cfg.enable_memory_pool = input() != 1;
      cfg.backpressure_threshold = high;
      cfg.backpressure_strategy = strategy;
      cfg.reopen_on_error = retry;
      cfg.retry_interval_ms = 1;
      client = transport::Serial::create(cfg, std::move(fake), io);
    } else {
      config::UdsClientConfig cfg;
      cfg.socket_path = "/fake/uds";
      cfg.enable_memory_pool = input() != 1;
      cfg.backpressure_threshold = high;
      cfg.backpressure_strategy = strategy;
      cfg.max_retries = retry ? 2 : 0;
      cfg.retry_interval_ms = 1;
      client = transport::UdsClient::create(cfg, std::move(fake), io);
    }
    client->start();
    drain();
    return client->is_connected();
  }
  void TearDown() override {
    if (client) test::stop_with_context(client, io);
  }
  wrapper::SendAccounting stats() { return *client->stats().send_accounting; }
  bool send(size_t n = 8) {
    std::vector<uint8_t> b(n, 42);
    switch (input()) {
      case 0:
      case 1:
        return client->async_write_copy(memory::ConstByteSpan(b.data(), b.size()));
      case 2:
        return client->async_write_move(std::move(b));
      case 3:
        return client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
      case 4:
        return client->async_try_write_copy(memory::ConstByteSpan(b.data(), b.size()));
      case 5:
        return client->async_try_write_move(std::move(b));
      default:
        return client->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(std::move(b)));
    }
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

TEST_P(StreamSendAccountingTest, StopBeforeEnqueue) {
  ASSERT_TRUE(connect());
  net::post(client->get_executor(), [&] {
    EXPECT_TRUE(send());
    client->stop();
  });
  drain();
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 8u);
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 0u);
  EXPECT_EQ(endpoint->starts, 0u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, StopActiveAndQueuedIgnoresLateSuccess) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  auto late = std::move(endpoint->write);
  net::post(client->get_executor(), [&] {
    client->stop();
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
TEST_P(StreamSendAccountingTest, PartialGatherCountsLogicalRequestsAndPrefix) {
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
  EXPECT_FALSE(client->is_connected());
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, InlineCompletionDoesNotDeadlock) {
  ASSERT_TRUE(connect());
  endpoint->inline_write = true;
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  EXPECT_EQ(stats().written.requests, 2u);
  EXPECT_EQ(stats().confirmed_written_bytes, 16u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, InitiationFailureTerminatesActiveAndPosted) {
  ASSERT_TRUE(connect());
  endpoint->throw_write = true;
  ASSERT_TRUE(send());
  ASSERT_TRUE(send());
  drain();
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  EXPECT_FALSE(client->is_connected());
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, ResetActiveAndQueuedExcludesOldEpoch) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  client->reset_stats();
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
TEST_P(StreamSendAccountingTest, ResetBeforePostedEnqueueExcludesOldEpoch) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  client->reset_stats();
  drain();
  endpoint->complete({}, 8);
  drain();
  EXPECT_EQ(stats().accepted.requests, 0u);
  EXPECT_EQ(stats().written.requests, 0u);
  EXPECT_EQ(stats().confirmed_written_bytes, 0u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, ReadLossOwnsOutstandingBeforeLateWrite) {
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
TEST_P(StreamSendAccountingTest, ReconnectIgnoresOldCompletion) {
  ASSERT_TRUE(connect(false, true));
  ASSERT_TRUE(send());
  drain();
  auto late = std::move(endpoint->write);
  auto read = std::move(endpoint->read);
  read(net::error::connection_reset, 0);
  read = {};
  drain();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!client->is_connected() && std::chrono::steady_clock::now() < deadline) {
    io.restart();
    io.run_one_for(std::chrono::milliseconds(5));
  }
  // Release the retained operation even if reconnect fails, so teardown can finish.
  if (!client->is_connected()) {
    late({}, 8);
    late = {};
    drain();
    FAIL() << "reconnect did not complete";
  }
  ASSERT_TRUE(send());
  drain();
  late({}, 8);
  late = {};
  drain();
  EXPECT_EQ(stats().outstanding.requests, 1u);
  endpoint->complete({}, 8);
  drain();
  EXPECT_EQ(stats().accepted.requests, 2u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().written.requests, 1u);
  EXPECT_EQ(stats().confirmed_written_bytes, 8u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, WriteEofIsTerminal) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  endpoint->complete(net::error::eof, 3);
  drain();
  EXPECT_FALSE(client->is_connected());
  EXPECT_EQ(stats().confirmed_written_bytes, 3u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, ShortSuccessIsTerminal) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  ASSERT_TRUE(send());
  drain();
  endpoint->complete({}, 3);
  drain();
  EXPECT_FALSE(client->is_connected());
  EXPECT_EQ(stats().confirmed_written_bytes, 3u);
  EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().connection_loss.discarded_before_write.requests, 1u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, RejectionDoesNotCreateAcceptedRequest) {
  ASSERT_TRUE(connect(true));
  EXPECT_FALSE(send(0));
  ASSERT_TRUE(send(800));
  drain();
  // Try APIs fail above the high watermark; plain APIs fail above the hard limit.
  EXPECT_FALSE(send(input() >= 4 ? 800 : *client->write_queue_limit()));
  EXPECT_EQ(stats().accepted.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 1u);
  EXPECT_EQ(stats().queue_pressure.discarded_before_write.requests, 0u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, KeepLatestOnlyDiscardsRemovedQueueEntries) {
  ASSERT_TRUE(connect(true));
  ASSERT_TRUE(send(800));
  drain();
  ASSERT_TRUE(client->async_write_move(std::vector<uint8_t>(400)));
  drain();
  ASSERT_TRUE(client->async_write_move(std::vector<uint8_t>(400)));
  drain();
  ASSERT_TRUE(client->async_write_move(std::vector<uint8_t>(400)));
  drain();
  const auto s = stats();
  EXPECT_GT(s.queue_pressure.discarded_before_write.requests, 0u);
  EXPECT_EQ(s.queue_pressure.aborted_during_write.requests, 0u);
  EXPECT_EQ(s.connection_loss.aborted_during_write.requests, 0u);
  expect_conserved();
}

TEST_P(StreamSendAccountingTest, ReliablePendingIsDiscardedAtStop) {
  ASSERT_TRUE(connect(true, false, true));
  ASSERT_TRUE(send(800));
  drain();
  ASSERT_TRUE(client->async_write_move(std::vector<uint8_t>(800)));
  drain();
  ASSERT_TRUE(client->async_write_move(std::vector<uint8_t>(800)));
  drain();
  EXPECT_GT(client->stats().pending_bytes, 0u);
  net::post(client->get_executor(), [&] { client->stop(); });
  drain();
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.bytes, 800u);
  EXPECT_EQ(stats().explicit_stop.discarded_before_write.bytes, 1600u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, DiscardedCompletionStillAllowsStop) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  endpoint->write = {};
  drain();
  net::post(client->get_executor(), [&] { client->stop(); });
  drain();
  EXPECT_EQ(stats().explicit_stop.aborted_during_write.requests, 1u);
  EXPECT_EQ(stats().outstanding.requests, 0u);
  expect_conserved();
}
TEST_P(StreamSendAccountingTest, ReadEofUsesTransportPolicy) {
  ASSERT_TRUE(connect());
  ASSERT_TRUE(send());
  drain();
  auto read = std::move(endpoint->read);
  read(net::error::eof, 0);
  read = {};
  drain();
  if (serial()) {
    EXPECT_TRUE(client->is_connected());
    EXPECT_TRUE(endpoint->read);
    EXPECT_EQ(stats().outstanding.requests, 1u);
    endpoint->complete({}, 8);
    drain();
    EXPECT_EQ(stats().written.requests, 1u);
    EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 0u);
  } else {
    EXPECT_FALSE(client->is_connected());
    EXPECT_EQ(stats().outstanding.requests, 0u);
    EXPECT_EQ(stats().connection_loss.aborted_during_write.requests, 1u);
  }
  expect_conserved();
}
INSTANTIATE_TEST_SUITE_P(UdsAndSerialInputs, StreamSendAccountingTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Range(0, 7)));
}  // namespace
