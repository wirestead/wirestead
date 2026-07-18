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
#include <memory>
#include <vector>

#include "fake_tcp_socket.hpp"
#include "wirestead/interface/itcp_socket.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using wirestead::test::FakeTcpSocket;
using namespace std::chrono_literals;

namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

// Unlike FakeTcpSocket, whose async_write always auto-completes immediately,
// this stub withholds the write completion indefinitely - modeling a real
// client that has stopped reading, so the outstanding write never finishes
// on its own. This is required to reproduce jwsung91/wirestead#452: without
// it, the fake socket's own auto-completion would drain the queue through
// the normal (already-correct) path, masking whether do_close() itself
// clears backpressure on disconnect.
class HangingWriteSocket : public FakeTcpSocket {
 public:
  explicit HangingWriteSocket(net::io_context& ioc) : FakeTcpSocket(ioc) {}

  void async_write(const net::const_buffer&,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    write_handler_ = std::move(handler);
  }

  bool has_write_handler() const { return !!write_handler_; }

 private:
  std::function<void(const boost::system::error_code&, std::size_t)> write_handler_;
};

}  // namespace

TEST(TransportTcpServerSessionTest, QueueLimitDropsMessage) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;  // 1KB bp_high; bp_limit = max(4KB, 4MB) = 4MB

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> backpressure_seen{false};
  session->on_backpressure([&](size_t) { backpressure_seen = true; });

  session->start();
  EXPECT_TRUE(session->alive());

  // 5MB exceeds bp_limit (4MB): message rejected synchronously, backpressure DOES NOT fire as it's not queued
  std::vector<uint8_t> huge(5 * 1024 * 1024, 0xAA);
  EXPECT_FALSE(session->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size())));
  auto stats = session->stats();
  EXPECT_EQ(stats.failed_sends, 1u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);

  ioc.run_for(50ms);

  EXPECT_FALSE(backpressure_seen.load());
  EXPECT_TRUE(session->alive());
}

TEST(TransportTcpServerSessionTest, MoveWriteRespectsQueueLimit) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> backpressure_seen{false};
  session->on_backpressure([&](size_t) { backpressure_seen = true; });

  session->start();
  EXPECT_TRUE(session->alive());

  // 5MB exceeds bp_limit (4MB): message rejected synchronously, backpressure DOES NOT fire
  std::vector<uint8_t> huge(5 * 1024 * 1024, 0xBB);
  EXPECT_FALSE(session->async_write_move(std::move(huge)));
  auto stats = session->stats();
  EXPECT_EQ(stats.failed_sends, 1u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);

  ioc.run_for(50ms);

  EXPECT_FALSE(backpressure_seen.load());
  EXPECT_TRUE(session->alive());
}

TEST(TransportTcpServerSessionTest, SharedWriteRespectsQueueLimit) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> backpressure_seen{false};
  session->on_backpressure([&](size_t) { backpressure_seen = true; });

  session->start();
  EXPECT_TRUE(session->alive());

  // 5MB exceeds bp_limit (4MB): message rejected synchronously, backpressure DOES NOT fire
  auto huge = std::make_shared<const std::vector<uint8_t>>(5 * 1024 * 1024, 0xCC);
  EXPECT_FALSE(session->async_write_shared(huge));
  auto stats = session->stats();
  EXPECT_EQ(stats.failed_sends, 1u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);

  ioc.run_for(50ms);

  EXPECT_FALSE(backpressure_seen.load());
  EXPECT_TRUE(session->alive());
}

TEST(TransportTcpServerSessionTest, BestEffortDropStatsExcludeInFlightWrite) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold, 0,
                                                    base::constants::BackpressureStrategy::BestEffort);

  session->start();
  ASSERT_TRUE(session->alive());

  std::vector<uint8_t> payload(bp_threshold * 2, 0xAB);
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(payload)));
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(payload)));
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(payload)));

  ioc.run_for(50ms);

  auto stats = session->stats();
  EXPECT_EQ(stats.messages_accepted, 3u);
  EXPECT_EQ(stats.bytes_accepted, payload.size() * 3);
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.dropped_messages, 1u);
  EXPECT_EQ(stats.dropped_bytes, payload.size());
}

TEST(TransportTcpServerSessionTest, BackpressureReliefAfterDrain) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::vector<size_t> events;
  session->on_backpressure([&](size_t queued) { events.push_back(queued); });

  session->start();
  EXPECT_TRUE(session->alive());

  std::vector<uint8_t> payload(bp_threshold * 2, 0xDD);  // exceed threshold, far below limit
  session->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));

  ioc.run_for(50ms);

  ASSERT_GE(events.size(), 2u);
  EXPECT_GE(events.front(), bp_threshold);
  EXPECT_LE(events.back(), bp_threshold / 2);
}

// Regression test for jwsung91/wirestead#452: if a client disconnects (read
// error) while backpressure is active for its session, do_close() must
// unconditionally clear it and fire on_backpressure - otherwise a caller
// blocked in send_to_blocking() for this client would never wake up, since
// nothing else will ever call report_backpressure() again once the session
// is gone.
TEST(TransportTcpServerSessionTest, BackpressureClearsOnDisconnectWhileActive) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  // HangingWriteSocket never completes a write on its own, modeling a client
  // that has stopped reading - the only way backpressure can ever clear is
  // via do_close()'s drain, not via a write eventually finishing.
  auto socket = std::make_unique<HangingWriteSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::vector<size_t> events;
  session->on_backpressure([&](size_t queued) { events.push_back(queued); });

  session->start();
  while (!socket_raw->has_handler()) {
    ioc.run_for(std::chrono::milliseconds(1));
  }

  std::vector<uint8_t> payload(bp_threshold * 2, 0xEE);
  ASSERT_TRUE(session->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  ioc.run_for(50ms);

  ASSERT_GE(events.size(), 1u);
  EXPECT_GE(events.back(), bp_threshold);
  ASSERT_TRUE(socket_raw->has_write_handler());  // write is stuck in-flight, never completed

  // Simulate an abrupt disconnect (client gone) while the write is still
  // stuck and backpressure is still active. Nothing but do_close() itself
  // can ever clear it now.
  socket_raw->emit_read(0, boost::asio::error::eof);
  ioc.restart();
  ioc.run_for(50ms);

  EXPECT_EQ(events.back(), 0u);
  EXPECT_FALSE(session->alive());
}

TEST(TransportTcpServerSessionTest, OnBytesExceptionClosesSession) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> closed{false};
  session->on_close([&]() { closed = true; });
  session->on_bytes([](memory::ConstByteSpan) { throw std::runtime_error("boom"); });

  session->start();
  // Allow start_read to register handler
  while (!socket_raw->has_handler()) {
    ioc.run_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(session->alive());

  // Ensure start_read has been called so read_handler_ is set
  ioc.run_for(5ms);

  // Trigger read handler to invoke throwing callback
  socket_raw->emit_read(4);

  ioc.restart();
  ioc.run_for(50ms);

  EXPECT_TRUE(closed.load());
  EXPECT_FALSE(session->alive());
}
