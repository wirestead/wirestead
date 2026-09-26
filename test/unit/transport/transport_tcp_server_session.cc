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
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "fake_tcp_socket.hpp"
#include "wirestead/interface/itcp_socket.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using wirestead::test::FakeTcpSocket;
using namespace std::chrono_literals;

namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

struct StopSessionOnExit {
  std::shared_ptr<TcpServerSession> session;
  net::io_context& io;
  ~StopSessionOnExit() {
    session->stop();
    io.restart();
    io.poll();
  }
};

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

  void close(boost::system::error_code& ec) override {
    FakeTcpSocket::close(ec);
    if (auto handler = std::move(write_handler_)) handler(boost::asio::error::operation_aborted, 0);
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
  StopSessionOnExit cleanup{session, ioc};

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
  StopSessionOnExit cleanup{session, ioc};

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
  StopSessionOnExit cleanup{session, ioc};

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

TEST(TransportTcpServerSessionTest, BestEffortPreservesAcceptedWritesUnderPressure) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold, 0,
                                                    base::constants::BackpressureStrategy::BestEffort);
  StopSessionOnExit cleanup{session, ioc};

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
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
}

TEST(TransportTcpServerSessionTest, BackpressureReliefAfterDrain) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);
  StopSessionOnExit cleanup{session, ioc};

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
  StopSessionOnExit cleanup{session, ioc};

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

TEST(TransportTcpServerSessionTest, OnBytesExceptionKeepsSessionReadable) {
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  size_t bp_threshold = 1024;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);
  StopSessionOnExit cleanup{session, ioc};

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

  EXPECT_FALSE(closed.load());
  EXPECT_TRUE(session->alive());
  session->stop();
  ioc.restart();
  ioc.run_for(10ms);
}

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> session_result;
thread_local int session_observations = 0;
void observe_session_result(const wirestead::wrapper::SendResult& result) {
  session_result = result;
  ++session_observations;
}
class TcpServerSessionResultTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpServerSessionResultTest, PreservesAdmissionReasonAndRejectedMoveStorage) {
  using Rejection = wirestead::wrapper::SendRejection;
  boost::asio::io_context io;
  auto socket = std::make_unique<FakeTcpSocket>(io);
  const auto strategy = GetParam() >= 12 ? base::constants::BackpressureStrategy::BestEffort
                                         : base::constants::BackpressureStrategy::Reliable;
  auto session =
      std::make_shared<TcpServerSession>(io, std::move(socket), 1024, 0, strategy, (GetParam() / 6) % 2 == 0);
  detail::g_tcp_session_write_result_hook.store(observe_session_result);
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    detail::g_tcp_session_write_result_hook.store(nullptr);
    session->stop();
    io.restart();
    io.run_for(50ms);
  }};
  auto write = [&](std::vector<uint8_t>& data) {
    session_result.reset();
    session_observations = 0;
    bool accepted;
    switch (GetParam() % 6) {
      case 0:
        accepted = session->async_write_copy({data.data(), data.size()});
        break;
      case 1:
        accepted = session->async_write_move(std::move(data));
        break;
      case 2:
        accepted = session->async_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
      case 3:
        accepted = session->async_try_write_copy({data.data(), data.size()});
        break;
      case 4:
        accepted = session->async_try_write_move(std::move(data));
        break;
      default:
        accepted = session->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
    }
    EXPECT_EQ(session_observations, 1);
    EXPECT_TRUE(session_result.has_value());
    if (session_result) {
      EXPECT_EQ(session_result->accepted(), accepted);
    }
    return accepted;
  };
  auto reject = [&](Rejection expected) {
    ASSERT_TRUE(session_result.has_value());
    ASSERT_FALSE(session_result->accepted());
    EXPECT_EQ(session_result->reason(), expected);
  };
  std::vector<uint8_t> data{1, 2, 3};
  EXPECT_FALSE(write(data));
  reject(Rejection::NotReady);
  EXPECT_EQ(data, (std::vector<uint8_t>{1, 2, 3}));
  session->start();
  std::vector<uint8_t> empty;
  EXPECT_FALSE(write(empty));
  reject(Rejection::InvalidArgument);
  EXPECT_TRUE(write(data));
  const auto accepted = session->stats().messages_accepted;
  const auto fill = GetParam() % 6 < 3 ? session->write_queue_limit() - 3 : 1024 - 3;
  if (GetParam() % 6 < 3)
    ASSERT_TRUE(session->async_write_move(std::vector<uint8_t>(fill, 'f')));
  else
    ASSERT_TRUE(session->async_try_write_move(std::vector<uint8_t>(fill, 'f')));
  data = {1, 2, 3};
  EXPECT_FALSE(write(data));
  reject(Rejection::WouldBlock);
  EXPECT_EQ(data, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(session->stats().messages_accepted, accepted + 1);
  session->stop();
  EXPECT_FALSE(write(data));
  reject(Rejection::NotReady);
}
INSTANTIATE_TEST_SUITE_P(FormsStrategiesAndPools, TcpServerSessionResultTest, ::testing::Range(0, 24));

TEST(TcpServerSessionResultContract, RejectsOversizeAndNullPayloads) {
  boost::asio::io_context io;
  auto socket = std::make_unique<FakeTcpSocket>(io);
  auto session = std::make_shared<TcpServerSession>(io, std::move(socket), 1024);
  session->start();
  detail::g_tcp_session_write_result_hook.store(observe_session_result);
  std::vector<uint8_t> data(base::constants::MAX_BUFFER_SIZE + 1, 1);
  EXPECT_FALSE(session->async_write_copy({data.data(), data.size()}));
  ASSERT_TRUE(session_result.has_value());
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  EXPECT_FALSE(session->async_write_move(std::move(data)));
  EXPECT_EQ(data.size(), base::constants::MAX_BUFFER_SIZE + 1);
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  EXPECT_FALSE(session->async_try_write_copy({data.data(), data.size()}));
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  EXPECT_FALSE(session->async_try_write_move(std::move(data)));
  EXPECT_EQ(data.size(), base::constants::MAX_BUFFER_SIZE + 1);
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  auto shared = std::make_shared<const std::vector<uint8_t>>(std::move(data));
  EXPECT_FALSE(session->async_write_shared(shared));
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  EXPECT_FALSE(session->async_try_write_shared(shared));
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::TooLarge);
  EXPECT_FALSE(session->async_write_shared(nullptr));
  EXPECT_EQ(session_result->reason(), wrapper::SendRejection::InvalidArgument);
  detail::g_tcp_session_write_result_hook.store(nullptr);
  session->stop();
  io.run_for(50ms);
}
}  // namespace

namespace {
struct SessionAdmissionPark {
  std::promise<void> entered;
  std::shared_future<void> release;
  std::atomic<bool> once{true};
};
std::atomic<SessionAdmissionPark*> session_park{nullptr};
void park_session_admission() {
  if (auto* park = session_park.load(); park && park->once.exchange(false)) {
    park->entered.set_value();
    park->release.wait();
  }
}
class TcpServerSessionAdmissionRaceTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpServerSessionAdmissionRaceTest, StopOrdersAfterAnAdmittedSubmission) {
  boost::asio::io_context io;
  auto socket = std::make_unique<FakeTcpSocket>(io);
  auto session = std::make_shared<TcpServerSession>(io, std::move(socket), 1024);
  session->start();
  std::promise<void> release;
  SessionAdmissionPark park;
  park.release = release.get_future().share();
  session_park.store(&park);
  detail::g_tcp_session_write_admission_hook.store(park_session_admission);
  std::future<bool> writer;
  std::future<void> stopper;
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    try {
      release.set_value();
    } catch (...) {
    }
    if (writer.valid()) writer.wait();
    if (stopper.valid()) stopper.wait();
    detail::g_tcp_session_write_admission_hook.store(nullptr);
    session_park.store(nullptr);
    session->stop();
    io.restart();
    io.run_for(50ms);
  }};
  writer = std::async(std::launch::async, [&] {
    std::vector<uint8_t> data{1, 2, 3};
    switch (GetParam()) {
      case 0:
        return session->async_write_copy({data.data(), data.size()});
      case 1:
        return session->async_write_move(std::move(data));
      case 2:
        return session->async_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
      case 3:
        return session->async_try_write_copy({data.data(), data.size()});
      case 4:
        return session->async_try_write_move(std::move(data));
      default:
        return session->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
    }
  });
  ASSERT_EQ(park.entered.get_future().wait_for(3s), std::future_status::ready);
  std::promise<void> stop_entered;
  stopper = std::async(std::launch::async, [&] {
    stop_entered.set_value();
    session->stop();
  });
  ASSERT_EQ(stop_entered.get_future().wait_for(3s), std::future_status::ready);
  EXPECT_EQ(stopper.wait_for(20ms), std::future_status::timeout);
  release.set_value();
  EXPECT_TRUE(writer.get());
  stopper.get();
  EXPECT_EQ(session->stats().messages_accepted, 1u);
  EXPECT_FALSE(session->async_write_move(std::vector<uint8_t>{4}));
}
INSTANTIATE_TEST_SUITE_P(AllForms, TcpServerSessionAdmissionRaceTest, ::testing::Range(0, 6));
}  // namespace
