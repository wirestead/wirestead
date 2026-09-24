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
#include <functional>
#include <future>
#include <optional>
#include <vector>

#include "../../mocks/mock_uds_socket.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/uds/boost_uds_socket.hpp"
#include "wirestead/transport/uds/uds_server_session.hpp"

using namespace wirestead;
using namespace transport;
using namespace testing;
using namespace std::chrono_literals;

namespace wirestead {
namespace test {

class UdsServerSessionLifecycleTest : public Test {
 protected:
  boost::asio::io_context ioc;
};

TEST_F(UdsServerSessionLifecycleTest, RedundantStop) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  session->start();
  session->stop();
  session->stop();  // Should return early

  ioc.run();
}

TEST_F(UdsServerSessionLifecycleTest, BackpressureLimitEnforced) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 100, 0);

  std::vector<uint8_t> large_data(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD + 1, 'A');
  EXPECT_FALSE(session->async_write_copy(memory::ConstByteSpan(large_data.data(), large_data.size())));

  // We can't easily check internal state but we can verify it doesn't crash
  // and exercises the bp_limit check.
  session->stop();
  ioc.run();
}

TEST_F(UdsServerSessionLifecycleTest, IdleTimeoutExpiration) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  EXPECT_CALL(*mock_socket, async_read_some(_, _)).Times(AtLeast(1));
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  std::atomic<bool> closed{false};
  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 10);  // 10ms timeout
  session->on_close([&]() { closed = true; });
  session->start();

  // Manually run io_context in a loop to allow timer and handlers to execute
  auto start = std::chrono::steady_clock::now();
  while (!closed && std::chrono::steady_clock::now() - start < 1s) {
    ioc.run_one_for(10ms);
  }

  EXPECT_TRUE(closed.load());
}

TEST_F(UdsServerSessionLifecycleTest, ReadErrorClosesSession) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillOnce(Invoke([](const boost::asio::mutable_buffer&,
                          std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        handler(make_error_code(boost::asio::error::connection_reset), 0);
      }));
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  std::atomic<bool> closed{false};
  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  session->on_close([&]() { closed = true; });
  session->start();

  ioc.run();

  EXPECT_TRUE(closed.load());
  EXPECT_FALSE(session->alive());
}

TEST_F(UdsServerSessionLifecycleTest, WriteErrorClosesSession) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  EXPECT_CALL(*mock_socket, async_read_some(_, _)).Times(AtLeast(1));
  EXPECT_CALL(*mock_socket, async_write(_, _))
      .WillOnce(Invoke([](const boost::asio::const_buffer& buffer,
                          std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        handler(make_error_code(boost::asio::error::broken_pipe), buffer.size());
      }));
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  std::atomic<bool> closed{false};
  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  session->on_close([&]() { closed = true; });
  session->start();

  std::vector<uint8_t> payload(32, 'x');
  EXPECT_TRUE(session->async_write_move(std::move(payload)));

  ioc.run();

  EXPECT_TRUE(closed.load());
  EXPECT_FALSE(session->alive());
}

TEST_F(UdsServerSessionLifecycleTest, SharedWritePendingFlushesAfterBackpressureRelief) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  std::function<void(const boost::system::error_code&, std::size_t)> first_write_handler;
  int write_calls = 0;

  EXPECT_CALL(*mock_socket, async_read_some(_, _)).Times(AtLeast(1));
  EXPECT_CALL(*mock_socket, async_write(_, _))
      .WillRepeatedly(Invoke([&](const boost::asio::const_buffer& buffer,
                                 std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        ++write_calls;
        if (write_calls == 1) {
          first_write_handler = std::move(handler);
          return;
        }
        handler({}, buffer.size());
      }));
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  std::vector<size_t> backpressure_events;
  session->on_backpressure([&](size_t queued) { backpressure_events.push_back(queued); });
  session->start();

  auto payload = std::make_shared<const std::vector<uint8_t>>(2048, 'p');
  EXPECT_TRUE(session->async_write_shared(payload));
  ioc.run_for(10ms);
  ASSERT_TRUE(first_write_handler);
  EXPECT_TRUE(session->is_backpressure_active());

  EXPECT_TRUE(session->async_write_shared(std::make_shared<const std::vector<uint8_t>>(64, 'q')));
  ioc.run_for(10ms);
  EXPECT_EQ(write_calls, 1);

  first_write_handler({}, payload->size());
  ioc.restart();
  ioc.run_for(20ms);

  EXPECT_GE(write_calls, 2);
  EXPECT_FALSE(backpressure_events.empty());

  session->stop();
  ioc.restart();
  ioc.run();
}

// Regression test for jwsung91/wirestead#452: if a client disconnects (read
// error) while backpressure is active for its session, do_close() must
// unconditionally clear it and fire on_backpressure - otherwise a caller
// blocked in send_to_blocking() for this client would never wake up, since
// nothing else will ever call report_backpressure() again once the session
// is gone. Uses a held (never-completing) write handler to model a client
// that has stopped reading, so nothing but do_close() can ever clear
// backpressure here.
TEST_F(UdsServerSessionLifecycleTest, BackpressureClearsOnDisconnectWhileActive) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler;

  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillOnce(Invoke([&](const boost::asio::mutable_buffer&,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        read_handler = std::move(handler);
      }));
  EXPECT_CALL(*mock_socket, async_write(_, _))
      .WillOnce(Invoke(
          [](const boost::asio::const_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {
            // Never invoke: the write is permanently stuck in-flight.
          }));
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  std::vector<size_t> events;
  session->on_backpressure([&](size_t queued) { events.push_back(queued); });
  session->start();

  std::vector<uint8_t> payload(2048, 'p');
  ASSERT_TRUE(session->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  ioc.run_for(10ms);

  ASSERT_GE(events.size(), 1u);
  EXPECT_GE(events.back(), 1024u);
  EXPECT_TRUE(session->is_backpressure_active());
  ASSERT_TRUE(read_handler);

  // Simulate an abrupt disconnect (client gone) while the write is still
  // stuck and backpressure is still active.
  read_handler(make_error_code(boost::asio::error::eof), 0);
  ioc.restart();
  ioc.run_for(20ms);

  EXPECT_EQ(events.back(), 0u);
  EXPECT_FALSE(session->alive());
}

TEST_F(UdsServerSessionLifecycleTest, SharedWriteRejectsNullAndClosedSession) {
  auto mock_socket = std::make_unique<wirestead::test::mocks::MockUdsSocket>();
  EXPECT_CALL(*mock_socket, close(_)).Times(1);

  auto session = std::make_shared<UdsServerSession>(ioc, std::move(mock_socket), 1024, 0);
  EXPECT_FALSE(session->async_write_shared(nullptr));

  session->start();
  session->stop();
  ioc.run();

  EXPECT_FALSE(session->async_write_shared(std::make_shared<const std::vector<uint8_t>>(8, 'z')));
}

}  // namespace test
}  // namespace wirestead

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> session_result;
thread_local int session_observations = 0;
void observe_session_result(const wirestead::wrapper::SendResult& result) {
  session_result = result;
  ++session_observations;
}
class UdsServerSessionResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsServerSessionResultTest, PreservesAdmissionReasonAndRejectedMoveStorage) {
  using Rejection = wirestead::wrapper::SendRejection;
  boost::asio::io_context io;
  auto socket = std::make_unique<::testing::NiceMock<wirestead::test::mocks::MockUdsSocket>>();
  const auto strategy = GetParam() >= 12 ? base::constants::BackpressureStrategy::BestEffort
                                         : base::constants::BackpressureStrategy::Reliable;
  auto session =
      std::make_shared<UdsServerSession>(io, std::move(socket), 1024, 0, strategy, (GetParam() / 6) % 2 == 0);
  detail::g_uds_session_write_result_hook.store(observe_session_result);
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    detail::g_uds_session_write_result_hook.store(nullptr);
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
INSTANTIATE_TEST_SUITE_P(FormsStrategiesAndPools, UdsServerSessionResultTest, ::testing::Range(0, 24));

TEST(UdsServerSessionResultContract, RejectsOversizeAndNullPayloads) {
  boost::asio::io_context io;
  auto socket = std::make_unique<::testing::NiceMock<wirestead::test::mocks::MockUdsSocket>>();
  auto session = std::make_shared<UdsServerSession>(io, std::move(socket), 1024);
  session->start();
  detail::g_uds_session_write_result_hook.store(observe_session_result);
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
  detail::g_uds_session_write_result_hook.store(nullptr);
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
class UdsServerSessionAdmissionRaceTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsServerSessionAdmissionRaceTest, StopOrdersAfterAnAdmittedSubmission) {
  boost::asio::io_context io;
  auto socket = std::make_unique<::testing::NiceMock<wirestead::test::mocks::MockUdsSocket>>();
  auto session = std::make_shared<UdsServerSession>(io, std::move(socket), 1024);
  session->start();
  std::promise<void> release;
  SessionAdmissionPark park;
  park.release = release.get_future().share();
  session_park.store(&park);
  detail::g_uds_session_write_admission_hook.store(park_session_admission);
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
    detail::g_uds_session_write_admission_hook.store(nullptr);
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
INSTANTIATE_TEST_SUITE_P(AllForms, UdsServerSessionAdmissionRaceTest, ::testing::Range(0, 6));
}  // namespace
