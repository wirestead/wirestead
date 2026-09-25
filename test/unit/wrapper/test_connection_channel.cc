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

#include <boost/asio/io_context.hpp>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <tuple>

#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead;
using wrapper::SendRejection;
using wrapper::SendResult;
using namespace std::chrono_literals;

namespace {
class CustomChannel : public interface::ConnectionChannel {
 public:
  struct Record {
    std::optional<SendRejection> ended;
    void end(SendRejection reason) {
      if (!ended) ended = reason;
    }
  };
  struct State {
    std::mutex mutex;
    std::shared_ptr<Record> current;
    bool pressure = false;
    std::optional<SendRejection> refusal;
    int captures = 0, polls = 0, writes = 0, accepted = 0;
  };
  class Pin : public interface::WriteConnection {
   public:
    Pin(std::shared_ptr<State> state, std::shared_ptr<Record> record)
        : state_(std::move(state)), record_(std::move(record)) {}
    std::optional<SendResult> poll_capacity() override {
      std::lock_guard<std::mutex> lock(state_->mutex);
      ++state_->polls;
      if (record_->ended) return SendResult::reject(*record_->ended);
      if (state_->pressure) return std::nullopt;
      return SendResult::accept();
    }
    SendResult write_copy(memory::ConstByteSpan) override { return admit(); }
    SendResult write_move(std::vector<uint8_t>&& data) override {
      auto result = admit();
      if (result) data.clear();
      return result;
    }
    SendResult write_shared(std::shared_ptr<const std::vector<uint8_t>>) override { return admit(); }

   private:
    SendResult admit() {
      std::lock_guard<std::mutex> lock(state_->mutex);
      ++state_->writes;
      if (record_->ended || state_->current != record_) return SendResult::reject(SendRejection::NotReady);
      if (state_->refusal) return SendResult::reject(*state_->refusal);
      ++state_->accepted;
      return SendResult::accept();
    }
    std::shared_ptr<State> state_;
    std::shared_ptr<Record> record_;
  };

  std::shared_ptr<State> state = std::make_shared<State>();
  void connect() {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->current) state->current->end(SendRejection::NotReady);
    state->current = std::make_shared<Record>();
  }
  void lose() {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->current) state->current->end(SendRejection::NotReady);
  }
  void pressure(bool value) {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->pressure = value;
  }
  void start() override { connect(); }
  void stop() override { cancel_write_waits(); }
  void cancel_write_waits() override {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->current) state->current->end(SendRejection::CancelledWhileWaiting);
  }
  bool is_connected() const override {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->current && !state->current->ended;
  }
  bool is_backpressure_active() const override {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->pressure;
  }
  std::optional<size_t> write_queue_limit() const override { return 32; }
  boost::asio::any_io_executor get_executor() override { return io_.get_executor(); }
  CaptureResult capture_write_connection() override {
    std::lock_guard<std::mutex> lock(state->mutex);
    ++state->captures;
    if (!state->current || state->current->ended) return SendRejection::NotReady;
    return std::make_shared<Pin>(state, state->current);
  }
  SendResult async_write_copy_result(memory::ConstByteSpan data) override {
    auto pin = capture_write_connection();
    if (auto reason = std::get_if<SendRejection>(&pin)) return SendResult::reject(*reason);
    return std::get<Connection>(pin)->write_copy(data);
  }
  SendResult async_write_move_result(std::vector<uint8_t>&& data) override {
    auto pin = capture_write_connection();
    if (auto reason = std::get_if<SendRejection>(&pin)) return SendResult::reject(*reason);
    return std::get<Connection>(pin)->write_move(std::move(data));
  }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) override {
    auto pin = capture_write_connection();
    if (auto reason = std::get_if<SendRejection>(&pin)) return SendResult::reject(*reason);
    return std::get<Connection>(pin)->write_shared(std::move(data));
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan data) override { return async_write_copy_result(data); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&& data) override {
    return async_write_move_result(std::move(data));
  }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return async_write_shared_result(std::move(data));
  }
  void on_bytes(OnBytes) override {}
  void on_state(OnState) override {}
  void on_backpressure(OnBackpressure) override {}

 private:
  boost::asio::io_context io_;
};

struct Park {
  std::mutex mutex;
  std::condition_variable cv;
  bool entered = false, released = false;
  void hold() {
    std::unique_lock<std::mutex> lock(mutex);
    entered = true;
    cv.notify_all();
    cv.wait(lock, [&] { return released; });
  }
  bool wait() {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, 5s, [&] { return entered; });
  }
  void release() {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
    cv.notify_all();
  }
};
Park* parked = nullptr;
std::optional<SendResult> observed;
void park_wait() { parked->hold(); }
void park_release(const SendResult&) { parked->hold(); }
void observe(const SendResult& result) { observed = result; }

class ConnectionChannelTest : public ::testing::TestWithParam<std::tuple<int, int>> {
 protected:
  std::shared_ptr<CustomChannel> channel;
  std::unique_ptr<wrapper::ChannelInterface> client;
  std::vector<uint8_t> moved{1, 2, 3};
  std::shared_ptr<const std::vector<uint8_t>> shared = std::make_shared<const std::vector<uint8_t>>(3, 7);
  void SetUp() override {
    channel = std::make_shared<CustomChannel>();
    switch (std::get<0>(GetParam())) {
      case 0:
        client = std::make_unique<wrapper::TcpClient>(channel);
        break;
      case 1:
        client = std::make_unique<wrapper::UdsClient>(channel);
        break;
      case 2:
        client = std::make_unique<wrapper::UdpClient>(channel);
        break;
      case 3:
        client = std::make_unique<wrapper::Serial>(channel);
        break;
    }
    restart();
    observed.reset();
    wrapper::detail::g_tcp_send_result_hook = observe;
    wrapper::detail::g_uds_send_result_hook = observe;
    wrapper::detail::g_udp_send_result_hook = observe;
    wrapper::detail::g_serial_send_result_hook = observe;
  }
  void TearDown() override {
    client->stop();
    wrapper::detail::g_tcp_send_result_hook = nullptr;
    wrapper::detail::g_uds_send_result_hook = nullptr;
    wrapper::detail::g_udp_send_result_hook = nullptr;
    wrapper::detail::g_serial_send_result_hook = nullptr;
    wait_hook(nullptr);
    release_hook(nullptr);
    parked = nullptr;
  }
  void restart() {
    channel->connect();
    ASSERT_TRUE(client->start().get());
  }
  void wait_hook(void (*hook)()) {
    wrapper::detail::g_tcp_capacity_wait_hook = hook;
    wrapper::detail::g_uds_capacity_wait_hook = hook;
    wrapper::detail::g_udp_capacity_wait_hook = hook;
    wrapper::detail::g_serial_capacity_wait_hook = hook;
  }
  void release_hook(void (*hook)(const SendResult&)) {
    wrapper::detail::g_tcp_capacity_wait_result_hook = hook;
    wrapper::detail::g_uds_capacity_wait_result_hook = hook;
    wrapper::detail::g_udp_capacity_wait_result_hook = hook;
    wrapper::detail::g_serial_capacity_wait_result_hook = hook;
  }
  bool send(bool attempt = false) {
    switch (std::get<1>(GetParam())) {
      case 0:
        return attempt ? client->try_send("abc") : client->send("abc");
      case 1:
        return attempt ? client->try_send_move(std::move(moved)) : client->send_move(std::move(moved));
      default:
        return attempt ? client->try_send_shared(shared) : client->send_shared(shared);
    }
  }
  void expect_reason(SendRejection reason) {
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(observed->accepted());
    EXPECT_EQ(observed->reason(), reason);
    EXPECT_EQ(moved, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(shared.use_count(), 1);
  }
  void terminal_wait(bool stop_first) {
    channel->pressure(true);
    Park park;
    parked = &park;
    wait_hook(park_wait);
    auto future = std::async(std::launch::async, [&] { return send(); });
    const bool entered = park.wait();
    if (stop_first) client->stop();
    channel->lose();
    if (!stop_first) client->stop();
    restart();
    channel->pressure(false);
    park.release();  // Always release even if the wait hook was not reached.
    ASSERT_TRUE(entered);
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(future.get());
    expect_reason(stop_first ? SendRejection::CancelledWhileWaiting : SendRejection::NotReady);
    EXPECT_EQ(channel->state->writes, 0);
    wait_hook(nullptr);
    EXPECT_TRUE(send());
    EXPECT_EQ(channel->state->accepted, 1);
  }
};

TEST_P(ConnectionChannelTest, LossThenStopAndRestartPreservesFirstCause) { terminal_wait(false); }
TEST_P(ConnectionChannelTest, StopThenLossAndRestartPreservesFirstCause) { terminal_wait(true); }
TEST_P(ConnectionChannelTest, ReplacementAfterCapacityReleaseCannotReceiveOldSend) {
  channel->pressure(true);
  Park enter, release;
  parked = &enter;
  wait_hook(park_wait);
  auto future = std::async(std::launch::async, [&] { return send(); });
  const bool entered = enter.wait();
  channel->pressure(false);
  // Switch hook context while sender is parked; the wait hook already has its
  // Park object. The second hook parks after selecting capacity acceptance.
  parked = &release;
  release_hook(park_release);
  enter.release();
  const bool released = release.wait();
  channel->connect();
  release.release();
  ASSERT_TRUE(entered);
  ASSERT_TRUE(released);
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
  EXPECT_FALSE(future.get());
  expect_reason(SendRejection::NotReady);
  EXPECT_EQ(channel->state->accepted, 0);
  EXPECT_EQ(channel->state->captures, 1);
}
TEST_P(ConnectionChannelTest, RetriesOnlyWouldBlockWithTheSamePin) {
  channel->state->refusal = SendRejection::WouldBlock;
  EXPECT_FALSE(send());
  expect_reason(SendRejection::WouldBlock);
  EXPECT_EQ(channel->state->writes, 5);
  EXPECT_EQ(channel->state->captures, 1);
}
TEST_P(ConnectionChannelTest, TerminalAdmissionIsNotRetried) {
  channel->state->refusal = SendRejection::QueueFull;
  EXPECT_FALSE(send());
  expect_reason(SendRejection::QueueFull);
  EXPECT_EQ(channel->state->writes, 1);
}
TEST_P(ConnectionChannelTest, CallbackDoesNotWaitOrAdmitAgainstPressure) {
  channel->pressure(true);
  wrapper::detail::CallbackGuard guard;
  EXPECT_FALSE(send());
  expect_reason(SendRejection::WouldBlock);
  EXPECT_EQ(channel->state->writes, 0);
}
TEST_P(ConnectionChannelTest, CallbackDoesNotRetryAdmission) {
  channel->state->refusal = SendRejection::WouldBlock;
  wrapper::detail::CallbackGuard guard;
  EXPECT_FALSE(send());
  expect_reason(SendRejection::WouldBlock);
  EXPECT_EQ(channel->state->writes, 1);
}
TEST_P(ConnectionChannelTest, ExplicitTryPreservesActualRejectionWithoutPolling) {
  channel->pressure(true);
  channel->state->refusal = SendRejection::WouldBlock;
  EXPECT_FALSE(send(true));
  expect_reason(SendRejection::WouldBlock);
  EXPECT_EQ(channel->state->polls, 0);
  EXPECT_EQ(channel->state->writes, 1);
}
TEST_P(ConnectionChannelTest, ValidationPrecedesCaptureAndWait) {
  channel->pressure(true);
  EXPECT_FALSE(client->send_blocking(std::string(33, 'x')));
  expect_reason(SendRejection::TooLarge);
  EXPECT_EQ(channel->state->captures, 0);
  EXPECT_EQ(channel->state->polls, 0);
}
TEST_P(ConnectionChannelTest, StoppedEntryDoesNotCaptureOrPoll) {
  client->stop();
  EXPECT_FALSE(send());
  expect_reason(SendRejection::NotStarted);
  EXPECT_EQ(channel->state->captures, 0);
  EXPECT_EQ(channel->state->polls, 0);
}
TEST_P(ConnectionChannelTest, CaptureRefusesDisconnectedChannel) {
  channel->lose();
  EXPECT_FALSE(send());
  expect_reason(SendRejection::NotReady);
  EXPECT_EQ(channel->state->captures, 1);
  EXPECT_EQ(channel->state->polls, 0);
  EXPECT_EQ(channel->state->writes, 0);
}
TEST_P(ConnectionChannelTest, BestEffortMapsCapacityRefusalAndNeverPolls) {
  const auto strategy = base::constants::BackpressureStrategy::BestEffort;
  switch (std::get<0>(GetParam())) {
    case 0:
      dynamic_cast<wrapper::TcpClient&>(*client).backpressure_strategy(strategy);
      break;
    case 1:
      dynamic_cast<wrapper::UdsClient&>(*client).backpressure_strategy(strategy);
      break;
    case 2:
      dynamic_cast<wrapper::UdpClient&>(*client).backpressure_strategy(strategy);
      break;
    case 3:
      dynamic_cast<wrapper::Serial&>(*client).backpressure_strategy(strategy);
      break;
  }
  channel->pressure(true);
  channel->state->refusal = SendRejection::WouldBlock;
  EXPECT_FALSE(send());
  expect_reason(SendRejection::QueueFull);
  EXPECT_EQ(channel->state->polls, 0);
  EXPECT_EQ(channel->state->writes, 1);
}
TEST_P(ConnectionChannelTest, InvalidSharedPayloadDoesNotReachChannel) {
  channel->pressure(true);
  EXPECT_FALSE(client->send_shared(nullptr));
  expect_reason(SendRejection::InvalidArgument);
  EXPECT_FALSE(client->try_send_shared(std::make_shared<const std::vector<uint8_t>>()));
  expect_reason(SendRejection::InvalidArgument);
  EXPECT_EQ(channel->state->captures, 0);
  EXPECT_EQ(channel->state->writes, 0);
}
TEST_P(ConnectionChannelTest, LineDelimiterIsIncludedInHardLimitValidation) {
  channel->pressure(true);
  EXPECT_FALSE(client->send_line_blocking(std::string(32, 'x')));
  expect_reason(SendRejection::TooLarge);
  EXPECT_FALSE(client->try_send_line(std::string(32, 'x')));
  expect_reason(SendRejection::TooLarge);
  EXPECT_EQ(channel->state->captures, 0);
}
INSTANTIATE_TEST_SUITE_P(AllClientsAndPayloads, ConnectionChannelTest,
                         ::testing::Combine(::testing::Range(0, 4), ::testing::Range(0, 3)));
}  // namespace
