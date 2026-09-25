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
#include <type_traits>
#include <utility>

#include "wirestead/interface/result_channel.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/wrapper/send_result.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

using wirestead::wrapper::SendRejection;
using wirestead::wrapper::SendResult;

static_assert(SendResult::accept().accepted());
static_assert(!SendResult::reject(SendRejection::QueueFull).accepted());
static_assert(SendResult::reject(SendRejection::QueueFull).reason() == SendRejection::QueueFull);
static_assert(!std::is_default_constructible_v<SendResult>);
static_assert(!std::is_convertible_v<SendResult, bool>);
static_assert(!std::is_constructible_v<SendResult, bool>);
static_assert(std::is_trivially_copyable_v<SendResult>);
static_assert(noexcept(SendResult::accept()));
static_assert(noexcept(SendResult::reject(SendRejection::NotReady).reason()));

TEST(SendResultTest, AcceptanceSupportsContextualBooleanUse) {
  const auto result = SendResult::accept();
  EXPECT_TRUE(result.accepted());
  EXPECT_TRUE(static_cast<bool>(result));
  EXPECT_FALSE(!result);
  bool entered = false;
  if (result) entered = true;
  EXPECT_TRUE(entered);
}
TEST(SendResultTest, CopiesPreserveOutcome) {
  auto result = SendResult::reject(SendRejection::CancelledWhileWaiting);
  const auto cancelled = result;
  result = SendResult::accept();
  EXPECT_TRUE(result.accepted());
  EXPECT_FALSE(cancelled.accepted());
  EXPECT_EQ(cancelled.reason(), SendRejection::CancelledWhileWaiting);
}
class SendRejectionTest : public ::testing::TestWithParam<SendRejection> {};
TEST_P(SendRejectionTest, RetainsReasonAndRejectsBooleanUse) {
  const auto result = SendResult::reject(GetParam());
  EXPECT_FALSE(result.accepted());
  EXPECT_FALSE(static_cast<bool>(result));
  EXPECT_TRUE(!result);
  EXPECT_EQ(result.reason(), GetParam());
  bool entered = false;
  if (result) entered = true;
  EXPECT_FALSE(entered);
}
INSTANTIATE_TEST_SUITE_P(AllReasons, SendRejectionTest,
                         ::testing::Values(SendRejection::NotStarted, SendRejection::Stopping, SendRejection::NotReady,
                                           SendRejection::WouldBlock, SendRejection::QueueFull, SendRejection::TooLarge,
                                           SendRejection::InvalidArgument, SendRejection::CancelledWhileWaiting));

namespace {
template <typename Server>
constexpr bool targeted_send_contract() {
  using Id = wirestead::ClientId;
  using View = std::string_view;
  using Fanout = wirestead::wrapper::FanoutResult;
  static_assert(std::is_same_v<decltype(std::declval<Server&>().broadcast(View{})), Fanout>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().try_broadcast(View{})), Fanout>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().broadcast_line(View{})), Fanout>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().try_broadcast_line(View{})), Fanout>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().send_to(Id{}, View{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().try_send_to(Id{}, View{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().send_to_blocking(Id{}, View{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().send_to_line(Id{}, View{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<Server&>().try_send_to_line(Id{}, View{})), SendResult>);
  return true;
}
static_assert(targeted_send_contract<wirestead::wrapper::ServerInterface>());
static_assert(targeted_send_contract<wirestead::wrapper::TcpServer>());
static_assert(targeted_send_contract<wirestead::wrapper::UdsServer>());
static_assert(targeted_send_contract<wirestead::wrapper::UdpServer>());
}  // namespace

TEST(SendResultTest, ServerInterfaceExposesValidationAndLifecycleReasons) {
  wirestead::wrapper::TcpServer tcp(0);
  wirestead::wrapper::UdsServer uds("unused-send-result-test");
  wirestead::wrapper::UdpServer udp(0);
  wirestead::wrapper::ServerInterface* servers[] = {&tcp, &uds, &udp};
  for (auto* server : servers) {
    const auto invalid = server->send_to(1, "");
    ASSERT_FALSE(invalid.accepted());
    EXPECT_EQ(invalid.reason(), SendRejection::InvalidArgument);
    for (auto send :
         {&wirestead::wrapper::ServerInterface::send_to, &wirestead::wrapper::ServerInterface::try_send_to,
          &wirestead::wrapper::ServerInterface::send_to_blocking, &wirestead::wrapper::ServerInterface::send_to_line,
          &wirestead::wrapper::ServerInterface::try_send_to_line}) {
      const auto stopped = (server->*send)(1, "payload");
      ASSERT_FALSE(stopped.accepted());
      EXPECT_EQ(stopped.reason(), SendRejection::NotStarted);
    }
    const auto empty_line = server->send_to_line(1, "");
    ASSERT_FALSE(empty_line.accepted());
    EXPECT_EQ(empty_line.reason(), SendRejection::NotStarted);
  }
}

namespace {
class CustomResultChannel : public wirestead::interface::ResultChannel {
 public:
  boost::asio::io_context io;
  SendResult outcome = SendResult::accept();
  int calls = 0;
  int form = -1;
  void start() override {}
  void stop() override {}
  bool is_connected() const override { return true; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override { return io.get_executor(); }
  void on_bytes(OnBytes) override {}
  void on_state(OnState) override {}
  void on_backpressure(OnBackpressure) override {}
  SendResult record(int value) {
    ++calls;
    form = value;
    return outcome;
  }
  SendResult async_write_copy_result(wirestead::memory::ConstByteSpan) override { return record(0); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return record(1); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override { return record(2); }
  SendResult async_try_write_copy_result(wirestead::memory::ConstByteSpan) override { return record(3); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&&) override { return record(4); }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override { return record(5); }
};
SendResult result_write(wirestead::interface::ResultChannel& channel, int form, std::vector<uint8_t>& data) {
  wirestead::memory::ConstByteSpan span(data.data(), data.size());
  auto shared = std::make_shared<const std::vector<uint8_t>>(data);
  switch (form) {
    case 0:
      return channel.async_write_copy_result(span);
    case 1:
      return channel.async_write_move_result(std::move(data));
    case 2:
      return channel.async_write_shared_result(shared);
    case 3:
      return channel.async_try_write_copy_result(span);
    case 4:
      return channel.async_try_write_move_result(std::move(data));
    default:
      return channel.async_try_write_shared_result(shared);
  }
}
bool bool_write(wirestead::interface::Channel& channel, int form, std::vector<uint8_t>& data) {
  wirestead::memory::ConstByteSpan span(data.data(), data.size());
  auto shared = std::make_shared<const std::vector<uint8_t>>(data);
  switch (form) {
    case 0:
      return channel.async_write_copy(span);
    case 1:
      return channel.async_write_move(std::move(data));
    case 2:
      return channel.async_write_shared(shared);
    case 3:
      return channel.async_try_write_copy(span);
    case 4:
      return channel.async_try_write_move(std::move(data));
    default:
      return channel.async_try_write_shared(shared);
  }
}
class ResultChannelAdapterTest : public ::testing::TestWithParam<int> {};
TEST_P(ResultChannelAdapterTest, RetainsTypedReasonAndDelegatesLegacyWriteExactlyOnce) {
  CustomResultChannel channel;
  std::shared_ptr<wirestead::interface::Channel> legacy(&channel, [](auto*) {});
  ASSERT_TRUE(std::dynamic_pointer_cast<wirestead::interface::ResultChannel>(legacy));
  for (auto outcome : {SendResult::accept(), SendResult::reject(SendRejection::NotStarted),
                       SendResult::reject(SendRejection::Stopping), SendResult::reject(SendRejection::NotReady),
                       SendResult::reject(SendRejection::WouldBlock), SendResult::reject(SendRejection::QueueFull),
                       SendResult::reject(SendRejection::TooLarge), SendResult::reject(SendRejection::InvalidArgument),
                       SendResult::reject(SendRejection::CancelledWhileWaiting)}) {
    channel.outcome = outcome;
    channel.calls = 0;
    std::vector<uint8_t> bytes{1, 2, 3};
    const auto result = result_write(channel, GetParam(), bytes);
    EXPECT_EQ(result.accepted(), outcome.accepted());
    if (!result.accepted()) {
      EXPECT_EQ(result.reason(), outcome.reason());
    }
    EXPECT_EQ(channel.calls, 1);
    EXPECT_EQ(channel.form, GetParam());
    EXPECT_EQ(bool_write(*legacy, GetParam(), bytes), outcome.accepted());
    EXPECT_EQ(channel.calls, 2);
    EXPECT_EQ(channel.form, GetParam());
  }
}
INSTANTIATE_TEST_SUITE_P(AllWriteForms, ResultChannelAdapterTest, ::testing::Range(0, 6));

class NativeResultChannelTest : public ::testing::TestWithParam<int> {};
TEST_P(NativeResultChannelTest, ReportsActualAdmissionReasonWithoutConsumingRejectedMove) {
  boost::asio::io_context io;
  std::shared_ptr<wirestead::interface::Channel> legacy;
  switch (GetParam() / 6) {
    case 0:
      legacy = wirestead::transport::TcpClient::create(wirestead::config::TcpClientConfig{}, io);
      break;
    case 1:
      legacy = wirestead::transport::UdsClient::create(wirestead::config::UdsClientConfig{}, io);
      break;
    case 2:
      legacy = wirestead::transport::UdpChannel::create(wirestead::config::UdpConfig{}, io);
      break;
    default:
      legacy = wirestead::transport::Serial::create(wirestead::config::SerialConfig{}, io);
      break;
  }
  const auto native = std::dynamic_pointer_cast<wirestead::interface::ResultChannel>(legacy);
  ASSERT_TRUE(native);
  std::vector<uint8_t> data{1, 2, 3};
  const auto before = native->stats().failed_sends;
  const auto result = result_write(*native, GetParam() % 6, data);
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::NotStarted);
  EXPECT_EQ(data, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(native->stats().failed_sends, before + 1);
  EXPECT_FALSE(bool_write(*legacy, GetParam() % 6, data));
  EXPECT_EQ(native->stats().failed_sends, before + 2);
}
INSTANTIATE_TEST_SUITE_P(FourTransportsSixForms, NativeResultChannelTest, ::testing::Range(0, 24));
}  // namespace

TEST(FanoutResultTest, EmptyIsNeitherAcceptanceNorRejection) {
  constexpr wirestead::wrapper::FanoutResult empty;
  static_assert(empty.empty());
  static_assert(!static_cast<bool>(empty));
  static_assert(!std::is_convertible_v<wirestead::wrapper::FanoutResult, bool>);
  EXPECT_EQ(empty.accepted_count(), 0u);
  EXPECT_EQ(empty.rejected_count(), 0u);
  EXPECT_EQ(empty.target_count(), 0u);
}
TEST(FanoutResultTest, MixedOutcomesRetainEveryReasonAndCopiesRemainStable) {
  wirestead::wrapper::FanoutResult result;
  result.add(SendResult::accept());
  for (auto reason : {SendRejection::NotStarted, SendRejection::Stopping, SendRejection::NotReady,
                      SendRejection::WouldBlock, SendRejection::QueueFull, SendRejection::TooLarge,
                      SendRejection::InvalidArgument, SendRejection::CancelledWhileWaiting}) {
    result.add(SendResult::reject(reason));
    result.add(SendResult::reject(reason));
    EXPECT_EQ(result.rejected_count(reason), 2u);
  }
  EXPECT_TRUE(result);
  EXPECT_EQ(result.target_count(), 17u);
  EXPECT_EQ(result.accepted_count(), 1u);
  EXPECT_EQ(result.rejected_count(), 16u);
  const auto copy = result;
  result.add(SendResult::accept());
  EXPECT_EQ(copy.accepted_count(), 1u);
  EXPECT_EQ(copy.target_count(), 17u);
}
TEST(FanoutResultTest, AllRejectedDiffersFromEmpty) {
  wirestead::wrapper::FanoutResult result;
  result.add(SendResult::reject(SendRejection::WouldBlock));
  EXPECT_FALSE(result);
  EXPECT_FALSE(result.empty());
  EXPECT_EQ(result.target_count(), 1u);
  EXPECT_EQ(result.rejected_count(), 1u);
}
TEST(FanoutResultTest, UnstartedServersHaveNoTargetsForEveryBroadcastForm) {
  wirestead::wrapper::TcpServer tcp(0);
  wirestead::wrapper::UdsServer uds("unused-fanout-test");
  wirestead::wrapper::UdpServer udp(0);
  for (wirestead::wrapper::ServerInterface* server : {static_cast<wirestead::wrapper::ServerInterface*>(&tcp),
                                                      static_cast<wirestead::wrapper::ServerInterface*>(&uds),
                                                      static_cast<wirestead::wrapper::ServerInterface*>(&udp)}) {
    for (auto send :
         {&wirestead::wrapper::ServerInterface::broadcast, &wirestead::wrapper::ServerInterface::try_broadcast,
          &wirestead::wrapper::ServerInterface::broadcast_line,
          &wirestead::wrapper::ServerInterface::try_broadcast_line}) {
      EXPECT_TRUE((server->*send)("payload").empty());
      EXPECT_TRUE((server->*send)("").empty());
    }
  }
}
