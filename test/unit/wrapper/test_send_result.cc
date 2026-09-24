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

#include <type_traits>
#include <utility>

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
