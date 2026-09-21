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

#include "wirestead/wrapper/send_result.hpp"

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
