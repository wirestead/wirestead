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

#include <limits>
#include <optional>

#include "wirestead/wrapper/send_validation.hpp"

using wirestead::wrapper::SendRejection;
using wirestead::wrapper::detail::payload_needs_capacity;
using wirestead::wrapper::detail::validate_payload_size;
constexpr auto kMaxPayload = wirestead::base::constants::MAX_BUFFER_SIZE;

static_assert(validate_payload_size(1).accepted());
static_assert(validate_payload_size(0).reason() == SendRejection::InvalidArgument);
static_assert(validate_payload_size(kMaxPayload + 1).reason() == SendRejection::TooLarge);
static_assert(noexcept(validate_payload_size(1)));
static_assert(noexcept(payload_needs_capacity(1)));

TEST(SendPayloadValidationTest, EmptyInputPrecedesEvenAZeroQueueLimit) {
  for (auto limit : {std::optional<size_t>{}, std::optional<size_t>{0}, std::optional<size_t>{1024}}) {
    const auto result = validate_payload_size(0, limit);
    ASSERT_FALSE(result.accepted());
    EXPECT_EQ(result.reason(), SendRejection::InvalidArgument);
    EXPECT_FALSE(payload_needs_capacity(0, limit));
  }
}
TEST(SendPayloadValidationTest, MessageMaximumIsInclusiveWithoutQueueMetadata) {
  EXPECT_TRUE(validate_payload_size(1).accepted());
  EXPECT_TRUE(validate_payload_size(kMaxPayload).accepted());
  const auto result = validate_payload_size(kMaxPayload + 1);
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
}
TEST(SendPayloadValidationTest, QueueMaximumIsInclusiveAndDistinctFromCurrentPressure) {
  EXPECT_TRUE(validate_payload_size(1024, 1024).accepted());
  const auto result = validate_payload_size(1025, 1024);
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
  EXPECT_FALSE(payload_needs_capacity(1025, 1024));
}
TEST(SendPayloadValidationTest, KnownZeroLimitRejectsNonemptyInput) {
  const auto result = validate_payload_size(1, 0);
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
}
TEST(SendPayloadValidationTest, LargerQueueDoesNotOverrideMessageMaximum) {
  const auto result = validate_payload_size(kMaxPayload + 1, std::numeric_limits<size_t>::max());
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
  EXPECT_TRUE(validate_payload_size(kMaxPayload, std::numeric_limits<size_t>::max()).accepted());
}
TEST(SendPayloadValidationTest, SizeTypeMaximumRejectsWithoutOverflow) {
  const auto result = validate_payload_size(std::numeric_limits<size_t>::max());
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
}
TEST(SendPayloadValidationTest, DelimiterCountsTowardCompletePayload) {
  EXPECT_TRUE(validate_payload_size(1).accepted());  // Empty line plus newline.
  EXPECT_TRUE(validate_payload_size(1023 + 1, 1024).accepted());
  const auto result = validate_payload_size(1024 + 1, 1024);
  ASSERT_FALSE(result.accepted());
  EXPECT_EQ(result.reason(), SendRejection::TooLarge);
}
