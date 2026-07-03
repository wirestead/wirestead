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

#include "unilink/transport/base/error_info_holder.hpp"

using namespace unilink::transport::base;
using namespace unilink::diagnostics;

TEST(ErrorInfoHolderTest, StartsWithNoError) {
  ErrorInfoHolder holder("test_component");
  EXPECT_FALSE(holder.last_error_info().has_value());
}

TEST(ErrorInfoHolderTest, RecordErrorPopulatesAllFields) {
  ErrorInfoHolder holder("test_component");
  boost::system::error_code ec = make_error_code(boost::system::errc::connection_refused);

  holder.record_error(ErrorLevel::ERROR, ErrorCategory::CONNECTION, "connect", ec, "Connection refused",
                      /*retryable=*/true, /*retry_count=*/3);

  auto info = holder.last_error_info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->level, ErrorLevel::ERROR);
  EXPECT_EQ(info->category, ErrorCategory::CONNECTION);
  EXPECT_EQ(info->component, "test_component");
  EXPECT_EQ(info->operation, "connect");
  EXPECT_EQ(info->message, "Connection refused");
  EXPECT_EQ(info->boost_error, ec);
  EXPECT_TRUE(info->retryable);
  EXPECT_EQ(info->retry_count, 3u);
}

TEST(ErrorInfoHolderTest, SubsequentRecordOverwritesPrevious) {
  ErrorInfoHolder holder("test_component");
  holder.record_error(ErrorLevel::WARNING, ErrorCategory::CONFIGURATION, "first", {}, "first error", false, 0);
  holder.record_error(ErrorLevel::CRITICAL, ErrorCategory::SYSTEM, "second", {}, "second error", true, 5);

  auto info = holder.last_error_info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->operation, "second");
  EXPECT_EQ(info->message, "second error");
  EXPECT_EQ(info->retry_count, 5u);
}
