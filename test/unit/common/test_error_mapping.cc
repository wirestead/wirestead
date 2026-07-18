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

#include "wirestead/diagnostics/error_mapping.hpp"
#include "wirestead/diagnostics/error_types.hpp"

using namespace wirestead::diagnostics;
using namespace wirestead;

TEST(ErrorMappingTest, MapBoostErrorToWirestead) {
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::connection_refused), ErrorCode::ConnectionRefused);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::timed_out), ErrorCode::TimedOut);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::connection_reset), ErrorCode::ConnectionReset);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::network_unreachable), ErrorCode::NotConnected);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::fault), ErrorCode::IoError);  // Fallback
}

TEST(ErrorMappingTest, IsRetryableTcpConnectError) {
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::connection_refused));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::timed_out));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::network_unreachable));
  EXPECT_FALSE(is_retryable_tcp_connect_error(boost::asio::error::operation_aborted));
  EXPECT_FALSE(is_retryable_tcp_connect_error(boost::system::error_code()));  // Success is not retryable error
}

TEST(ErrorMappingTest, ToErrorContext) {
  ErrorInfo info(ErrorLevel::ERROR, ErrorCategory::CONNECTION, "test", "op", "msg",
                 boost::asio::error::connection_refused);
  auto ctx = to_error_context(info);
  EXPECT_EQ(ctx.code(), ErrorCode::ConnectionRefused);
  EXPECT_EQ(ctx.message(), "msg");
}

TEST(ErrorMappingTest, ToErrorContextNoBoostError) {
  ErrorInfo info(ErrorLevel::ERROR, ErrorCategory::CONFIGURATION, "test", "op", "config invalid");
  auto ctx = to_error_context(info);
  EXPECT_EQ(ctx.code(), ErrorCode::InvalidConfiguration);
  EXPECT_EQ(ctx.message(), "config invalid");
}
