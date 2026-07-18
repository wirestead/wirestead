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

#include <boost/asio/error.hpp>

#include "wirestead/diagnostics/error_mapping.hpp"

using namespace wirestead;
using namespace wirestead::diagnostics;

namespace wirestead {
namespace test {

TEST(ErrorMappingEdgeCaseTest, ToWiresteadErrorCodeBranches) {
  // Success case
  EXPECT_EQ(to_wirestead_error_code(boost::system::error_code()), ErrorCode::Success);

  // Each specific mapping branch
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::connection_refused), ErrorCode::ConnectionRefused);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::timed_out), ErrorCode::TimedOut);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::connection_reset), ErrorCode::ConnectionReset);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::connection_aborted), ErrorCode::ConnectionAborted);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::network_unreachable), ErrorCode::NotConnected);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::host_unreachable), ErrorCode::NotConnected);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::already_connected), ErrorCode::AlreadyConnected);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::address_in_use), ErrorCode::PortInUse);
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::access_denied), ErrorCode::AccessDenied);

  // Fallback branch
  EXPECT_EQ(to_wirestead_error_code(boost::asio::error::not_found), ErrorCode::IoError);
}

TEST(ErrorMappingEdgeCaseTest, IsRetryableTcpConnectErrorBranches) {
  EXPECT_FALSE(is_retryable_tcp_connect_error(boost::system::error_code()));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::connection_refused));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::timed_out));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::connection_reset));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::network_unreachable));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::host_unreachable));
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::try_again));
  EXPECT_FALSE(is_retryable_tcp_connect_error(boost::asio::error::operation_aborted));

  // Default case (conservative true)
  EXPECT_TRUE(is_retryable_tcp_connect_error(boost::asio::error::access_denied));
}

TEST(ErrorMappingEdgeCaseTest, IsRetryableUdsConnectErrorBranches) {
  EXPECT_FALSE(is_retryable_uds_connect_error(boost::system::error_code()));
  EXPECT_TRUE(is_retryable_uds_connect_error(boost::asio::error::connection_refused));
  EXPECT_TRUE(is_retryable_uds_connect_error(make_error_code(boost::system::errc::no_such_file_or_directory)));
  EXPECT_TRUE(is_retryable_uds_connect_error(boost::asio::error::timed_out));
  EXPECT_TRUE(is_retryable_uds_connect_error(boost::asio::error::try_again));
  EXPECT_FALSE(is_retryable_uds_connect_error(boost::asio::error::operation_aborted));

  // Default case
  EXPECT_TRUE(is_retryable_uds_connect_error(boost::asio::error::access_denied));
}

TEST(ErrorMappingEdgeCaseTest, ToErrorContextBranches) {
  // Branch with boost_error
  ErrorInfo info_with_boost(ErrorLevel::ERROR, ErrorCategory::COMMUNICATION, "comp", "op", "msg",
                            boost::asio::error::connection_refused);
  auto ctx1 = to_error_context(info_with_boost);
  EXPECT_EQ(ctx1.code(), ErrorCode::ConnectionRefused);

  // Branch without boost_error, mapping categories
  ErrorInfo info_conn(ErrorLevel::ERROR, ErrorCategory::CONNECTION, "comp", "op", "msg");
  EXPECT_EQ(to_error_context(info_conn).code(), ErrorCode::NotConnected);

  ErrorInfo info_conf(ErrorLevel::ERROR, ErrorCategory::CONFIGURATION, "comp", "op", "msg");
  EXPECT_EQ(to_error_context(info_conf).code(), ErrorCode::InvalidConfiguration);

  ErrorInfo info_sys(ErrorLevel::ERROR, ErrorCategory::SYSTEM, "comp", "op", "msg");
  EXPECT_EQ(to_error_context(info_sys).code(), ErrorCode::InternalError);

  ErrorInfo info_unknown(ErrorLevel::ERROR, ErrorCategory::UNKNOWN, "comp", "op", "msg");
  EXPECT_EQ(to_error_context(info_unknown).code(), ErrorCode::IoError);
}

}  // namespace test
}  // namespace wirestead
