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

#include "wirestead/base/error_codes.hpp"

namespace wirestead {
namespace test {

TEST(ErrorCodesToStringTest, ToStringAllCodes) {
  EXPECT_EQ(to_string(ErrorCode::Success), "Success");
  EXPECT_EQ(to_string(ErrorCode::Unknown), "Unknown Error");
  EXPECT_EQ(to_string(ErrorCode::InvalidConfiguration), "Invalid Configuration");
  EXPECT_EQ(to_string(ErrorCode::InternalError), "Internal Error");
  EXPECT_EQ(to_string(ErrorCode::IoError), "I/O Error");
  EXPECT_EQ(to_string(ErrorCode::ConnectionRefused), "Connection Refused");
  EXPECT_EQ(to_string(ErrorCode::ConnectionReset), "Connection Reset");
  EXPECT_EQ(to_string(ErrorCode::ConnectionAborted), "Connection Aborted");
  EXPECT_EQ(to_string(ErrorCode::TimedOut), "Operation Timed Out");
  EXPECT_EQ(to_string(ErrorCode::NotConnected), "Not Connected");
  EXPECT_EQ(to_string(ErrorCode::AlreadyConnected), "Already Connected");
  EXPECT_EQ(to_string(ErrorCode::PortInUse), "Port Already In Use");
  EXPECT_EQ(to_string(ErrorCode::AccessDenied), "Access Denied");
  EXPECT_EQ(to_string(ErrorCode::Stopped), "Stopped");
  EXPECT_EQ(to_string(ErrorCode::StartFailed), "Failed to Start");

  // Coverage for default case
  EXPECT_EQ(to_string(static_cast<ErrorCode>(999)), "Unknown Error Code");
}

}  // namespace test
}  // namespace wirestead
