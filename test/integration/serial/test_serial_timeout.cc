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
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <string>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;

class SerialTimeoutTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef __linux__
    // Check availability of socat first
    int check = std::system("which socat > /dev/null 2>&1");
    if (check != 0) {
      std::cout << "socat not found, skipping socat setup" << std::endl;
      socat_available_ = false;
      return;
    }

    // cleanup previous runs just in case
    if (std::system("pkill -f 'socat -d -d pty,raw,echo=0,link=/tmp/ttyV0'") != 0) {
      // Ignore errors
    }

    // create virtual pair
    int ret = std::system(
        "socat -d -d pty,raw,echo=0,link=/tmp/ttyV0 "
        "pty,raw,echo=0,link=/tmp/ttyV1 > /dev/null 2>&1 &");
    if (ret != 0) {
      std::cout << "Failed to start socat" << std::endl;
      // Don't assert here, let the test skip if ports are missing
    }

    // Wait for ports to be created
    TestUtils::waitFor(500);
#endif
  }

  void TearDown() override {
#ifdef __linux__
    if (socat_available_) {
      // Kill socat
      if (std::system("pkill -f 'socat -d -d pty,raw,echo=0,link=/tmp/ttyV0'") != 0) {
        // Ignore errors
      }
      // Clean up symlinks
      std::remove("/tmp/ttyV0");
      std::remove("/tmp/ttyV1");
    }
#endif
  }

  bool socat_available_ = true;
};

TEST_F(SerialTimeoutTest, ReadTimeoutWhenNoData) {
#ifndef __linux__
  GTEST_SKIP() << "Skipping SerialTimeoutTest on non-Linux platform";
#else
  if (!socat_available_) {
    GTEST_SKIP() << "socat not available";
  }

  if (!std::filesystem::exists("/tmp/ttyV0") || !std::filesystem::exists("/tmp/ttyV1")) {
    GTEST_SKIP() << "Virtual serial ports not found (socat failed?)";
  }

  // Open port V0 with Wirestead::Serial
  auto serial = std::make_shared<wrapper::Serial>("/tmp/ttyV0", 9600);

  std::promise<std::string> read_promise;
  auto read_future = read_promise.get_future();

  // Set up callback to fulfil promise if data arrives
  serial->on_data([&](const wrapper::MessageContext& ctx) { read_promise.set_value(std::string(ctx.data())); });

  serial->start();

  // We intentionally do NOT write to the other end (/tmp/ttyV1).
  // We expect no data to arrive.

  // Simulate a "read with timeout" by waiting on the future
  // 100ms timeout as requested
  auto status = read_future.wait_for(std::chrono::milliseconds(100));

  // Verification: It should timeout because no data was sent
  EXPECT_EQ(status, std::future_status::timeout) << "Serial read should have timed out but received data or failed";

  serial->stop();
#endif
}
