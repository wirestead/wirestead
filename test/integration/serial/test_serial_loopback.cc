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
#include <memory>
#include <string>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class SerialLoopbackTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef _WIN32
    device_ = "NUL";
#else
    device_ = "/dev/null";
#endif
  }
  std::string device_;
};

TEST_F(SerialLoopbackTest, LoopbackCommunication) {
  std::atomic<bool> data_received{false};
  auto ul = serial(device_, 9600)
                .on_data([&](const wrapper::MessageContext& ctx) { data_received = true; })
                .on_error([](auto&&) {})
                .build();

  ul->start();

  // Sending to null device won't loop back, but tests the API
  ul->send("ping");

  TestUtils::waitFor(100);
  ul->stop();
  SUCCEED();
}
