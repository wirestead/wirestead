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
#include <thread>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class SafetyIntegratedTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }
  uint16_t port_;
};

TEST_F(SafetyIntegratedTest, RapidDestruction) {
  for (int i = 0; i < 5; ++i) {
    auto server = tcp_server(port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
    server->start();
    // Destroy immediately
  }
  SUCCEED();
}

TEST_F(SafetyIntegratedTest, NullCallbackSafety) {
  auto server = tcp_server(port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server->start();

  // These should not crash
  server->on_data(nullptr);
  server->on_connect(nullptr);
  server->on_disconnect(nullptr);
  server->on_error(nullptr);

  server->stop();
  SUCCEED();
}
