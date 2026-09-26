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

#include "wrapper_contract_test_utils.hpp"
using namespace wirestead;
using namespace wirestead::test::wrapper_support;
using namespace std::chrono_literals;
namespace {
template <class H>
class ServerConfigPolicyTest : public ::testing::Test {};
using Servers = ::testing::Types<TcpServerLoopbackHarness, UdsServerLoopbackHarness, UdpServerLoopbackHarness>;
TYPED_TEST_SUITE(ServerConfigPolicyTest, Servers);
TYPED_TEST(ServerConfigPolicyTest, LiveAllowlistAndCompletedStopBoundary) {
  TypeParam harness;
  auto server = harness.start_server();
  const auto prior = server->backpressure_threshold();
  EXPECT_THROW(server->backpressure_threshold(2048), std::logic_error);
  EXPECT_EQ(server->backpressure_threshold(), prior);
  if constexpr (std::is_same_v<TypeParam, UdpServerLoopbackHarness>) {
    EXPECT_NO_THROW(server->idle_timeout(100ms));
    EXPECT_NO_THROW(server->backpressure_strategy(base::constants::BackpressureStrategy::BestEffort));
  } else {
    EXPECT_THROW(server->idle_timeout(100ms), std::logic_error);
    EXPECT_THROW(server->backpressure_strategy(base::constants::BackpressureStrategy::BestEffort), std::logic_error);
  }
  EXPECT_THROW(server->manage_external_context(true), std::logic_error);
  EXPECT_NO_THROW(server->max_clients(2));
  EXPECT_NO_THROW(server->batch_size(2).batch_latency(10ms));
  EXPECT_THROW(server->max_clients(std::numeric_limits<size_t>::max()), std::invalid_argument);
  server->stop();
  EXPECT_NO_THROW(server->backpressure_threshold(2048));
  EXPECT_NO_THROW(server->idle_timeout(100ms));
  harness.stop_all();
}
}  // namespace
