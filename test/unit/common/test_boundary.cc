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
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/builder/unified_builder.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/config/serial_config.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/util/input_validator.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace wirestead::transport;
using namespace wirestead::config;
using namespace wirestead::memory;
using namespace wirestead::diagnostics;
using namespace wirestead::concurrency;
using namespace wirestead::util;
using namespace wirestead::builder;
using namespace std::chrono_literals;

namespace wirestead {
namespace test {

/**
 * @brief Boundary condition tests for critical components
 */
class BoundaryTest : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetUp(); }

  void TearDown() override { BaseTest::TearDown(); }
};

// ============================================================================
// MEMORY POOL BOUNDARY TESTS
// ============================================================================

TEST_F(BoundaryTest, MemoryPoolBoundaryConditions) {
  auto& pool = memory::GlobalMemoryPool::instance();

  // 1. 최소 크기 테스트
  auto min_buffer = pool.acquire(1);
  EXPECT_NE(min_buffer, nullptr);
  if (min_buffer) {
    pool.release(std::move(min_buffer), 1);
  }

  // 2. 최대 풀 크기 테스트
  auto max_pool_buffer = pool.acquire(65536);
  EXPECT_NE(max_pool_buffer, nullptr);
  if (max_pool_buffer) {
    pool.release(std::move(max_pool_buffer), 65536);
  }
}

TEST_F(BoundaryTest, MemoryPoolPredefinedSizes) {
  auto& pool = memory::GlobalMemoryPool::instance();
  std::vector<size_t> predefined_sizes = {1024, 4096, 16384, 65536};

  for (size_t size : predefined_sizes) {
    auto buffer = pool.acquire(size);
    EXPECT_NE(buffer, nullptr);
    if (buffer) {
      pool.release(std::move(buffer), size);
    }
  }
}

TEST_F(BoundaryTest, MemoryPoolOverload) {
  auto& pool = memory::GlobalMemoryPool::instance();
  std::vector<std::unique_ptr<uint8_t[]>> buffers;

  // Allocate many buffers to force pool growth
  for (int i = 0; i < 100; ++i) {
    buffers.push_back(pool.acquire(4096));
    ASSERT_NE(buffers.back(), nullptr);
  }

  // Release all
  for (auto& buf : buffers) {
    pool.release(std::move(buf), 4096);
  }
}

// ============================================================================
// INPUT VALIDATOR EDGE TESTS
// ============================================================================

TEST_F(BoundaryTest, InputValidatorEdgeCases) {
  // IP Address Edge Cases
  EXPECT_TRUE(InputValidator::is_valid_ipv4("0.0.0.0"));
  EXPECT_TRUE(InputValidator::is_valid_ipv4("255.255.255.255"));
  EXPECT_FALSE(InputValidator::is_valid_ipv4("256.0.0.1"));
  EXPECT_FALSE(InputValidator::is_valid_ipv4("1.2.3"));
  EXPECT_FALSE(InputValidator::is_valid_ipv4("a.b.c.d"));
  EXPECT_FALSE(InputValidator::is_valid_ipv4(""));

  // Port Edge Cases (port 0 is invalid)
  EXPECT_THROW(InputValidator::validate_port(0), diagnostics::ValidationException);
  EXPECT_NO_THROW(InputValidator::validate_port(1));
  EXPECT_NO_THROW(InputValidator::validate_port(65535));

  // Hostname Edge Cases
  EXPECT_TRUE(InputValidator::is_valid_hostname("localhost"));
  EXPECT_TRUE(InputValidator::is_valid_hostname("example.com"));
  EXPECT_TRUE(InputValidator::is_valid_hostname("a.b-c.com"));
  EXPECT_FALSE(InputValidator::is_valid_hostname("-abc.com"));
  EXPECT_FALSE(InputValidator::is_valid_hostname("abc-.com"));
}
// ============================================================================
// CONFIGURATION BOUNDARY TESTS
// ============================================================================

TEST_F(BoundaryTest, TcpClientConfigBoundaries) {
  TcpClientConfig valid_min;
  valid_min.host = "127.0.0.1";
  valid_min.port = 1;
  valid_min.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
  EXPECT_TRUE(valid_min.is_valid());
}

TEST_F(BoundaryTest, TcpServerConfigBoundaries) {
  TcpServerConfig valid_min;
  valid_min.port = 1;
  EXPECT_TRUE(valid_min.is_valid());
}

TEST_F(BoundaryTest, SerialConfigBoundaries) {
  SerialConfig valid_min;
  valid_min.device = "/dev/ttyUSB0";
  valid_min.baud_rate = 9600;
  EXPECT_TRUE(valid_min.is_valid());
}

}  // namespace test
}  // namespace wirestead

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}