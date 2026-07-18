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

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace std::chrono_literals;
using wirestead::test::TestUtils;

// ============================================================================
// IMPROVED ARCHITECTURE TESTS
// ============================================================================

class ImprovedArchitectureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Stop IoContextManager for auto-init test
    if (wirestead::concurrency::IoContextManager::instance().is_running()) {
      std::cout << "Stopping IoContextManager for auto-init test..." << std::endl;
      wirestead::concurrency::IoContextManager::instance().stop();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void TearDown() override {
    try {
      if (client_) {
        std::cout << "Stopping client..." << std::endl;
        client_->stop();
        client_.reset();
      }
      if (server_) {
        std::cout << "Stopping server..." << std::endl;
        server_->stop();
        server_.reset();
      }

      // Clean up with sufficient time
      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      // IoContextManager is not managed individually in each test
      // It's a global state so it can affect other tests
    } catch (const std::exception& e) {
      std::cout << "Exception in TearDown: " << e.what() << std::endl;
    } catch (...) {
      std::cout << "Unknown exception in TearDown" << std::endl;
    }
  }

 protected:
  std::shared_ptr<wrapper::TcpServer> server_;
  std::shared_ptr<wrapper::TcpClient> client_;
};

/**
 * @brief Current Resource Sharing Issue Verification Test
 */
TEST_F(ImprovedArchitectureTest, CurrentResourceSharingIssue) {
  std::cout << "Testing current resource sharing issue..." << std::endl;

  uint16_t test_port = TestUtils::getAvailableTestPort();

  // Create server
  server_ = wirestead::tcp_server(test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  ASSERT_NE(server_, nullptr);
  std::cout << "Server created successfully" << std::endl;

  // Create client
  client_ = wirestead::tcp_client("127.0.0.1", test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  ASSERT_NE(client_, nullptr);
  std::cout << "Client created successfully" << std::endl;

  // Brief wait
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::cout << "Test completed - resource sharing issue demonstrated" << std::endl;
}

/**
 * @brief Proposed Independent Resource Management Test
 */
TEST_F(ImprovedArchitectureTest, ProposedIndependentResourceManagement) {
  std::cout << "Testing proposed independent resource management..." << std::endl;

  // Auto-initialization test using AutoInitializer
  EXPECT_FALSE(builder::AutoInitializer::io_context_running());

  // Auto-initialization
  builder::AutoInitializer::ensure_io_context_running();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  EXPECT_TRUE(builder::AutoInitializer::io_context_running());

  std::cout << "Independent resource management test completed" << std::endl;
}

/**
 * @brief Upper API Auto-initialization Test
 *
 * Every builder constructor (TcpServerBuilder, TcpClientBuilder, etc.) calls
 * AutoInitializer::ensure_io_context_running() unconditionally, so the global
 * IoContextManager singleton comes up as a side effect of constructing *any*
 * builder - this has nothing to do with whether the resulting transport
 * actually uses that shared context. Since #440, TcpServer/Serial default to
 * a dedicated io_context + thread and never touch the singleton at all; see
 * TcpServerIndependentOfIoContextManager below for the assertion that
 * actually matters post-#440 (the server keeps working even if the
 * singleton is stopped).
 */
TEST_F(ImprovedArchitectureTest, UpperAPIAutoInitialization) {
  std::cout << "Testing upper API auto-initialization..." << std::endl;

  uint16_t test_port = TestUtils::getAvailableTestPort();

  // Builder auto-initializes even if IoContextManager is not running
  if (wirestead::concurrency::IoContextManager::instance().is_running()) {
    wirestead::concurrency::IoContextManager::instance().stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // The *builder constructor* starts IoContextManager (AutoInitializer),
  // regardless of whether build()'s resulting server ends up using it.
  server_ = wirestead::tcp_server(test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  ASSERT_NE(server_, nullptr);

  // Check if IoContextManager started automatically
  EXPECT_TRUE(wirestead::concurrency::IoContextManager::instance().is_running());

  std::cout << "Upper API auto-initialization test completed" << std::endl;
}

/**
 * @brief #440: TcpServer defaults to a dedicated io_context + thread and
 * must keep running normally even if the (now-unrelated) global
 * IoContextManager singleton is stopped.
 */
TEST_F(ImprovedArchitectureTest, TcpServerIndependentOfIoContextManager) {
  uint16_t test_port = TestUtils::getAvailableTestPort();

  server_ = wirestead::tcp_server(test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(server_, nullptr);

  auto f = server_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return server_->listening(); }, 1000));

  // Stopping the global singleton (started as an AutoInitializer side effect
  // of building the *builder*, not because this server needs it) must not
  // affect this server's own dedicated io_context/thread at all.
  if (wirestead::concurrency::IoContextManager::instance().is_running()) {
    wirestead::concurrency::IoContextManager::instance().stop();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  EXPECT_TRUE(server_->listening());

  client_ = wirestead::tcp_client("127.0.0.1", test_port).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  client_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return client_->connected(); }, 2000));
}

/**
 * @brief Resource Sharing Analysis Test
 */
TEST_F(ImprovedArchitectureTest, ResourceSharingAnalysis) {
  std::cout << "Analyzing resource sharing..." << std::endl;

  // Resource management test through IoContextManager
  wirestead::concurrency::IoContextManager::instance().get_context();

  std::cout << "Resource sharing analysis completed" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}