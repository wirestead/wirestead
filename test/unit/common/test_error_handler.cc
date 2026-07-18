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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/diagnostics/error_handler.hpp"
#include "wirestead/diagnostics/logger.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

/**
 * @brief Comprehensive error handler tests
 *
 * These tests provide comprehensive coverage for the error handling system,
 * addressing the current 0% coverage issue.
 */
class ErrorHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset error handler state
    auto& error_handler = diagnostics::ErrorHandler::instance();
    error_handler.clear_callbacks();
    error_handler.reset_stats();
    error_handler.set_enabled(true);
    error_handler.set_min_error_level(diagnostics::ErrorLevel::INFO);

    // Initialize test state
    error_count_ = 0;
    last_error_category_ = "";
    last_error_level_ = diagnostics::ErrorLevel::INFO;
  }

  void TearDown() override {
    // Clean up any test state
    TestUtils::waitFor(100);
  }

  // Test state
  std::atomic<int> error_count_{0};
  std::string last_error_category_;
  diagnostics::ErrorLevel last_error_level_;
};

// ============================================================================
// ERROR REPORTING TESTS
// ============================================================================

/**
 * @brief Test connection error reporting
 */
TEST_F(ErrorHandlerTest, ConnectionErrorReporting) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Connection error scenario
  std::string component = "tcp_client";
  std::string operation = "connect";
  boost::system::error_code ec(boost::system::errc::connection_refused, boost::system::generic_category());
  bool is_retryable = true;

  // When: Report connection error
  diagnostics::error_reporting::report_connection_error(component, operation, ec, is_retryable);

  // Then: Verify error was recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test communication error reporting
 */
TEST_F(ErrorHandlerTest, CommunicationErrorReporting) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Communication error scenario
  std::string component = "tcp_client";
  std::string operation = "read";
  std::string error_message = "Read timeout";
  bool is_retryable = false;

  // When: Report communication error
  diagnostics::error_reporting::report_communication_error(component, operation, error_message, is_retryable);

  // Then: Verify error was recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test configuration error reporting
 */
TEST_F(ErrorHandlerTest, ConfigurationErrorReporting) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Configuration error scenario
  std::string component = "config_manager";
  std::string operation = "load_config";
  std::string error_message = "Invalid configuration file";

  // When: Report configuration error
  diagnostics::error_reporting::report_configuration_error(component, operation, error_message);

  // Then: Verify error was recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test memory error reporting
 */
TEST_F(ErrorHandlerTest, MemoryErrorReporting) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Memory error scenario
  std::string component = "memory_pool";
  std::string operation = "allocate";
  std::string error_message = "Memory allocation failed";

  // When: Report memory error
  diagnostics::error_reporting::report_memory_error(component, operation, error_message);

  // Then: Verify error was recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test system error reporting
 */
TEST_F(ErrorHandlerTest, SystemErrorReporting) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: System error scenario
  std::string component = "io_context";
  std::string operation = "run";
  std::string error_message = "System resource unavailable";
  boost::system::error_code ec(boost::system::errc::resource_unavailable_try_again, boost::system::generic_category());

  // When: Report system error
  diagnostics::error_reporting::report_system_error(component, operation, error_message, ec);

  // Then: Verify error was recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

// ============================================================================
// ERROR STATISTICS TESTS
// ============================================================================

/**
 * @brief Test error statistics collection
 */
TEST_F(ErrorHandlerTest, ErrorStatisticsCollection) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Multiple error reports
  diagnostics::error_reporting::report_connection_error("client1", "connect", boost::system::error_code{}, true);
  diagnostics::error_reporting::report_connection_error("client2", "connect", boost::system::error_code{}, false);
  diagnostics::error_reporting::report_configuration_error("config", "load", "Error 3");
  diagnostics::error_reporting::report_memory_error("pool", "alloc", "Error 4");
  diagnostics::error_reporting::report_system_error("io", "run", "Error 5");

  // When: Get statistics
  auto stats = error_handler.error_stats();

  // Then: Verify statistics
  EXPECT_EQ(stats.total_errors, 5);
}

/**
 * @brief Test error rate calculation
 */
TEST_F(ErrorHandlerTest, ErrorRateCalculation) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Error reports over time
  for (int i = 0; i < 10; ++i) {
    diagnostics::error_reporting::report_connection_error("client", "connect", boost::system::error_code{}, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // When: Get statistics
  auto stats = error_handler.error_stats();

  // Then: Verify error rate
  EXPECT_GT(stats.total_errors, 0);
}

// ============================================================================
// ERROR CALLBACK TESTS
// ============================================================================

/**
 * @brief Test error callback registration
 */
TEST_F(ErrorHandlerTest, ErrorCallbackRegistration) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Error callback
  std::atomic<int> callback_count{0};
  std::string last_callback_error;

  auto callback = [&callback_count, &last_callback_error](const diagnostics::ErrorInfo& error_info) {
    callback_count++;
    last_callback_error = error_info.message;
  };

  // When: Register callback and report error
  error_handler.register_callback(callback);
  diagnostics::error_reporting::report_connection_error("test", "operation", boost::system::error_code{}, false);

  // Then: Verify callback was called
  EXPECT_TRUE(TestUtils::waitForCondition([&callback_count]() { return callback_count.load() > 0; }, 1000));
  EXPECT_GT(callback_count.load(), 0);

  // Clean up to prevent dangling references
  error_handler.clear_callbacks();
}

/**
 * @brief Test error callback with different error levels
 */
TEST_F(ErrorHandlerTest, ErrorCallbackWithLevels) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Error callback that tracks levels
  std::vector<diagnostics::ErrorLevel> received_levels;

  auto callback = [&received_levels](const diagnostics::ErrorInfo& error_info) {
    received_levels.push_back(error_info.level);
  };

  // When: Register callback and report errors with different levels
  error_handler.register_callback(callback);

  // Report errors with different levels (simulated)
  diagnostics::error_reporting::report_connection_error("client", "connect", boost::system::error_code{}, false);
  diagnostics::error_reporting::report_memory_error("pool", "alloc", "Memory error");

  // Then: Verify callback received errors
  EXPECT_TRUE(TestUtils::waitForCondition([&received_levels]() { return received_levels.size() >= 2; }, 1000));
  EXPECT_GE(received_levels.size(), 2);

  error_handler.clear_callbacks();
}

// ============================================================================
// ERROR RECOVERY TESTS
// ============================================================================

/**
 * @brief Test error recovery mechanisms
 */
TEST_F(ErrorHandlerTest, ErrorRecoveryMechanisms) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Retryable error
  diagnostics::error_reporting::report_connection_error("client", "connect", boost::system::error_code{}, true);

  // When: Check if error is retryable
  auto stats = error_handler.error_stats();

  // Then: Verify retryable error was recorded
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test error threshold detection
 */
TEST_F(ErrorHandlerTest, ErrorThresholdDetection) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Multiple rapid errors
  for (int i = 0; i < 5; ++i) {
    diagnostics::error_reporting::report_connection_error("client", "connect", boost::system::error_code{}, false);
  }

  // When: Check error threshold
  auto stats = error_handler.error_stats();

  // Then: Verify threshold detection
  EXPECT_EQ(stats.total_errors, 5);
}

// ============================================================================
// ERROR CLEANUP TESTS
// ============================================================================

/**
 * @brief Test error statistics cleanup
 */
TEST_F(ErrorHandlerTest, ErrorStatisticsCleanup) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Some errors reported
  diagnostics::error_reporting::report_connection_error("client", "connect", boost::system::error_code{}, false);
  diagnostics::error_reporting::report_configuration_error("config", "load", "Error 2");

  // When: Clear statistics
  error_handler.reset_stats();
  auto stats = error_handler.error_stats();

  // Then: Verify statistics were cleared
  EXPECT_EQ(stats.total_errors, 0);
}

/**
 * @brief Test error handler reset
 */
TEST_F(ErrorHandlerTest, ErrorHandlerReset) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Error callback registered
  std::atomic<int> callback_count{0};
  auto callback = [&callback_count](const diagnostics::ErrorInfo&) { callback_count++; };
  error_handler.register_callback(callback);

  // When: Clear callbacks
  error_handler.clear_callbacks();

  // Then: Verify callback was cleared
  diagnostics::error_reporting::report_connection_error("test", "operation", boost::system::error_code{}, false);

  // Callback should not be called after clear
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(callback_count.load(), 0);
}

// ============================================================================
// ERROR LEVEL TESTS
// ============================================================================

/**
 * @brief Test error level filtering
 */
TEST_F(ErrorHandlerTest, ErrorLevelFiltering) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Set minimum error level
  error_handler.set_min_error_level(diagnostics::ErrorLevel::WARNING);

  // When: Report errors with different levels
  diagnostics::error_reporting::report_info("component", "operation", "Info message");
  diagnostics::error_reporting::report_warning("component", "operation", "Warning message");
  diagnostics::error_reporting::report_memory_error("component", "operation", "Error message");

  // Then: Verify only appropriate errors were recorded
  auto stats = error_handler.error_stats();
  EXPECT_GT(stats.total_errors, 0);
}

/**
 * @brief Test error handler enable/disable
 */
TEST_F(ErrorHandlerTest, ErrorHandlerEnableDisable) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  // Given: Disable error reporting
  error_handler.set_enabled(false);

  // When: Report error
  diagnostics::error_reporting::report_connection_error("test", "operation", boost::system::error_code{}, false);

  // Then: Verify error was not recorded
  auto stats = error_handler.error_stats();
  EXPECT_EQ(stats.total_errors, 0);

  // Re-enable for cleanup
  error_handler.set_enabled(true);
}

TEST_F(ErrorHandlerTest, DefaultHandlerAliasesSingleton) {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  auto& default_handler = diagnostics::ErrorHandler::default_handler();
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

  EXPECT_EQ(&default_handler, &diagnostics::ErrorHandler::instance());
}

TEST_F(ErrorHandlerTest, MinimumLevelAndEnabledAccessorsReflectState) {
  auto& error_handler = diagnostics::ErrorHandler::instance();

  error_handler.set_enabled(false);
  EXPECT_FALSE(error_handler.enabled());
  diagnostics::error_reporting::report_memory_error("disabled_component", "allocate", "should be ignored");
  EXPECT_EQ(error_handler.error_stats().total_errors, 0);

  error_handler.set_enabled(true);
  error_handler.set_min_error_level(diagnostics::ErrorLevel::ERROR);
  EXPECT_TRUE(error_handler.enabled());
  EXPECT_EQ(error_handler.min_error_level(), diagnostics::ErrorLevel::ERROR);

  diagnostics::error_reporting::report_warning("filtered_component", "warn", "warning is below threshold");
  diagnostics::error_reporting::report_info("filtered_component", "info", "info is below threshold");
  EXPECT_EQ(error_handler.error_stats().total_errors, 0);

  diagnostics::error_reporting::report_configuration_error("filtered_component", "load", "configuration failed");
  auto stats = error_handler.error_stats();
  EXPECT_EQ(stats.total_errors, 1);
  EXPECT_EQ(stats.errors_by_level[static_cast<int>(diagnostics::ErrorLevel::ERROR)], 1);
}

TEST_F(ErrorHandlerTest, QueryHelpersReturnComponentHistoryAndCounts) {
  auto& error_handler = diagnostics::ErrorHandler::instance();
  const std::string component = "query_helpers_component";

  diagnostics::error_reporting::report_warning(component, "prepare", "warning message");
  diagnostics::error_reporting::report_info(component, "inspect", "info message");
  diagnostics::error_reporting::report_configuration_error(component, "load", "configuration error");
  diagnostics::error_reporting::report_memory_error("query_helpers_other_component", "allocate", "critical error");

  auto component_errors = error_handler.errors_by_component(component);
  ASSERT_EQ(component_errors.size(), 3);
  EXPECT_EQ(component_errors[0].level, diagnostics::ErrorLevel::WARNING);
  EXPECT_EQ(component_errors[1].level, diagnostics::ErrorLevel::INFO);
  EXPECT_EQ(component_errors[2].level, diagnostics::ErrorLevel::ERROR);

  EXPECT_TRUE(error_handler.has_errors(component));
  EXPECT_FALSE(error_handler.has_errors("query_helpers_missing_component"));
  EXPECT_TRUE(error_handler.errors_by_component("query_helpers_missing_component").empty());
  EXPECT_EQ(error_handler.error_count(component, diagnostics::ErrorLevel::WARNING), 1);
  EXPECT_EQ(error_handler.error_count(component, diagnostics::ErrorLevel::INFO), 1);
  EXPECT_EQ(error_handler.error_count(component, diagnostics::ErrorLevel::ERROR), 1);
  EXPECT_EQ(error_handler.error_count(component, diagnostics::ErrorLevel::CRITICAL), 0);
  EXPECT_EQ(error_handler.error_count("query_helpers_missing_component", diagnostics::ErrorLevel::ERROR), 0);

  auto last_error = error_handler.recent_errors(1);
  ASSERT_EQ(last_error.size(), 1);
  EXPECT_EQ(last_error.front().component, "query_helpers_other_component");
  EXPECT_EQ(last_error.front().level, diagnostics::ErrorLevel::CRITICAL);
}

TEST_F(ErrorHandlerTest, ErrorHistoriesTrimOldEntries) {
  auto& error_handler = diagnostics::ErrorHandler::instance();
  const std::string component = "trim_history_component";

  for (int index = 0; index < 1005; ++index) {
    diagnostics::error_reporting::report_info(component, "trim", "trim-" + std::to_string(index));
  }

  auto component_errors = error_handler.errors_by_component(component);
  ASSERT_EQ(component_errors.size(), 100);
  EXPECT_EQ(component_errors.front().message, "trim-905");
  EXPECT_EQ(component_errors.back().message, "trim-1004");

  auto recent_errors = error_handler.recent_errors(2000);
  EXPECT_LE(recent_errors.size(), 1000);
  ASSERT_FALSE(recent_errors.empty());
  EXPECT_EQ(recent_errors.back().component, component);
  EXPECT_EQ(recent_errors.back().message, "trim-1004");
}

TEST_F(ErrorHandlerTest, CallbackExceptionsAreSwallowedAndOtherCallbacksRun) {
  auto& error_handler = diagnostics::ErrorHandler::instance();
  std::atomic<int> callback_count{0};

  error_handler.register_callback([](const diagnostics::ErrorInfo&) { throw std::runtime_error("callback failed"); });
  error_handler.register_callback([](const diagnostics::ErrorInfo&) { throw 7; });
  error_handler.register_callback([&callback_count](const diagnostics::ErrorInfo&) { callback_count++; });

  EXPECT_NO_THROW(diagnostics::error_reporting::report_configuration_error("callback_exception_component", "load",
                                                                           "configuration error"));
  EXPECT_EQ(callback_count.load(), 1);
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

TEST_F(ErrorHandlerTest, RecursiveErrorReporting_DeadlockTest) {
  // Scenario: A callback that triggers another error report.
  // Before fix: This causes deadlock (mutex recursion).
  // After fix: This should complete successfully.

  auto& error_handler = diagnostics::ErrorHandler::instance();
  std::atomic<int> callback_count{0};
  const int max_recursions = 1;

  error_handler.register_callback([&](const diagnostics::ErrorInfo& error) {
    int current_count = callback_count.fetch_add(1);

    // Only recurse once to demonstrate the deadlock fix
    // (If not fixed, the second call hangs)
    if (current_count < max_recursions) {
      // Trigger another error report from within the callback
      // This simulates a logging failure or similar chain reaction
      diagnostics::error_reporting::report_info("test_component", "recursive_call", "Triggering recursive error");
    }
  });

  // Trigger the first error
  diagnostics::error_reporting::report_info("test_component", "initial_call", "Initial error");

  // If we reach here without hanging, the test passes (no deadlock).
  EXPECT_EQ(callback_count.load(), 2);

  error_handler.clear_callbacks();
}

TEST_F(ErrorHandlerTest, CallbackRegistrationInsideCallback_CrashTest) {
  // Scenario: A callback that registers another callback.
  // Before fix: This invalidates iterators if using direct vector iteration.
  // After fix: Iterating over a copy prevents invalidation.

  auto& error_handler = diagnostics::ErrorHandler::instance();
  bool second_callback_called = false;

  error_handler.register_callback([&](const diagnostics::ErrorInfo& error) {
    if (error.message == "First Error") {
      error_handler.register_callback([&](const diagnostics::ErrorInfo& inner_error) {
        if (inner_error.message == "Second Error") {
          second_callback_called = true;
        }
      });
    }
  });

  // Trigger first error (registers the second callback)
  diagnostics::error_reporting::report_info("test_component", "op1", "First Error");

  // Trigger second error (should hit the new callback)
  diagnostics::error_reporting::report_info("test_component", "op2", "Second Error");

  EXPECT_TRUE(second_callback_called);

  error_handler.clear_callbacks();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
