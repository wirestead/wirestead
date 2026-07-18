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
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/diagnostics/logger.hpp"

using namespace wirestead;
using namespace wirestead::diagnostics;
using namespace std::chrono_literals;
using wirestead::test::TestUtils;

/**
 * @brief Logger behavior tests
 * Tests uncovered functions in logger.cc
 */
class LoggerBehaviorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create unique log file for each test
    const ::testing::TestInfo* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string test_name = test_info->name();

    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    static std::atomic<uint64_t> seq{0};
    std::string file_name =
        "wirestead_logger_behavior_test_" + test_name + "_" + std::to_string(now) + "_" + std::to_string(seq++);
    test_log_file_ = TestUtils::makeTempFilePath(file_name);
    TestUtils::removeFileIfExists(test_log_file_);
  }

  void TearDown() override {
    TestUtils::removeFileIfExists(test_log_file_);
    // Reset logger state
    Logger::instance().set_enabled(true);
    Logger::instance().set_level(LogLevel::DEBUG);
    Logger::instance().set_console_output(false);
    Logger::instance().set_file_output("");
    Logger::instance().set_callback(nullptr);
    Logger::instance().set_format("{timestamp} [{level}] [{component}] [{operation}] {message}");
    clearLogLevelEnv();
    clearUnilinkLogLevelEnv();
  }

  void setLogLevelEnv(const std::string& value) {
#ifdef _WIN32
    _putenv_s("WIRESTEAD_LOG_LEVEL", value.c_str());
#else
    setenv("WIRESTEAD_LOG_LEVEL", value.c_str(), 1);
#endif
  }

  void clearLogLevelEnv() {
#ifdef _WIN32
    _putenv_s("WIRESTEAD_LOG_LEVEL", "");
#else
    unsetenv("WIRESTEAD_LOG_LEVEL");
#endif
  }

  void setUnilinkLogLevelEnv(const std::string& value) {
#ifdef _WIN32
    _putenv_s("UNILINK_LOG_LEVEL", value.c_str());
#else
    setenv("UNILINK_LOG_LEVEL", value.c_str(), 1);
#endif
  }

  void clearUnilinkLogLevelEnv() {
#ifdef _WIN32
    _putenv_s("UNILINK_LOG_LEVEL", "");
#else
    unsetenv("UNILINK_LOG_LEVEL");
#endif
  }

  std::filesystem::path test_log_file_;
};

// ============================================================================
// FLUSH FUNCTIONALITY TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, FlushWithFileOutput) {
  Logger::instance().set_file_output(test_log_file_.string());
  Logger::instance().set_level(LogLevel::DEBUG);

  // Log some messages
  WIRESTEAD_LOG_DEBUG("test", "operation", "Debug message");
  WIRESTEAD_LOG_INFO("test", "operation", "Info message");

  // Flush should work with file output
  Logger::instance().flush();

  // Verify file was created and contains messages
  std::ifstream file(test_log_file_);
  ASSERT_TRUE(file.is_open());

  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  EXPECT_TRUE(content.find("Debug message") != std::string::npos);
  EXPECT_TRUE(content.find("Info message") != std::string::npos);
}

TEST_F(LoggerBehaviorTest, FlushWithoutFileOutput) {
  // Flush should work even without file output
  Logger::instance().flush();
  // Should not crash
}

// ============================================================================
// WRITE TO CONSOLE TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, WriteToConsoleErrorLevel) {
  Logger::instance().set_console_output(true);
  Logger::instance().set_level(LogLevel::ERROR);

  // Test ERROR level console output
  WIRESTEAD_LOG_ERROR("test", "operation", "Error message");

  // Should not crash
}

TEST_F(LoggerBehaviorTest, WriteToConsoleCriticalLevel) {
  Logger::instance().set_console_output(true);
  Logger::instance().set_level(LogLevel::CRITICAL);

  // Test CRITICAL level console output
  WIRESTEAD_LOG_CRITICAL("test", "operation", "Critical message");

  // Should not crash
}

TEST_F(LoggerBehaviorTest, LogLevelFiltersOutLowerMessages) {
  Logger::instance().set_level(LogLevel::ERROR);
  // These should be filtered
  WIRESTEAD_LOG_DEBUG("test", "operation", "debug");
  WIRESTEAD_LOG_INFO("test", "operation", "info");
  // This should pass
  WIRESTEAD_LOG_ERROR("test", "operation", "error");
  Logger::instance().flush();
  SUCCEED();
}

// ============================================================================
// WRITE TO FILE TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, WriteToFileWithRotation) {
  LogRotationConfig config;
  config.max_file_size_bytes = 1000;  // Small size for testing
  config.max_files = 3;

  Logger::instance().set_file_output_with_rotation(test_log_file_.string(), config);
  Logger::instance().set_level(LogLevel::DEBUG);

  // Generate enough logs to trigger rotation
  for (int i = 0; i < 50; ++i) {
    WIRESTEAD_LOG_DEBUG("test", "operation",
                        "Message " + std::to_string(i) + " - " + std::string(50, 'x'));  // Long message
  }

  // Flush to ensure all messages are written
  Logger::instance().flush();

  // Check if original log file exists
  EXPECT_TRUE(std::filesystem::exists(test_log_file_));
}

TEST_F(LoggerBehaviorTest, WriteToFileWithoutOpenFile) {
  Logger::instance().set_file_output("");  // Clear file output
  Logger::instance().set_level(LogLevel::DEBUG);

  WIRESTEAD_LOG_DEBUG("test", "operation", "Message without file");

  // Should not crash
}

// ============================================================================
// LOG ROTATION TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, CheckAndRotateLog) {
  LogRotationConfig config;
  config.max_file_size_bytes = 500;  // Very small for testing
  config.max_files = 2;

  Logger::instance().set_file_output_with_rotation(test_log_file_.string(), config);
  Logger::instance().set_level(LogLevel::DEBUG);

  // Generate logs to trigger rotation
  for (int i = 0; i < 20; ++i) {
    WIRESTEAD_LOG_DEBUG("test", "operation", "Long message " + std::to_string(i) + " " + std::string(100, 'x'));
  }

  Logger::instance().flush();

  // Should not crash
}

// ============================================================================
// ASYNC LOGGING TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, AsyncLoggingEnabled) {
  AsyncLogConfig config;
  config.flush_interval = std::chrono::milliseconds(100);
  config.max_queue_size = 1000;

  Logger::instance().set_async_logging(true, config);

  EXPECT_TRUE(Logger::instance().async_logging_enabled());

  // Log some messages
  WIRESTEAD_LOG_DEBUG("test", "operation", "Async debug message");
  WIRESTEAD_LOG_INFO("test", "operation", "Async info message");

  // Ensure all async messages are flushed
  Logger::instance().flush();

  // Teardown async logging
  Logger::instance().set_async_logging(false, config);
  EXPECT_FALSE(Logger::instance().async_logging_enabled());
}

TEST_F(LoggerBehaviorTest, AsyncLoggingDisabled) {
  AsyncLogConfig config;
  config.flush_interval = std::chrono::milliseconds(100);
  config.max_queue_size = 1000;

  Logger::instance().set_async_logging(false, config);

  EXPECT_FALSE(Logger::instance().async_logging_enabled());

  // Log some messages
  WIRESTEAD_LOG_DEBUG("test", "operation", "Sync debug message");
  WIRESTEAD_LOG_INFO("test", "operation", "Sync info message");
}

// ============================================================================
// CALLBACK FUNCTIONALITY TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, LogCallback) {
  std::vector<std::string> captured_logs;

  // Set up callback
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });

  Logger::instance().set_level(LogLevel::DEBUG);

  // Log some messages
  WIRESTEAD_LOG_DEBUG("test", "operation", "Callback debug message");
  WIRESTEAD_LOG_INFO("test", "operation", "Callback info message");

  // Flush to ensure callback is called
  Logger::instance().flush();

  // Verify callback was called
  EXPECT_GE(captured_logs.size(), 2);

  // Check if messages contain expected content
  bool found_debug = false, found_info = false;
  for (const auto& log : captured_logs) {
    if (log.find("Callback debug message") != std::string::npos) found_debug = true;
    if (log.find("Callback info message") != std::string::npos) found_info = true;
  }

  EXPECT_TRUE(found_debug);
  EXPECT_TRUE(found_info);
}

// ============================================================================
// EDGE CASES AND ERROR CONDITIONS
// ============================================================================

TEST_F(LoggerBehaviorTest, LogWithEmptyComponent) {
  Logger::instance().set_level(LogLevel::DEBUG);
  WIRESTEAD_LOG_DEBUG("", "operation", "Message with empty component");
  // Should not crash
}

TEST_F(LoggerBehaviorTest, LogWithEmptyOperation) {
  Logger::instance().set_level(LogLevel::DEBUG);
  WIRESTEAD_LOG_DEBUG("component", "", "Message with empty operation");
  // Should not crash
}

TEST_F(LoggerBehaviorTest, LogWithEmptyMessage) {
  Logger::instance().set_level(LogLevel::DEBUG);
  WIRESTEAD_LOG_DEBUG("component", "operation", "");
  // Should not crash
}

TEST_F(LoggerBehaviorTest, LogWhenDisabled) {
  Logger::instance().set_enabled(false);
  Logger::instance().set_level(LogLevel::DEBUG);
  WIRESTEAD_LOG_DEBUG("test", "operation", "Message when disabled");
  // Should not crash
}

TEST_F(LoggerBehaviorTest, LogLevelFiltering) {
  Logger::instance().set_level(LogLevel::WARNING);
  WIRESTEAD_LOG_DEBUG("test", "operation", "Debug message");
  WIRESTEAD_LOG_INFO("test", "operation", "Info message");
  WIRESTEAD_LOG_WARNING("test", "operation", "Warning message");
  WIRESTEAD_LOG_ERROR("test", "operation", "Error message");
  // Should not crash
}

// ============================================================================
// CONCURRENT LOGGING TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, ConcurrentLogging) {
  Logger::instance().set_file_output(test_log_file_.string());
  Logger::instance().set_level(LogLevel::DEBUG);

  const int num_threads = 4;
  const int messages_per_thread = 10;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([t, messages_per_thread]() {
      for (int i = 0; i < messages_per_thread; ++i) {
        WIRESTEAD_LOG_DEBUG("thread" + std::to_string(t), "operation", "Message " + std::to_string(i));
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  Logger::instance().flush();
  std::ifstream file(test_log_file_);
  ASSERT_TRUE(file.is_open());
}

// ============================================================================
// ADDITIONAL COVERAGE TESTS
// ============================================================================

TEST_F(LoggerBehaviorTest, CallbackExceptionSafety) {
  // spdlog has its own error handler, so this might not behave exactly as before
  // but we ensure it doesn't crash the main thread.
  Logger::instance().set_callback([](LogLevel, const std::string&) { throw std::runtime_error("Callback exception"); });

  Logger::instance().set_level(LogLevel::INFO);

  EXPECT_NO_THROW(WIRESTEAD_LOG_INFO("test", "callback_exception", "Message"));

  Logger::instance().flush();
  Logger::instance().set_callback(nullptr);
}

TEST_F(LoggerBehaviorTest, CustomFormatHandling) {
  std::vector<std::string> captured_logs;
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });
  Logger::instance().set_level(LogLevel::INFO);
  Logger::instance().set_format("[{level}] {component}/{operation}: {message}");

  WIRESTEAD_LOG_INFO("test_component", "fmt_operation", "formatted message");
  Logger::instance().flush();

  ASSERT_FALSE(captured_logs.empty());
  EXPECT_NE(captured_logs.back().find("[info] test_component/fmt_operation: formatted message"), std::string::npos);

  Logger::instance().set_format("{timestamp} [{level}] [{component}] [{operation}] {message}");
  Logger::instance().set_callback(nullptr);
}

TEST_F(LoggerBehaviorTest, DisabledLoggerSkipsMacroMessageEvaluation) {
  Logger::instance().set_enabled(false);
  Logger::instance().set_level(LogLevel::DEBUG);

  int evaluations = 0;
  auto make_message = [&evaluations]() {
    ++evaluations;
    return std::string("expensive message");
  };

  WIRESTEAD_LOG_DEBUG("test", "disabled", make_message());

  EXPECT_EQ(evaluations, 0);
}

TEST_F(LoggerBehaviorTest, ParameterizedLogMacroUsesRuntimeLevel) {
  std::vector<std::string> captured_logs;
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });
  Logger::instance().set_level(LogLevel::WARNING);

  int level_evaluations = 0;
  auto choose_level = [&level_evaluations]() {
    ++level_evaluations;
    return LogLevel::WARNING;
  };

  WIRESTEAD_LOG(choose_level(), "test", "runtime_level", "runtime level message");
  Logger::instance().flush();

  EXPECT_EQ(level_evaluations, 1);
  ASSERT_FALSE(captured_logs.empty());
  EXPECT_NE(captured_logs.back().find("runtime level message"), std::string::npos);
  Logger::instance().set_callback(nullptr);
}

TEST_F(LoggerBehaviorTest, ParameterizedLogMacroSkipsFilteredMessageEvaluation) {
  Logger::instance().set_level(LogLevel::ERROR);

  int evaluations = 0;
  auto make_message = [&evaluations]() {
    ++evaluations;
    return std::string("filtered message");
  };

  const auto runtime_level = LogLevel::INFO;
  WIRESTEAD_LOG(runtime_level, "test", "filtered", make_message());

  EXPECT_EQ(evaluations, 0);
}

TEST_F(LoggerBehaviorTest, OutputsDisabledSkipsMessageEvaluation) {
  Logger::instance().set_enabled(true);
  Logger::instance().set_level(LogLevel::DEBUG);
  Logger::instance().set_outputs(0);

  int evaluations = 0;
  auto make_message = [&evaluations]() {
    ++evaluations;
    return std::string("no output message");
  };

  WIRESTEAD_LOG_INFO("test", "outputs_disabled", make_message());

  EXPECT_FALSE(Logger::instance().has_outputs());
  EXPECT_EQ(evaluations, 0);
}

TEST_F(LoggerBehaviorTest, ReloadsLogLevelFromEnvironment) {
  setLogLevelEnv("WARNING");

  Logger::instance().reload_from_environment();
  EXPECT_TRUE(Logger::instance().enabled());
  EXPECT_EQ(Logger::instance().level(), LogLevel::WARNING);

  setLogLevelEnv("OFF");

  Logger::instance().reload_from_environment();
  EXPECT_FALSE(Logger::instance().enabled());

  clearLogLevelEnv();
}

TEST_F(LoggerBehaviorTest, ReloadsLogLevelAliasesAndReportsInvalidEnvironment) {
  auto expect_level = [this](const std::string& value, LogLevel expected) {
    Logger::instance().set_enabled(true);
    setLogLevelEnv(value);
    Logger::instance().reload_from_environment();
    EXPECT_TRUE(Logger::instance().enabled());
    EXPECT_EQ(Logger::instance().level(), expected);
    EXPECT_TRUE(Logger::instance().last_error().empty());
  };

  expect_level(" trace ", LogLevel::DEBUG);
  expect_level("INFO", LogLevel::INFO);
  expect_level("warn", LogLevel::WARNING);
  expect_level("ERR", LogLevel::ERROR);
  expect_level("fatal", LogLevel::CRITICAL);

  setLogLevelEnv("none");
  Logger::instance().reload_from_environment();
  EXPECT_FALSE(Logger::instance().enabled());
  EXPECT_TRUE(Logger::instance().last_error().empty());

  Logger::instance().set_enabled(true);
  setLogLevelEnv("verbose");
  Logger::instance().reload_from_environment();
  EXPECT_FALSE(Logger::instance().last_error().empty());

  clearLogLevelEnv();
}

TEST_F(LoggerBehaviorTest, WiresteadLogLevelTakesPrecedenceOverUnilinkLogLevel) {
  // WIRESTEAD_LOG_LEVEL alone still works (no regression).
  Logger::instance().set_enabled(true);
  setLogLevelEnv("ERROR");
  Logger::instance().reload_from_environment();
  EXPECT_TRUE(Logger::instance().enabled());
  EXPECT_EQ(Logger::instance().level(), LogLevel::ERROR);
  EXPECT_TRUE(Logger::instance().last_error().empty());
  clearLogLevelEnv();

  // UNILINK_LOG_LEVEL alone works as a legacy fallback.
  Logger::instance().set_enabled(true);
  setUnilinkLogLevelEnv("WARNING");
  Logger::instance().reload_from_environment();
  EXPECT_TRUE(Logger::instance().enabled());
  EXPECT_EQ(Logger::instance().level(), LogLevel::WARNING);
  EXPECT_TRUE(Logger::instance().last_error().empty());
  clearUnilinkLogLevelEnv();

  // When both are set to different values, WIRESTEAD_LOG_LEVEL wins.
  Logger::instance().set_enabled(true);
  setLogLevelEnv("ERROR");
  setUnilinkLogLevelEnv("DEBUG");
  Logger::instance().reload_from_environment();
  EXPECT_TRUE(Logger::instance().enabled());
  EXPECT_EQ(Logger::instance().level(), LogLevel::ERROR);
  EXPECT_TRUE(Logger::instance().last_error().empty());

  clearLogLevelEnv();
  clearUnilinkLogLevelEnv();
}

TEST_F(LoggerBehaviorTest, CallbackReenabledAfterOutputsDisabled) {
  std::vector<std::string> captured_logs;

  Logger::instance().set_callback([](LogLevel, const std::string&) {});
  Logger::instance().set_outputs(0);
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });
  Logger::instance().set_level(LogLevel::INFO);

  WIRESTEAD_LOG_INFO("test", "callback", "callback restored");
  Logger::instance().flush();

  ASSERT_FALSE(captured_logs.empty());
  EXPECT_NE(captured_logs.back().find("callback restored"), std::string::npos);
}

TEST_F(LoggerBehaviorTest, OutputDisabling) {
  // Test set_file_output with empty string
  Logger::instance().set_file_output(test_log_file_.string());
  Logger::instance().set_file_output("");  // Disable

  // Test set_callback with nullptr
  Logger::instance().set_callback(nullptr);  // Disable

  // Test set_console_output
  Logger::instance().set_console_output(false);  // Disable

  // Log something - should go nowhere effectively (internal state check)
  WIRESTEAD_LOG_INFO("test", "disable", "Void message");

  // Re-enable console for other tests (TearDown handles this, but good practice)
  Logger::instance().set_console_output(true);
}

TEST_F(LoggerBehaviorTest, FileOpenFailure) {
  // Test set_file_output with invalid path
  // Assuming /invalid/path/test.log is not writable.
  // On Windows this might need a different invalid path like "Z:/..." or invalid chars.
  // Using a path with invalid characters is safer for cross-platform failure.
#ifdef _WIN32
  std::string invalid_path = "Z:\\nonexistent\\test.log";
#else
  std::string invalid_path = "/root/test_log_permission_denied.log";  // Assuming not running as root
#endif

  // This should print error to stderr but not throw
  EXPECT_FALSE(Logger::instance().try_set_file_output(invalid_path));
  EXPECT_FALSE(Logger::instance().last_error().empty());
}

TEST_F(LoggerBehaviorTest, UnsupportedRotationOptionsReportFailure) {
  LogRotationConfig config;
  config.enable_compression = true;

  EXPECT_FALSE(Logger::instance().try_set_file_output_with_rotation(test_log_file_.string(), config));
  EXPECT_FALSE(Logger::instance().last_error().empty());
}

TEST_F(LoggerBehaviorTest, ConsoleErrorStreamSelection) {
  // We can't easily capture stderr/stdout directly in a portable way with GTest,
  // but we can ensure the code paths are exercised and no crash occurs.
  Logger::instance().set_console_output(true);
  Logger::instance().set_level(LogLevel::DEBUG);  // Ensure all levels are processed

  // Normal message (stdout via INFO)
  WIRESTEAD_LOG_INFO("test", "console", "Normal message");

  // Error message (stderr via ERROR)
  WIRESTEAD_LOG_ERROR("test", "console", "Error message");

  // Critical message (stderr via CRITICAL)
  WIRESTEAD_LOG_CRITICAL("test", "console", "Critical message");

  // Clean up
  Logger::instance().set_console_output(false);
}

TEST_F(LoggerBehaviorTest, OutputBitmaskRestoresFileAndCallbackSinks) {
  std::vector<std::string> captured_logs;

  Logger::instance().set_file_output(test_log_file_.string());
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });
  Logger::instance().set_outputs(0);
  EXPECT_FALSE(Logger::instance().has_outputs());

  Logger::instance().set_outputs(static_cast<int>(LogOutput::FILE) | static_cast<int>(LogOutput::CALLBACK));
  Logger::instance().set_level(LogLevel::INFO);
  WIRESTEAD_LOG_INFO("test", "outputs", "restored outputs");
  Logger::instance().flush();

  ASSERT_FALSE(captured_logs.empty());
  EXPECT_NE(captured_logs.back().find("restored outputs"), std::string::npos);

  std::ifstream file(test_log_file_);
  ASSERT_TRUE(file.is_open());
  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  EXPECT_NE(content.find("restored outputs"), std::string::npos);
}

TEST_F(LoggerBehaviorTest, UnsupportedRotationPatternReportsFailure) {
  LogRotationConfig config;
  config.file_pattern = "{name}-{index}.txt";

  EXPECT_FALSE(Logger::instance().try_set_file_output_with_rotation(test_log_file_.string(), config));
  EXPECT_FALSE(Logger::instance().last_error().empty());
}

TEST_F(LoggerBehaviorTest, DirectLevelMethodsAndDefaultLoggerAlias) {
  std::vector<std::string> captured_logs;
  Logger::instance().set_callback(
      [&captured_logs](LogLevel /* level */, const std::string& message) { captured_logs.push_back(message); });
  Logger::instance().set_level(LogLevel::DEBUG);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  auto& logger = Logger::default_logger();
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
  logger.debug("direct", "debug", "debug direct");
  logger.info("direct", "info", "info direct");
  logger.warning("direct", "warning", "warning direct");
  logger.error("direct", "error", "error direct");
  logger.critical("direct", "critical", "critical direct");
  logger.flush();

  ASSERT_GE(captured_logs.size(), 5u);
}

TEST_F(LoggerBehaviorTest, ReenablingAsyncLoggingTearsDownExistingAsyncLogger) {
  AsyncLogConfig config;
  config.flush_interval = std::chrono::milliseconds(0);
  config.shutdown_timeout = std::chrono::milliseconds(1000);

  Logger::instance().set_async_logging(true, config);
  EXPECT_TRUE(Logger::instance().async_logging_enabled());

  Logger::instance().set_async_logging(true, config);
  EXPECT_TRUE(Logger::instance().async_logging_enabled());

  Logger::instance().set_async_logging(false, config);
  EXPECT_FALSE(Logger::instance().async_logging_enabled());
}
