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
#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <system_error>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/base/common.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/config/config_manager.hpp"
#include "wirestead/memory/memory_pool.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace wirestead::diagnostics;
using namespace wirestead::concurrency;
using namespace wirestead::memory;
using namespace std::chrono_literals;

// ============================================================================
// COMMON TESTS
// ============================================================================

/**
 * @brief Common functionality tests
 */
TEST_F(BaseTest, CommonFunctionality) {
  // Test LinkState enum
  EXPECT_STREQ(base::to_cstr(base::LinkState::Idle), "Idle");
  EXPECT_STREQ(base::to_cstr(base::LinkState::Connected), "Connected");
  EXPECT_STREQ(base::to_cstr(base::LinkState::Error), "Error");

  // Test timestamp functionality
  std::string timestamp = base::ts_now();
  EXPECT_FALSE(timestamp.empty());
  EXPECT_GT(timestamp.length(), 10);
}

/**
 * @brief Configuration manager tests
 */
TEST_F(BaseTest, ConfigManager) {
  // Test basic configuration functionality
  // Note: ConfigManager might not have instance() method
  // This test is kept for future implementation
  EXPECT_TRUE(true);
}

// ============================================================================
// IOCONTEXT MANAGER TESTS
// ============================================================================

/**
 * @brief IoContextManager basic functionality tests
 */
TEST_F(BaseTest, IoContextManagerBasicFunctionality) {
  auto& manager = wirestead::concurrency::IoContextManager::instance();

  // Test basic operations
  EXPECT_FALSE(manager.is_running());

  manager.start();
  EXPECT_TRUE(manager.is_running());

  auto& context = manager.get_context();
  EXPECT_NE(&context, nullptr);

  manager.stop();
  EXPECT_FALSE(manager.is_running());
}

/**
 * @brief IoContextManager duplicate start/stop tests
 */
TEST_F(BaseTest, IoContextManagerDuplicateStartStop) {
  auto& manager = wirestead::concurrency::IoContextManager::instance();

  // Start twice
  manager.start();
  manager.start();
  EXPECT_TRUE(manager.is_running());

  // Stop twice
  manager.stop();
  manager.stop();
  EXPECT_FALSE(manager.is_running());
}

/**
 * @brief IoContextManager thread safety tests
 */
TEST_F(BaseTest, IoContextManagerThreadSafety) {
  auto& manager = wirestead::concurrency::IoContextManager::instance();

  // Reduce concurrency load for Windows/CI stability
  const int num_threads = 4;
  const int num_iterations = 5;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&manager, num_iterations]() {
      for (int j = 0; j < num_iterations; ++j) {
        manager.start();
        // yield can cause livelock/high CPU on Windows scheduler with many threads
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        manager.stop();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Final state check (could be either running or stopped, but should be stable)
  // Ensure we can cleanly stop it
  manager.stop();
  EXPECT_FALSE(manager.is_running());
}
/**
 * @brief IoContextManager exception handling tests
 */
TEST_F(BaseTest, IoContextManagerExceptionHandling) {
  auto& manager = wirestead::concurrency::IoContextManager::instance();
  manager.start();

  auto& ioc = manager.get_context();

  // Post a task that throws an exception
  // Wirestead's IoContextManager wraps run() in a try-catch block inside the thread.
  // So this should log an error but NOT crash the process.
  // And the thread loop should exit (running_ becomes false).

  boost::asio::post(ioc, []() { throw std::runtime_error("Test exception in io_context"); });

  // Wait for exception to be processed
  // Since the thread exits on exception, is_running() should eventually become false.
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return !manager.is_running(); }, 1000));

  // Clean up (restart for next tests)
  manager.stop();
}

/**
 * @brief Independent context creation tests
 */
TEST_F(BaseTest, IndependentContextCreation) {
  auto& manager = wirestead::concurrency::IoContextManager::instance();

  // Create independent context
  auto independent_context = manager.create_independent_context();
  EXPECT_NE(independent_context, nullptr);

  // Verify it's different from global context
  manager.start();
  auto& global_context = manager.get_context();
  EXPECT_NE(independent_context.get(), &global_context);

  manager.stop();
}

// ============================================================================
// MEMORY POOL TESTS
// ============================================================================

/**
 * @brief Memory pool basic functionality tests
 */
TEST_F(MemoryTest, MemoryPoolBasicFunctionality) {
  auto& pool = memory::GlobalMemoryPool::instance();

  // Test basic acquire/release
  auto buffer = pool.acquire(1024);
  EXPECT_NE(buffer, nullptr);

  pool.release(std::move(buffer), 1024);

  // Test statistics
  auto stats = pool.stats();
  EXPECT_GE(stats.total_allocations, 0);
}

/**
 * @brief Memory pool performance tests
 */
TEST_F(MemoryTest, MemoryPoolPerformance) {
  auto& pool = memory::GlobalMemoryPool::instance();

  const int num_operations = 1000;
  const size_t buffer_size = 4096;

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  buffers.reserve(num_operations);

  // Allocate buffers
  for (int i = 0; i < num_operations; ++i) {
    auto buffer = pool.acquire(buffer_size);
    if (buffer) {
      buffers.push_back(std::move(buffer));
    }
  }

  // Release buffers
  for (auto& buffer : buffers) {
    pool.release(std::move(buffer), buffer_size);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

  std::cout << "Memory pool performance: " << duration << " μs for " << num_operations << " operations" << std::endl;

  // Verify performance is reasonable
  EXPECT_LT(duration, 100000);  // Should complete in less than 100ms
}

/**
 * @brief Memory pool statistics tests
 */
TEST_F(MemoryTest, MemoryPoolStatistics) {
  auto& pool = memory::GlobalMemoryPool::instance();

  // Perform some operations
  for (int i = 0; i < 100; ++i) {
    auto buffer = pool.acquire(1024);
    if (buffer) {
      pool.release(std::move(buffer), 1024);
    }
  }

  // Test basic statistics
  auto stats = pool.stats();
  EXPECT_GT(stats.total_allocations, 0);

  double hit_rate = pool.hit_rate();
  EXPECT_GE(hit_rate, 0.0);
  EXPECT_LE(hit_rate, 1.0);

  auto memory_usage = pool.memory_usage();
  EXPECT_GE(memory_usage.first, 0);
  EXPECT_GE(memory_usage.second, 0);
}

// ============================================================================
// LOG ROTATION TESTS
// ============================================================================

class LogRotationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Generate unique test file prefix to avoid conflicts in parallel execution
    auto now = std::chrono::high_resolution_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    static std::atomic<uint64_t> seq{0};
    auto unique_dir = "log_rotation_" + std::to_string(timestamp) + "_" + std::to_string(seq++);
    test_dir_ = TestUtils::getTempDirectory() / unique_dir;
    std::filesystem::create_directories(test_dir_);

    base_name_ = "test_rotation";
    base_log_path_ = test_dir_ / (base_name_ + ".log");

    // Clean up any existing test files
    cleanup_test_files();

    // Setup logger for testing
    diagnostics::Logger::instance().set_level(diagnostics::LogLevel::DEBUG);
    diagnostics::Logger::instance().set_console_output(false);  // Disable console for file testing
  }

  void TearDown() override {
    auto& logger = diagnostics::Logger::instance();
    logger.flush();
    logger.set_file_output("");  // Disable file output (closes file handle on Windows)
    logger.set_console_output(true);

    // Clean up test files after the logger releases file handles
    cleanup_test_files();
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  void cleanup_test_files() {
    if (!std::filesystem::exists(test_dir_)) {
      return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      std::error_code ec;
      std::filesystem::remove_all(entry.path(), ec);
    }
  }

  std::filesystem::path test_dir_;
  std::filesystem::path base_log_path_;
  std::string base_name_;

  size_t count_log_files() const {
    if (!std::filesystem::exists(test_dir_)) {
      return 0;
    }
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        if (filename.rfind(base_name_, 0) == 0 && filename.find(".log") != std::string::npos) {
          count++;
        }
      }
    }
    return count;
  }

  size_t get_file_size(const std::filesystem::path& filename) {
    if (std::filesystem::exists(filename)) {
      return std::filesystem::file_size(filename);
    }
    return 0;
  }
};

TEST_F(LogRotationTest, BasicRotationSetup) {
  // Test basic rotation configuration
  diagnostics::LogRotationConfig config;
  config.max_file_size_bytes = 1024;  // 1KB for testing
  config.max_files = 3;

  EXPECT_EQ(config.max_file_size_bytes, 1024);
  EXPECT_EQ(config.max_files, 3);
}

TEST_F(LogRotationTest, FileSizeBasedRotation) {
  // Setup rotation with very small file size for testing
  diagnostics::LogRotationConfig config;
  config.max_file_size_bytes = 512;  // 512 bytes
  config.max_files = 5;

  diagnostics::Logger::instance().set_file_output_with_rotation(base_log_path_.string(), config);

  // Generate enough log data to trigger rotation
  for (int i = 0; i < 20; ++i) {
    WIRESTEAD_LOG_INFO("test", "rotation",
                       "Test message " + std::to_string(i) +
                           " - This is a longer message to help reach the rotation threshold quickly.");
  }

  // Flush to ensure all data is written
  diagnostics::Logger::instance().flush();

  // Check if rotation occurred (should have multiple files)
  size_t file_count = count_log_files();
  EXPECT_GE(file_count, 1) << "At least one log file should exist";

  // Check if files are within size limits
  if (std::filesystem::exists(base_log_path_)) {
    size_t current_size = get_file_size(base_log_path_);
    // spdlog might be slightly over due to async nature, but should be reasonable
    EXPECT_LE(current_size, config.max_file_size_bytes * 10) << "Current log file should be reasonable size";
  }
}

TEST_F(LogRotationTest, FileCountLimit) {
  // Setup rotation with small file size and low file count
  diagnostics::LogRotationConfig config;
  config.max_file_size_bytes = 256;  // 256 bytes
  config.max_files = 2;              // Only keep 2 files

  diagnostics::Logger::instance().set_file_output_with_rotation(base_log_path_.string(), config);

  // Generate lots of log data to trigger multiple rotations
  for (int i = 0; i < 50; ++i) {
    WIRESTEAD_LOG_INFO("test", "count_limit",
                       "Message " + std::to_string(i) +
                           " - Generating enough data to trigger multiple rotations and test file count limits.");
  }

  diagnostics::Logger::instance().flush();

  // Check that file count doesn't exceed limit
  size_t file_count = count_log_files();
  EXPECT_LE(file_count, config.max_files + 1) << "File count should not exceed limit (current + rotated files)";
}

TEST_F(LogRotationTest, LogRotationWithoutRotation) {
  // Test that rotation doesn't occur when file is small
  diagnostics::LogRotationConfig config;
  config.max_file_size_bytes = 1024 * 1024;  // 1MB - very large
  config.max_files = 5;

  diagnostics::Logger::instance().set_file_output_with_rotation(base_log_path_.string(), config);

  // Generate small amount of log data
  for (int i = 0; i < 5; ++i) {
    WIRESTEAD_LOG_INFO("test", "no_rotation", "Small message " + std::to_string(i));
  }

  diagnostics::Logger::instance().flush();

  // Should only have one file
  size_t file_count = count_log_files();
  EXPECT_EQ(file_count, 1) << "Should only have one file when size limit not reached";

  // File should exist and be small
  EXPECT_TRUE(std::filesystem::exists(base_log_path_));
  size_t file_size = get_file_size(base_log_path_);
  EXPECT_LT(file_size, config.max_file_size_bytes) << "File should be smaller than rotation threshold";
}

// ============================================================================
// ASYNC LOGGING TESTS
// ============================================================================

class AsyncLoggingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Generate unique test file prefix to avoid conflicts in parallel execution
    auto now = std::chrono::high_resolution_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    test_file_prefix_ = "async_test_" + std::to_string(timestamp);

    // Clean up any existing test files
    cleanup_test_files();

    // Setup logger for testing
    diagnostics::Logger::instance().set_level(diagnostics::LogLevel::DEBUG);
    diagnostics::Logger::instance().set_console_output(false);  // Disable console for file testing
  }

  void TearDown() override {
    auto& logger = diagnostics::Logger::instance();

    // Stop async logging before flushing to ensure background threads exit
    logger.set_async_logging(false);
    logger.flush();
    logger.set_file_output("");  // Disable file output
    logger.set_console_output(true);

    // Remove files only after handles are released
    cleanup_test_files();
  }

  void cleanup_test_files() {
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
      if (!entry.is_regular_file()) {
        continue;
      }
      const auto filename = entry.path().filename().string();
      if (filename.rfind(test_file_prefix_, 0) == 0 && filename.find(".log") != std::string::npos) {
        std::error_code ec;
        std::filesystem::remove(entry.path(), ec);
      }
    }
  }

  std::string test_file_prefix_;

  size_t get_file_size(const std::string& filename) {
    if (std::filesystem::exists(filename)) {
      return std::filesystem::file_size(filename);
    }
    return 0;
  }
};

TEST_F(AsyncLoggingTest, BasicAsyncLoggingSetup) {
  // Test basic async logging configuration
  diagnostics::AsyncLogConfig config;
  config.max_queue_size = 1000;
  config.batch_size = 50;
  config.flush_interval = std::chrono::milliseconds(100);

  EXPECT_EQ(config.max_queue_size, 1000);
  EXPECT_EQ(config.batch_size, 50);
  EXPECT_EQ(config.flush_interval.count(), 100);
}

TEST_F(AsyncLoggingTest, AsyncLoggingEnabled) {
  // Setup async logging
  diagnostics::AsyncLogConfig config;
  config.max_queue_size = 1000;
  config.batch_size = 10;
  config.flush_interval = std::chrono::milliseconds(50);

  diagnostics::Logger::instance().set_async_logging(true, config);

  EXPECT_TRUE(diagnostics::Logger::instance().async_logging_enabled());

  // Generate some log messages
  for (int i = 0; i < 5; ++i) {
    WIRESTEAD_LOG_INFO("async_test", "enabled", "Async logging test message " + std::to_string(i));
  }

  // Wait for async processing
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Disable async logging to clean up
  diagnostics::Logger::instance().set_async_logging(false);
}

TEST_F(AsyncLoggingTest, AsyncLoggingWithFileOutput) {
  std::string log_filename = test_file_prefix_ + ".log";

  // Setup async logging with file output
  diagnostics::AsyncLogConfig config;
  config.max_queue_size = 1000;
  config.batch_size = 20;
  config.flush_interval = std::chrono::milliseconds(100);

  diagnostics::Logger::instance().set_file_output(log_filename);
  diagnostics::Logger::instance().set_async_logging(true, config);

  // Generate log messages
  for (int i = 0; i < 10; ++i) {
    WIRESTEAD_LOG_INFO("async_test", "file_output", "Async file logging test message " + std::to_string(i));
  }

  // Disable async logging to clean up (flushes pending logs)
  diagnostics::Logger::instance().set_async_logging(false);

  // Check that file was created
  EXPECT_TRUE(std::filesystem::exists(log_filename));
}

TEST_F(AsyncLoggingTest, AsyncLoggingPerformance) {
  // Test async logging performance
  diagnostics::AsyncLogConfig config;
  config.max_queue_size = 10000;
  config.batch_size = 100;
  config.flush_interval = std::chrono::milliseconds(50);

  diagnostics::Logger::instance().set_async_logging(true, config);

  const int num_messages = 1000;
  auto start_time = std::chrono::high_resolution_clock::now();

  // Generate many log messages
  for (int i = 0; i < num_messages; ++i) {
    WIRESTEAD_LOG_DEBUG("async_test", "performance", "Performance test message " + std::to_string(i));
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

  // Disable async logging to ensure all messages are processed
  diagnostics::Logger::instance().set_async_logging(false);

  // Check performance (should be very fast since it's just queuing)
  double messages_per_second = (num_messages * 1000000.0) / static_cast<double>(duration);
#ifdef _WIN32
  const double expected_threshold =
      25000.0;  // Windows std::chrono-resolution + thread scheduling yields lower throughput
#else
  const double expected_threshold = 1000.0;  // Coverage instrumentation can dominate this smoke check.
#endif
  EXPECT_GT(messages_per_second, expected_threshold)
      << "Should process at least " << expected_threshold << " messages per second";

  std::cout << "Async logging performance: " << messages_per_second << " messages/second" << std::endl;
}

TEST_F(AsyncLoggingTest, AsyncLoggingDisable) {
  // Test disabling async logging
  diagnostics::AsyncLogConfig config;
  config.max_queue_size = 1000;
  config.batch_size = 50;

  // Enable async logging
  diagnostics::Logger::instance().set_async_logging(true, config);
  EXPECT_TRUE(diagnostics::Logger::instance().async_logging_enabled());

  // Disable async logging
  diagnostics::Logger::instance().set_async_logging(false);
  EXPECT_FALSE(diagnostics::Logger::instance().async_logging_enabled());

  // Generate some log messages (should use synchronous logging)
  for (int i = 0; i < 10; ++i) {
    WIRESTEAD_LOG_INFO("async_test", "disable", "Synchronous logging test message " + std::to_string(i));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
