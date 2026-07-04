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

#pragma once

#include <gtest/gtest.h>

#include "unilink/base/platform.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "unilink/memory/memory_pool.hpp"

namespace unilink {
namespace test {

/**
 * @brief Common test utilities for unilink tests
 */
class TestUtils {
 public:
  /**
   * @brief Get a unique test port number
   * @return uint16_t Unique port number
   */
  static uint16_t getTestPort() { return getAvailableTestPort(); }

  /**
   * @brief Get a guaranteed available test port
   * @return uint16_t Available port number
   */
  static uint16_t getAvailableTestPort() {
    static std::atomic<uint16_t> port_counter{0};

    // Initialize with random offset on first use to avoid collisions between test processes
    uint16_t current = port_counter.load();
    if (current == 0) {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<uint16_t> dis(30000, 40000);
      uint16_t start_port = dis(gen);
      port_counter.compare_exchange_strong(current, start_port);
    }

    for (int attempt = 0; attempt < 1024; ++attempt) {
      uint16_t candidate = port_counter.fetch_add(1);
      // Wrap if we go too high
      if (candidate > 60000) {
        port_counter.store(30000);
        candidate = port_counter.fetch_add(1);
      }
      if (isPortAvailable(candidate)) {
        return candidate;
      }
    }
    throw std::runtime_error("Unable to find available test port after many attempts");
  }

  /**
   * @brief Check if a port is available for binding
   * @param port Port number to check
   * @return true if port is available, false otherwise
   */
  static bool isPortAvailable(uint16_t port) {
#ifdef _WIN32
    ensure_winsock_initialized();
    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) return false;

    BOOL reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    bool available = bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    closesocket(sock);
    return available;
#else
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    // Set SO_REUSEADDR to allow binding to recently used ports
    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    bool available = bind(sock, (sockaddr*)&addr, sizeof(addr)) == 0;
    close(sock);
    return available;
#endif
  }

  /**
   * @brief Wait for a condition with timeout
   * @param condition Function that returns true when condition is met
   * @param timeout_ms Timeout in milliseconds
   * @return true if condition was met, false if timeout
   */
  template <typename Condition>
  static bool waitForCondition(Condition&& condition, int timeout_ms = 5000) {
    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(timeout_ms);

    // Use shorter polling interval for better responsiveness
    while (std::chrono::steady_clock::now() - start < timeout) {
      if (condition()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
  }

  /**
   * @brief Wait for a condition with retry logic
   * @param condition Function that returns true when condition is met
   * @param timeout_ms Timeout in milliseconds per attempt
   * @param retry_count Number of retry attempts
   * @return true if condition was met, false if all retries failed
   */
  template <typename Condition>
  static bool waitForConditionWithRetry(Condition&& condition, int timeout_ms = 2000, int retry_count = 3) {
    for (int i = 0; i < retry_count; ++i) {
      if (waitForCondition(condition, timeout_ms)) {
        return true;
      }
      // Brief pause between retries
      if (i < retry_count - 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    return false;
  }

  /**
   * @brief Wait for a specific duration
   * @param ms Duration in milliseconds
   */
  static void waitFor(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

  /**
   * @brief Generate test data of specified size
   * @param size Size of test data
   * @return std::string Test data
   */
  static std::string generateTestData(size_t size) {
    std::string data;
    data.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      data += static_cast<char>('A' + (i % 26));
    }
    return data;
  }

  /**
   * @brief Returns a writable temporary directory for tests
   * @return std::filesystem::path Temporary directory path
   */
  static std::filesystem::path getTempDirectory() {
    auto base = std::filesystem::temp_directory_path() / "unilink_tests";
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    return base;
  }

  /**
   * @brief Builds a temp file path under the shared test temp directory
   * @param filename File name (without directory)
   * @return std::filesystem::path Absolute path to temp file
   */
  static std::filesystem::path makeTempFilePath(const std::string& filename) { return getTempDirectory() / filename; }

  /**
   * @brief Builds a temp file path with a process-local uniqueness suffix
   * @param prefix File name prefix
   * @param suffix Optional suffix including extension
   * @return std::filesystem::path Absolute path to a unique temp file
   */
  static std::filesystem::path makeUniqueTempFilePath(const std::string& prefix, const std::string& suffix = "") {
    static std::atomic<uint64_t> unique_counter{0};
    std::random_device rd;
    auto tick = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    auto counter = unique_counter.fetch_add(1, std::memory_order_relaxed);
    auto random = static_cast<uint64_t>(rd());
    return makeTempFilePath(prefix + "_" + std::to_string(tick) + "_" + std::to_string(counter) + "_" +
                            std::to_string(random) + suffix);
  }

  /**
   * @brief Builds a short Unix domain socket path suitable for platform-specific path limits
   * @param prefix File name prefix
   * @return std::filesystem::path Absolute path to a unique short socket path
   */
  static std::filesystem::path makeUniqueUdsSocketPath(const std::string& prefix = "ul") {
    static std::atomic<uint64_t> unique_counter{0};
    std::random_device rd;
    auto counter = unique_counter.fetch_add(1, std::memory_order_relaxed);
    auto random = static_cast<uint64_t>(rd() & 0xFFFF);
    std::filesystem::path short_dir;
#ifdef _WIN32
    short_dir = std::filesystem::temp_directory_path();
#else
    short_dir = std::filesystem::path("/tmp");
#endif
    return short_dir / (prefix + "_" + std::to_string(counter) + "_" + std::to_string(random) + ".sock");
  }

  /**
   * @brief Removes a file if it exists, ignoring errors
   * @param path File path to remove
   */
  static void removeFileIfExists(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }

#ifdef _WIN32
 private:
  static void ensure_winsock_initialized();
#endif
};

#ifdef _WIN32
inline void TestUtils::ensure_winsock_initialized() {
  static std::once_flag winsock_once;
  static WSADATA wsa_data;
  std::call_once(winsock_once, []() {
    int result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (result != 0) {
      throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
    }
  });
}
#endif

/**
 * @brief Base test class with common setup/teardown
 */
class BaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Common setup for all tests
    test_start_time_ = std::chrono::steady_clock::now();
  }

  void TearDown() override {
    // Common teardown for all tests
    auto test_duration = std::chrono::steady_clock::now() - test_start_time_;
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(test_duration).count();

    // Log test duration if it's unusually long
    if (duration_ms > 5000) {
      std::cout << "Warning: Test took " << duration_ms << "ms to complete" << std::endl;
    }
  }

  std::chrono::steady_clock::time_point test_start_time_;
};

/**
 * @brief Test class for network-related tests
 */
class NetworkTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    test_port_ = TestUtils::getTestPort();
  }

  void TearDown() override {
    // Clean up any network resources
    BaseTest::TearDown();
  }

  uint16_t test_port_;
};

/**
 * @brief Test class for performance tests
 */
class PerformanceTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    performance_start_ = std::chrono::high_resolution_clock::now();
  }

  void TearDown() override {
    auto performance_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(performance_end - performance_start_).count();

    std::cout << "Performance test completed in " << duration << " μs" << std::endl;
    BaseTest::TearDown();
  }

  std::chrono::high_resolution_clock::time_point performance_start_;
};

/**
 * @brief Test class for memory-related tests
 */
class MemoryTest : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetUp(); }

  void TearDown() override { BaseTest::TearDown(); }
};

/**
 * @brief Test class for integration tests
 */
class IntegrationTest : public NetworkTest {
 protected:
  void SetUp() override {
    NetworkTest::SetUp();
    // Additional setup for integration tests
  }

  void TearDown() override {
    // Clean up integration test resources
    // Removed fixed 1-second sleep to improve test performance.
    // Individual tests should ensure their servers/clients are stopped properly.
    NetworkTest::TearDown();
  }
};

}  // namespace test
}  // namespace unilink
