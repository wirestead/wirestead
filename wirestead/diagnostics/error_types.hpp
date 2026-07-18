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

#include <algorithm>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

namespace wirestead {
namespace diagnostics {

#ifdef ERROR
#undef ERROR
#endif
#ifdef WARNING
#undef WARNING
#endif

/**
 * @brief Error severity levels
 */
enum class ErrorLevel {
  INFO = 0,     // Informational message (normal operation info)
  WARNING = 1,  // Warning (recoverable issue)
  ERROR = 2,    // Error (retry required)
  CRITICAL = 3  // Critical error (unrecoverable)
};

/**
 * @brief Error categories for classification
 */
enum class ErrorCategory {
  CONNECTION = 0,     // Connection related (TCP, Serial connect/disconnect)
  COMMUNICATION = 1,  // Communication related (data send/receive)
  CONFIGURATION = 2,  // Configuration related (invalid config values)
  MEMORY = 3,         // Memory related (allocation/deallocation errors)
  SYSTEM = 4,         // System related (OS level errors)
  UNKNOWN = 5         // Unknown error
};

/**
 * @brief Comprehensive error information structure
 */
struct ErrorInfo {
  ErrorLevel level;                                 // Error severity
  ErrorCategory category;                           // Error category
  std::string component;                            // Component name (serial, tcp_server, tcp_client)
  std::string operation;                            // Operation being performed (read, write, connect, bind)
  std::string message;                              // Error message
  boost::system::error_code boost_error;            // Boost error code
  std::chrono::system_clock::time_point timestamp;  // Error occurrence time
  bool retryable;                                   // Retry possibility
  uint32_t retry_count;                             // Current retry count
  std::string context;                              // Additional context information

  /**
   * @brief Constructor for basic error info
   */
  ErrorInfo(ErrorLevel l, ErrorCategory c, std::string_view comp, std::string_view op, std::string_view msg)
      : level(l),
        category(c),
        component(comp),
        operation(op),
        message(msg),
        timestamp(std::chrono::system_clock::now()),
        retryable(false),
        retry_count(0) {}

  /**
   * @brief Constructor with Boost error code
   */
  ErrorInfo(ErrorLevel l, ErrorCategory c, std::string_view comp, std::string_view op, std::string_view msg,
            const boost::system::error_code& ec, bool retry = false)
      : level(l),
        category(c),
        component(comp),
        operation(op),
        message(msg),
        boost_error(ec),
        timestamp(std::chrono::system_clock::now()),
        retryable(retry),
        retry_count(0) {}

  /**
   * @brief Get formatted timestamp string
   */
  std::string timestamp_string() const {
    auto time_t = std::chrono::system_clock::to_time_t(timestamp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()) % 1000;

    std::tm tm_buf{};
#if defined(_MSC_VER)
    localtime_s(&tm_buf, &time_t);
#else
    localtime_r(&time_t, &tm_buf);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
  }

  /**
   * @brief Get error level as string
   */
  std::string level_string() const {
    switch (level) {
      case ErrorLevel::INFO:
        return "INFO";
      case ErrorLevel::WARNING:
        return "WARNING";
      case ErrorLevel::ERROR:
        return "ERROR";
      case ErrorLevel::CRITICAL:
        return "CRITICAL";
    }
    return "UNKNOWN";
  }

  /**
   * @brief Get error category as string
   */
  std::string category_string() const {
    switch (category) {
      case ErrorCategory::CONNECTION:
        return "CONNECTION";
      case ErrorCategory::COMMUNICATION:
        return "COMMUNICATION";
      case ErrorCategory::CONFIGURATION:
        return "CONFIGURATION";
      case ErrorCategory::MEMORY:
        return "MEMORY";
      case ErrorCategory::SYSTEM:
        return "SYSTEM";
      case ErrorCategory::UNKNOWN:
        return "UNKNOWN";
    }
    return "UNKNOWN";
  }

  /**
   * @brief Get formatted error summary
   */
  std::string summary() const {
    std::ostringstream oss;
    oss << "[" << level_string() << "] " << "[" << component << "] " << "[" << operation << "] " << message;

    if (boost_error) {
      oss << " (boost: " << boost_error.message() << ", code: " << boost_error.value() << ")";
    }

    if (retryable) {
      oss << " [RETRYABLE, count: " << retry_count << "]";
    }

    return oss.str();
  }
};

/**
 * @brief Error statistics for monitoring
 */
struct ErrorStats {
  size_t total_errors = 0;
  size_t errors_by_level[4] = {0, 0, 0, 0};           // INFO, WARNING, ERROR, CRITICAL
  size_t errors_by_category[6] = {0, 0, 0, 0, 0, 0};  // CONNECTION, COMMUNICATION, etc.
  size_t retryable_errors = 0;
  size_t successful_retries = 0;
  size_t failed_retries = 0;

  std::chrono::system_clock::time_point first_error;
  std::chrono::system_clock::time_point last_error;

  /**
   * @brief Reset all statistics
   */
  void reset() {
    total_errors = 0;
    std::fill(std::begin(errors_by_level), std::end(errors_by_level), 0);
    std::fill(std::begin(errors_by_category), std::end(errors_by_category), 0);
    retryable_errors = 0;
    successful_retries = 0;
    failed_retries = 0;
    first_error = std::chrono::system_clock::time_point{};
    last_error = std::chrono::system_clock::time_point{};
  }

  /**
   * @brief Get error rate (errors per minute)
   */
  double error_rate() const {
    if (total_errors == 0) return 0.0;

    auto duration = std::chrono::duration_cast<std::chrono::minutes>(last_error - first_error).count();
    return duration > 0 ? static_cast<double>(total_errors) / static_cast<double>(duration) : 0.0;
  }
};

}  // namespace diagnostics
}  // namespace wirestead
