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

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <vector>

#include "wirestead/base/visibility.hpp"

#ifdef DEBUG
#undef DEBUG
#endif
#ifdef INFO
#undef INFO
#endif
#ifdef WARNING
#undef WARNING
#endif
#ifdef ERROR
#undef ERROR
#endif
#ifdef CRITICAL
#undef CRITICAL
#endif
#ifdef CALLBACK
#undef CALLBACK
#endif

namespace wirestead {
namespace diagnostics {

/**
 * @brief Log severity levels
 */
enum class LogLevel { DEBUG = 0, INFO = 1, WARNING = 2, ERROR = 3, CRITICAL = 4 };

/**
 * @brief Log output destinations
 */
enum class LogOutput { CONSOLE = 0x01, FILE = 0x02, CALLBACK = 0x04 };

/**
 * @brief Log rotation configuration
 */
struct LogRotationConfig {
  size_t max_file_size_bytes = 10 * 1024 * 1024;    // 10MB default
  size_t max_files = 10;                            // Keep 10 files max
  bool enable_compression = false;                  // Reserved for future use
  std::string file_pattern = "{name}.{index}.log";  // Reserved for future use

  LogRotationConfig() = default;

  LogRotationConfig(size_t max_size, size_t max_count) : max_file_size_bytes(max_size), max_files(max_count) {}
};

/**
 * @brief Async logging configuration
 */
struct AsyncLogConfig {
  size_t max_queue_size = 10000;                     // Maximum queue size
  size_t batch_size = 100;                           // Reserved; spdlog manages batching internally
  std::chrono::milliseconds flush_interval{100};     // Flush interval
  std::chrono::milliseconds shutdown_timeout{5000};  // Shutdown timeout
  bool enable_backpressure = true;                   // Enable backpressure handling (maps to spdlog block policy)
  bool enable_batch_processing = true;               // Reserved; spdlog manages batching internally

  AsyncLogConfig() = default;

  AsyncLogConfig(size_t max_q, size_t batch, std::chrono::milliseconds interval)
      : max_queue_size(max_q), batch_size(batch), flush_interval(interval) {}
};

/**
 * @brief Centralized logging system with async support
 *
 * Provides thread-safe, configurable logging with multiple output destinations,
 * async processing, batch operations, and performance optimizations for production use.
 */
class WIRESTEAD_API Logger {
 public:
  using LogCallback = std::function<void(LogLevel level, const std::string& formatted_message)>;

  /**
   * @brief Get singleton instance
   */
  static Logger& instance();
  [[deprecated("Use Logger::instance() instead")]]
  static Logger& default_logger();

  Logger();
  ~Logger();

  // Move semantics
  Logger(Logger&&) noexcept;
  Logger& operator=(Logger&&) noexcept;

  // Non-copyable
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  /**
   * @brief Set minimum log level
   * @param level Messages below this level will be ignored
   */
  void set_level(LogLevel level);

  /**
   * @brief Get current log level
   */
  LogLevel level() const;

  /**
   * @brief Check whether a message at the given level would be logged.
   */
  bool should_log(LogLevel level) const;

  /**
   * @brief Enable/disable console output
   * @param enable True to enable console output
   */
  void set_console_output(bool enable);

  /**
   * @brief Set file output
   * @param filename Log file path (empty string to disable file output)
   */
  void set_file_output(const std::string& filename);

  /**
   * @brief Set file output and report whether it succeeded.
   * @param filename Log file path (empty string to disable file output)
   * @return True when the requested output state was applied
   */
  bool try_set_file_output(const std::string& filename);

  /**
   * @brief Set file output with rotation
   * @param filename Log file path
   * @param config Rotation configuration
   */
  void set_file_output_with_rotation(const std::string& filename,
                                     const LogRotationConfig& config = LogRotationConfig{});

  /**
   * @brief Set file output with rotation and report whether it succeeded.
   * @param filename Log file path
   * @param config Rotation configuration
   * @return True when the requested output state was applied
   */
  bool try_set_file_output_with_rotation(const std::string& filename,
                                         const LogRotationConfig& config = LogRotationConfig{});

  /**
   * @brief Enable/disable async logging
   * @param enable True to enable async logging
   * @param config Async logging configuration
   */
  void set_async_logging(bool enable, const AsyncLogConfig& config = AsyncLogConfig{});

  /**
   * @brief Check if async logging is enabled
   */
  bool async_logging_enabled() const;

  /**
   * @brief Re-apply supported logger settings from environment variables.
   *
   * Currently supports WIRESTEAD_LOG_LEVEL values DEBUG, INFO, WARNING, ERROR, CRITICAL, and OFF.
   */
  void reload_from_environment();

  /**
   * @brief Set log callback
   * @param callback Function to call for each log message
   */
  void set_callback(LogCallback callback);

  /**
   * @brief Set output destinations
   * @param outputs Bitwise OR of LogOutput flags
   */
  void set_outputs(int outputs);

  /**
   * @brief Enable/disable logging
   * @param enabled True to enable logging
   */
  void set_enabled(bool enabled);

  /**
   * @brief Check if logging is enabled
   */
  bool enabled() const;

  /**
   * @brief Return true when at least one output destination is active.
   */
  bool has_outputs() const;

  /**
   * @brief Last logger configuration error, empty when the last operation succeeded.
   */
  std::string last_error() const;

  /**
   * @brief Set log format
   * @param format Format string with placeholders: {timestamp}, {level}, {component}, {operation}, {source}, {file},
   * {line}, {function}, {message}
   */
  void set_format(const std::string& format);

  /**
   * @brief Flush all outputs
   */
  void flush();

  // Main logging functions
  void log(LogLevel level, std::string_view component, std::string_view operation, std::string_view message,
           const std::source_location& loc = std::source_location::current());

  void debug(std::string_view component, std::string_view operation, std::string_view message,
             const std::source_location& loc = std::source_location::current());
  void info(std::string_view component, std::string_view operation, std::string_view message,
            const std::source_location& loc = std::source_location::current());
  void warning(std::string_view component, std::string_view operation, std::string_view message,
               const std::source_location& loc = std::source_location::current());
  void error(std::string_view component, std::string_view operation, std::string_view message,
             const std::source_location& loc = std::source_location::current());
  void critical(std::string_view component, std::string_view operation, std::string_view message,
                const std::source_location& loc = std::source_location::current());

 private:
  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief Convenience macros for logging
 */
#define WIRESTEAD_LOG(level, component, operation, message)                                               \
  do {                                                                                                    \
    const auto wirestead_log_level = (level);                                                             \
    if (wirestead::diagnostics::Logger::instance().should_log(wirestead_log_level)) {                     \
      wirestead::diagnostics::Logger::instance().log(wirestead_log_level, component, operation, message); \
    }                                                                                                     \
  } while (0)

#define WIRESTEAD_LOG_DEBUG(component, operation, message) \
  WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::DEBUG, component, operation, message)

#define WIRESTEAD_LOG_INFO(component, operation, message) \
  WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::INFO, component, operation, message)

#define WIRESTEAD_LOG_WARNING(component, operation, message) \
  WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::WARNING, component, operation, message)

#define WIRESTEAD_LOG_ERROR(component, operation, message) \
  WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::ERROR, component, operation, message)

#define WIRESTEAD_LOG_CRITICAL(component, operation, message) \
  WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::CRITICAL, component, operation, message)

/**
 * @brief Performance logging macros for expensive operations.
 *
 * Usage: WIRESTEAD_LOG_PERF_START and WIRESTEAD_LOG_PERF_END must use the same
 * `component` and `operation` tokens, and each `operation` token must be
 * unique within its enclosing scope (the token becomes part of a variable name).
 */
#define WIRESTEAD_LOG_PERF_START(component, operation)                                                 \
  auto _perf_start_##operation =                                                                       \
      (wirestead::diagnostics::Logger::instance().should_log(wirestead::diagnostics::LogLevel::DEBUG)) \
          ? std::chrono::high_resolution_clock::now()                                                  \
          : std::chrono::high_resolution_clock::time_point()

#define WIRESTEAD_LOG_PERF_END(component, operation)                                                      \
  do {                                                                                                    \
    if (wirestead::diagnostics::Logger::instance().should_log(wirestead::diagnostics::LogLevel::DEBUG)) { \
      auto _perf_end_##operation = std::chrono::high_resolution_clock::now();                             \
      using _us_t = std::chrono::microseconds;                                                            \
      auto _diff_##operation = _perf_end_##operation - _perf_start_##operation;                           \
      auto _perf_duration_##operation = std::chrono::duration_cast<_us_t>(_diff_##operation).count();     \
      WIRESTEAD_LOG(wirestead::diagnostics::LogLevel::DEBUG, component, operation,                        \
                    "Duration: " + std::to_string(_perf_duration_##operation) + " μs");                   \
    }                                                                                                     \
  } while (0)

#define UNILINK_LOG(level, component, operation, message) WIRESTEAD_LOG(level, component, operation, message)

#define UNILINK_LOG_DEBUG(component, operation, message) WIRESTEAD_LOG_DEBUG(component, operation, message)

#define UNILINK_LOG_INFO(component, operation, message) WIRESTEAD_LOG_INFO(component, operation, message)

#define UNILINK_LOG_WARNING(component, operation, message) WIRESTEAD_LOG_WARNING(component, operation, message)

#define UNILINK_LOG_ERROR(component, operation, message) WIRESTEAD_LOG_ERROR(component, operation, message)

#define UNILINK_LOG_CRITICAL(component, operation, message) WIRESTEAD_LOG_CRITICAL(component, operation, message)

#define UNILINK_LOG_PERF_START(component, operation) WIRESTEAD_LOG_PERF_START(component, operation)

#define UNILINK_LOG_PERF_END(component, operation) WIRESTEAD_LOG_PERF_END(component, operation)

}  // namespace diagnostics
}  // namespace wirestead
