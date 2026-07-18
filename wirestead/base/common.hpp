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

#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Include logger for log_message function
#include "wirestead/base/constants.hpp"
#include "wirestead/base/platform.hpp"
#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {

/**
 * @brief Strong type alias for client identifiers
 */
using ClientId = size_t;

namespace base {

enum class LinkState { Idle, Connecting, Listening, Connected, Closed, Error };

// LCOV_EXCL_START
inline const char* to_cstr(LinkState s) {
  switch (s) {
    case LinkState::Idle:
      return "Idle";
    case LinkState::Connecting:
      return "Connecting";
    case LinkState::Listening:
      return "Listening";
    case LinkState::Connected:
      return "Connected";
    case LinkState::Closed:
      return "Closed";
    case LinkState::Error:
      return "Error";
  }
  return "?";
}

inline std::string ts_now() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const auto tt = system_clock::to_time_t(now);
  std::tm tm{};
#if defined(WIRESTEAD_PLATFORM_WINDOWS)
  localtime_s(&tm, &tt);
#else
  localtime_r(&tt, &tm);
#endif
  const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  std::ostringstream oss;
  oss << std::put_time(&tm, "%F %T") << '.' << std::setw(3) << std::setfill('0') << ms.count();
  return oss.str();  // e.g., 2025-09-15 13:07:42.123
}

inline void log_message(std::string_view tag, std::string_view direction, std::string_view message) {
  // Remove trailing newline from message if it exists
  std::string_view clean_message = message;
  if (!clean_message.empty() && clean_message.back() == '\n') {
    clean_message.remove_suffix(1);
  }

  // Use new logging system
  diagnostics::Logger::instance().info(tag, direction, clean_message);
}

// Platform-specific feature availability
inline bool is_advanced_logging_available() { return PlatformInfo::is_advanced_logging_available(); }

inline bool is_performance_monitoring_available() { return PlatformInfo::is_performance_monitoring_available(); }

inline bool is_latest_optimizations_available() { return PlatformInfo::is_latest_optimizations_available(); }

inline bool is_experimental_features_available() { return PlatformInfo::is_experimental_features_available(); }

inline std::string get_platform_warning() { return PlatformInfo::get_support_warning(); }
// LCOV_EXCL_STOP

// Safe memory operations
namespace safe_memory {
/**
 * @brief Safely copy memory with bounds checking
 * @param dest Destination buffer
 * @param src Source buffer
 * @param size Number of bytes to copy
 * @throws std::invalid_argument if parameters are invalid
 */
inline void safe_memcpy(uint8_t* dest, const uint8_t* src, size_t size) {
  if (!dest) {
    throw std::invalid_argument("Destination pointer is null");
  }
  if (!src) {
    throw std::invalid_argument("Source pointer is null");
  }
  if (size == 0) {
    return;  // Nothing to copy
  }
  if (size > constants::MAX_BUFFER_SIZE) {
    throw std::invalid_argument("Copy size too large (max MAX_BUFFER_SIZE)");
  }

  std::memcpy(dest, src, size);
}

/**
 * @brief Safely copy memory with bounds checking (overloaded for char*)
 * @param dest Destination buffer
 * @param src Source buffer
 * @param size Number of bytes to copy
 * @throws std::invalid_argument if parameters are invalid
 */
inline void safe_memcpy(char* dest, const char* src, size_t size) {
  safe_memcpy(reinterpret_cast<uint8_t*>(dest), reinterpret_cast<const uint8_t*>(src), size);
}
}  // namespace safe_memory

// Safe type conversion utilities
namespace safe_convert {
/**
 * @brief Safely convert uint8_t* to const char* for string operations
 * @param data Pointer to uint8_t data
 * @param size Size of the data
 * @return std::string constructed from the data
 */
inline std::string uint8_to_string(const uint8_t* data, size_t size) {
  if (!data || size == 0) {
    return std::string{};
  }
  return std::string(data, data + size);
}

/**
 * @brief Safely convert const char* to const uint8_t* for binary operations
 * @param data Pointer to char data
 * @param size Size of the data
 * @return std::vector<uint8_t> containing the data
 */
inline std::vector<uint8_t> string_to_uint8(const char* data, size_t size) {
  if (!data || size == 0) {
    return std::vector<uint8_t>{};
  }
  return std::vector<uint8_t>(data, data + size);
}

/**
 * @brief Safely convert std::string to std::vector<uint8_t>
 * @param str The string to convert
 * @return std::vector<uint8_t> containing the string data
 */
inline std::vector<uint8_t> string_to_uint8(std::string_view str) {
  return std::vector<uint8_t>(str.begin(), str.end());
}

/**
 * @brief Safely obtain a view of std::string as byte array without allocation
 * @param str The string to convert
 * @return std::pair<const uint8_t*, size_t> containing pointer and size
 */
inline std::pair<const uint8_t*, size_t> string_to_bytes(std::string_view str) {
  return {reinterpret_cast<const uint8_t*>(str.data()), str.size()};
}
}  // namespace safe_convert

// Removed unused feed_lines function to eliminate -Wunused-function warning
}  // namespace base

}  // namespace wirestead
