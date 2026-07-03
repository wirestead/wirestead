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
#include <cctype>
#include <cstdint>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include "unilink/base/constants.hpp"
#include "unilink/base/visibility.hpp"
#include "unilink/diagnostics/exceptions.hpp"

namespace unilink {
namespace util {

/**
 * @brief Input validation utility class
 *
 * Provides comprehensive input validation for all unilink components.
 * Throws ValidationException for invalid inputs with detailed error messages.
 */
class UNILINK_API InputValidator {
 public:
  // Network validation
  static void validate_host(const std::string& host);
  static void validate_port(uint16_t port);
  static void validate_ipv4_address(const std::string& address);
  static void validate_ipv6_address(const std::string& address);
  static void validate_uds_path(const std::string& path);

  // Serial validation
  static void validate_device_path(const std::string& device);
  static void validate_baud_rate(uint32_t baud_rate);
  static void validate_data_bits(uint8_t data_bits);
  static void validate_stop_bits(uint8_t stop_bits);
  static void validate_parity(const std::string& parity);

  // Memory validation
  static void validate_buffer_size(size_t size);
  static void validate_memory_alignment(const void* ptr, size_t alignment);

  // Timeout and interval validation
  static void validate_timeout(unsigned timeout_ms);
  static void validate_retry_interval(unsigned interval_ms);
  static void validate_retry_count(int retry_count);

  // String validation
  static void validate_non_empty_string(const std::string& str, const std::string& field_name);
  static void validate_string_length(const std::string& str, size_t max_length, const std::string& field_name);

  // Numeric validation
  static void validate_positive_number(int64_t value, const std::string& field_name);
  static void validate_range(int64_t value, int64_t min, int64_t max, const std::string& field_name);
  static void validate_range(size_t value, size_t min, size_t max, const std::string& field_name);

  // Validation helpers (exposed for public use)
  static bool is_valid_host(const std::string& host);
  static bool is_valid_ipv4(std::string_view address);
  static bool is_valid_ipv6(const std::string& address);
  static bool is_valid_hostname(std::string_view hostname);
  static bool is_valid_uds_path(const std::string& path);
  static bool is_valid_device_path(const std::string& device);

 private:
  // Constants for retry count
  static constexpr int FINITE_MIN_RETRY_COUNT = 0;
  static constexpr int FINITE_MAX_RETRY_COUNT = base::constants::MAX_RETRIES_LIMIT;
};

// Inline implementations for simple validations
inline void InputValidator::validate_non_empty_string(const std::string& str, const std::string& field_name) {
  if (str.empty()) {
    throw diagnostics::ValidationException(field_name + " cannot be empty", field_name, "non-empty string");
  }
}

inline void InputValidator::validate_string_length(const std::string& str, size_t max_length,
                                                   const std::string& field_name) {
  if (str.length() > max_length) {
    throw diagnostics::ValidationException(field_name + " length exceeds maximum allowed length", field_name,
                                           "length <= " + std::to_string(max_length));
  }
}

inline void InputValidator::validate_positive_number(int64_t value, const std::string& field_name) {
  if (value <= 0) {
    throw diagnostics::ValidationException(field_name + " must be positive", field_name, "positive number");
  }
}

inline void InputValidator::validate_range(int64_t value, int64_t min, int64_t max, const std::string& field_name) {
  if (value < min || value > max) {
    throw diagnostics::ValidationException(field_name + " out of range", field_name,
                                           std::to_string(min) + " <= value <= " + std::to_string(max));
  }
}

inline void InputValidator::validate_range(size_t value, size_t min, size_t max, const std::string& field_name) {
  if (value < min || value > max) {
    throw diagnostics::ValidationException(field_name + " out of range", field_name,
                                           std::to_string(min) + " <= value <= " + std::to_string(max));
  }
}

inline void InputValidator::validate_buffer_size(size_t size) {
  validate_range(size, static_cast<size_t>(base::constants::MIN_BUFFER_SIZE),
                 static_cast<size_t>(base::constants::MAX_BUFFER_SIZE), "buffer_size");
}

inline void InputValidator::validate_timeout(unsigned timeout_ms) {
  validate_range(static_cast<int64_t>(timeout_ms), static_cast<int64_t>(base::constants::MIN_CONNECTION_TIMEOUT_MS),
                 static_cast<int64_t>(base::constants::MAX_CONNECTION_TIMEOUT_MS), "timeout_ms");
}

inline void InputValidator::validate_retry_interval(unsigned interval_ms) {
  validate_range(static_cast<int64_t>(interval_ms), static_cast<int64_t>(base::constants::MIN_RETRY_INTERVAL_MS),
                 static_cast<int64_t>(base::constants::MAX_RETRY_INTERVAL_MS), "retry_interval_ms");
}

inline void InputValidator::validate_retry_count(int retry_count) {
  if (retry_count == base::constants::DEFAULT_MAX_RETRIES) {  // -1 means infinite, which is valid
    return;
  }
  if (retry_count < FINITE_MIN_RETRY_COUNT || retry_count > FINITE_MAX_RETRY_COUNT) {
    throw diagnostics::ValidationException("retry_count out of range", "retry_count",
                                           std::to_string(FINITE_MIN_RETRY_COUNT) + " <= retry_count <= " +
                                               std::to_string(FINITE_MAX_RETRY_COUNT) + " or -1 for infinite");
  }
}

inline void InputValidator::validate_port(uint16_t port) {
  if (port == 0) {
    throw diagnostics::ValidationException("port cannot be zero", "port", "non-zero port number");
  }
  // Port numbers are already constrained by uint16_t type (0-65535)
}

inline void InputValidator::validate_baud_rate(uint32_t baud_rate) {
  validate_range(static_cast<int64_t>(baud_rate), static_cast<int64_t>(base::constants::MIN_BAUD_RATE),
                 static_cast<int64_t>(base::constants::MAX_BAUD_RATE), "baud_rate");
}

inline void InputValidator::validate_data_bits(uint8_t data_bits) {
  validate_range(static_cast<int64_t>(data_bits), static_cast<int64_t>(base::constants::MIN_DATA_BITS),
                 static_cast<int64_t>(base::constants::MAX_DATA_BITS), "data_bits");
}

inline void InputValidator::validate_stop_bits(uint8_t stop_bits) {
  validate_range(static_cast<int64_t>(stop_bits), static_cast<int64_t>(base::constants::MIN_STOP_BITS),
                 static_cast<int64_t>(base::constants::MAX_STOP_BITS), "stop_bits");
}

inline void InputValidator::validate_memory_alignment(const void* ptr, size_t alignment) {
  if (ptr == nullptr) {
    throw diagnostics::ValidationException("memory pointer cannot be null", "ptr", "non-null pointer");
  }

  uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
  if (address % alignment != 0) {
    throw diagnostics::ValidationException("memory pointer not properly aligned", "ptr",
                                           "aligned to " + std::to_string(alignment) + " bytes");
  }
}

}  // namespace util
}  // namespace unilink
