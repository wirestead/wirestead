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

#include <stdexcept>
#include <string>

#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace diagnostics {

/**
 * @brief Base exception class for all wirestead exceptions
 *
 * Provides a common base for all exceptions thrown by the wirestead library.
 * Includes additional context information for better error reporting.
 */
class WIRESTEAD_API WiresteadException : public std::runtime_error {
 public:
  explicit WiresteadException(const std::string& message, const std::string& component = "",
                              const std::string& operation = "")
      : std::runtime_error(message), component_(component), operation_(operation) {}

  virtual ~WiresteadException() noexcept;

  const std::string& component() const noexcept { return component_; }
  const std::string& operation() const noexcept { return operation_; }

  std::string full_message() const {
    if (component_.empty() && operation_.empty()) {
      return what();
    }
    if (operation_.empty()) {
      return "[" + component_ + "] " + what();
    }
    if (component_.empty()) {
      return std::string(what()) + " (operation: " + operation_ + ")";
    }
    return "[" + component_ + "] " + what() + " (operation: " + operation_ + ")";
  }

 private:
  std::string component_;
  std::string operation_;
};

/**
 * @brief Exception thrown during builder operations
 *
 * Indicates errors that occur during the construction or configuration
 * of communication channels using the Builder pattern.
 */
class WIRESTEAD_API BuilderException : public WiresteadException {
 public:
  explicit BuilderException(const std::string& message, const std::string& builder_type = "",
                            const std::string& operation = "")
      : WiresteadException(message, "builder", operation), builder_type_(builder_type) {}

  ~BuilderException() noexcept override;

  const std::string& builder_type() const noexcept { return builder_type_; }

  std::string full_message() const {
    if (builder_type_.empty()) {
      return WiresteadException::full_message();
    }
    return "[" + builder_type_ + "] " + WiresteadException::full_message();
  }

 private:
  std::string builder_type_;
};

/**
 * @brief Exception thrown during input validation
 *
 * Indicates that input parameters failed validation checks.
 * Provides detailed information about what validation failed.
 */
class WIRESTEAD_API ValidationException : public WiresteadException {
 public:
  explicit ValidationException(const std::string& message, const std::string& parameter = "",
                               const std::string& expected = "")
      : WiresteadException(message, "validation", "validate"), parameter_(parameter), expected_(expected) {}

  ~ValidationException() noexcept override;

  const std::string& parameter() const noexcept { return parameter_; }
  const std::string& expected() const noexcept { return expected_; }

  std::string full_message() const {
    if (parameter_.empty() && expected_.empty()) {
      return WiresteadException::full_message();
    }
    if (expected_.empty()) {
      return WiresteadException::full_message() + " (parameter: " + parameter_ + ")";
    }
    if (parameter_.empty()) {
      return WiresteadException::full_message() + " (expected: " + expected_ + ")";
    }
    return WiresteadException::full_message() + " (parameter: " + parameter_ + ") (expected: " + expected_ + ")";
  }

 private:
  std::string parameter_;
  std::string expected_;
};

/**
 * @brief Exception thrown during memory operations
 *
 * Indicates errors related to memory allocation, deallocation,
 * or memory safety violations.
 */
class WIRESTEAD_API MemoryException : public WiresteadException {
 public:
  explicit MemoryException(const std::string& message, size_t size = 0, const std::string& operation = "")
      : WiresteadException(message, "memory", operation), size_(size) {}

  ~MemoryException() noexcept override;

  size_t size() const noexcept { return size_; }

  std::string full_message() const {
    if (size_ == 0) {
      return WiresteadException::full_message();
    }
    return WiresteadException::full_message() + " (size: " + std::to_string(size_) + " bytes)";
  }

 private:
  size_t size_;
};

/**
 * @brief Exception thrown during connection operations
 *
 * Indicates errors that occur during network or serial connection
 * establishment, maintenance, or teardown.
 */
class WIRESTEAD_API ConnectionException : public WiresteadException {
 public:
  explicit ConnectionException(const std::string& message, const std::string& connection_type = "",
                               const std::string& operation = "")
      : WiresteadException(message, "connection", operation), connection_type_(connection_type) {}

  ~ConnectionException() noexcept override;

  const std::string& connection_type() const noexcept { return connection_type_; }

  std::string full_message() const {
    if (connection_type_.empty()) {
      return WiresteadException::full_message();
    }
    return "[" + connection_type_ + "] " + WiresteadException::full_message();
  }

 private:
  std::string connection_type_;
};

/**
 * @brief Exception thrown during configuration operations
 *
 * Indicates errors that occur during configuration loading,
 * validation, or application.
 */
class WIRESTEAD_API ConfigurationException : public WiresteadException {
 public:
  explicit ConfigurationException(const std::string& message, const std::string& config_section = "",
                                  const std::string& operation = "")
      : WiresteadException(message, "configuration", operation), config_section_(config_section) {}

  ~ConfigurationException() noexcept override;

  const std::string& config_section() const noexcept { return config_section_; }

  std::string full_message() const {
    if (config_section_.empty()) {
      return WiresteadException::full_message();
    }
    return WiresteadException::full_message() + " (section: " + config_section_ + ")";
  }

 private:
  std::string config_section_;
};

using UnilinkException = WiresteadException;

}  // namespace diagnostics

}  // namespace wirestead
