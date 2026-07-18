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

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/memory/safe_span.hpp"

namespace wirestead {
namespace memory {

/**
 * @brief Safe data buffer for type-safe data transfer
 *
 * This class provides a safe wrapper around binary data that can be
 * constructed from various sources and provides safe access methods.
 */
class WIRESTEAD_API SafeDataBuffer {
 public:
  // Constructors
  explicit SafeDataBuffer(const std::string& data);
  explicit SafeDataBuffer(std::string_view data);
  explicit SafeDataBuffer(std::vector<uint8_t> data);
  explicit SafeDataBuffer(const uint8_t* data, size_t size);
  explicit SafeDataBuffer(const char* data, size_t size);
  explicit SafeDataBuffer(ConstByteSpan span);

  // Copy and move constructors
  SafeDataBuffer(const SafeDataBuffer&) = default;
  SafeDataBuffer& operator=(const SafeDataBuffer&) = default;
  SafeDataBuffer(SafeDataBuffer&&) = default;
  SafeDataBuffer& operator=(SafeDataBuffer&&) = default;

  // Destructor
  ~SafeDataBuffer() = default;

  // Access methods
  std::string as_string() const;
  ConstByteSpan as_span() const noexcept;
  const uint8_t* data() const noexcept;
  size_t size() const noexcept;
  bool empty() const noexcept;

  // Safe element access
  const uint8_t& operator[](size_t index) const;
  const uint8_t& at(size_t index) const;

  // Comparison operators
  bool operator==(const SafeDataBuffer& other) const noexcept;
  bool operator!=(const SafeDataBuffer& other) const noexcept;

  // Utility methods
  void clear() noexcept;
  void reserve(size_t capacity);
  void resize(size_t new_size);

  // Validation
  bool valid() const noexcept;
  void validate() const;

 private:
  std::vector<uint8_t> data_;

  // Helper methods
  void validate_index(size_t index) const;
  void validate_construction(const void* ptr, size_t size) const;
};

/**
 * @brief Safe data handler type for callbacks
 */
using SafeDataHandler = std::function<void(const SafeDataBuffer&)>;

/**
 * @brief Factory functions for creating SafeDataBuffer instances
 */
namespace safe_buffer_factory {
SafeDataBuffer from_string(const std::string& str);
SafeDataBuffer from_c_string(const char* str);
SafeDataBuffer from_vector(const std::vector<uint8_t>& vec);
SafeDataBuffer from_raw_data(const uint8_t* data, size_t size);
SafeDataBuffer from_span(ConstByteSpan span);
}  // namespace safe_buffer_factory

}  // namespace memory
}  // namespace wirestead
