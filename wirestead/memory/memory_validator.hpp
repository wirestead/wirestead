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
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>

#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace memory {

/**
 * @brief Memory validation utilities for enhanced safety
 */
namespace memory_validator {

/**
 * @brief Validate memory region accessibility
 * @param ptr Pointer to memory region
 * @param size Size of memory region
 * @return true if memory is accessible, false otherwise
 */
bool memory_accessible(const void* ptr, size_t size);

/**
 * @brief Validate memory alignment
 * @param ptr Pointer to check
 * @param alignment Required alignment
 * @return true if properly aligned, false otherwise
 */
bool memory_aligned(const void* ptr, size_t alignment);

/**
 * @brief Check for buffer overflow/underflow patterns
 * @param ptr Pointer to buffer
 * @param size Buffer size
 * @param canary_size Size of canary bytes to check
 * @return true if no overflow detected, false if overflow found
 */
bool check_buffer_bounds(const void* ptr, size_t size, size_t canary_size = 8);

/**
 * @brief Initialize canary bytes around buffer
 * @param ptr Pointer to buffer
 * @param size Buffer size
 * @param canary_size Size of canary bytes
 */
void initialize_canary_bytes(void* ptr, size_t size, size_t canary_size = 8);

/**
 * @brief Validate canary bytes around buffer
 * @param ptr Pointer to buffer
 * @param size Buffer size
 * @param canary_size Size of canary bytes
 * @return true if canaries are intact, false if corrupted
 */
bool validate_canary_bytes(const void* ptr, size_t size, size_t canary_size = 8);

/**
 * @brief Safe memory copy with comprehensive validation
 * @param dest Destination buffer
 * @param src Source buffer
 * @param size Number of bytes to copy
 * @throws std::invalid_argument if validation fails
 */
void safe_memcpy(void* dest, const void* src, size_t size);

/**
 * @brief Safe memory move with comprehensive validation
 * @param dest Destination buffer
 * @param src Source buffer
 * @param size Number of bytes to move
 * @throws std::invalid_argument if validation fails
 */
void safe_memmove(void* dest, const void* src, size_t size);

/**
 * @brief Safe memory set with comprehensive validation
 * @param ptr Pointer to buffer
 * @param value Value to set
 * @param size Number of bytes to set
 * @throws std::invalid_argument if validation fails
 */
void safe_memset(void* ptr, int value, size_t size);

/**
 * @brief Check for double-free conditions
 * @param ptr Pointer that was freed
 * @return true if this is a potential double-free
 */
bool double_free(void* ptr);

/**
 * @brief Check for use-after-free conditions
 * @param ptr Pointer to check
 * @return true if pointer appears to be used after free
 */
bool use_after_free(const void* ptr);

}  // namespace memory_validator

/**
 * @brief RAII wrapper for memory validation
 */
class WIRESTEAD_API MemoryValidator {
 public:
  explicit MemoryValidator(void* ptr, size_t size, size_t canary_size = 8);
  ~MemoryValidator();

  // Non-copyable, non-movable
  MemoryValidator(const MemoryValidator&) = delete;
  MemoryValidator& operator=(const MemoryValidator&) = delete;
  MemoryValidator(MemoryValidator&&) = delete;
  MemoryValidator& operator=(MemoryValidator&&) = delete;

  // Validation methods
  bool validate() const;
  void check_bounds() const;

  // Access methods
  void* data() const { return ptr_; }
  size_t size() const { return size_; }

 private:
  void* ptr_;
  size_t size_;
  size_t canary_size_;
  std::vector<uint8_t> original_canaries_;
  bool canaries_initialized_;

  void initialize_canaries();
  bool validate_canaries() const;
};

/**
 * @brief Memory pattern generator for testing
 */
class WIRESTEAD_API MemoryPatternGenerator {
 public:
  static std::vector<uint8_t> generate_pattern(size_t size, uint8_t seed = 0xAA);
  static std::vector<uint8_t> generate_random_pattern(size_t size);
  static bool validate_pattern(const void* ptr, size_t size, uint8_t expected_seed = 0xAA);
};

}  // namespace memory
}  // namespace wirestead
