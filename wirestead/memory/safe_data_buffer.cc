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

#include "wirestead/memory/safe_data_buffer.hpp"

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <cstring>

namespace wirestead {
namespace memory {

// Constructors
SafeDataBuffer::SafeDataBuffer(const std::string& data) : data_(data.begin(), data.end()) {
  validate_construction(data.data(), data.size());
}

SafeDataBuffer::SafeDataBuffer(std::string_view data) : data_(data.begin(), data.end()) {
  validate_construction(data.data(), data.size());
}

SafeDataBuffer::SafeDataBuffer(std::vector<uint8_t> data) : data_(std::move(data)) {
  // No validation needed for moved vector
}

SafeDataBuffer::SafeDataBuffer(const uint8_t* data, size_t size) {
  validate_construction(data, size);
  if (data && size > 0) {
    data_.assign(data, data + size);
  }
}

SafeDataBuffer::SafeDataBuffer(const char* data, size_t size) {
  validate_construction(data, size);
  if (data && size > 0) {
    data_.assign(reinterpret_cast<const uint8_t*>(data), reinterpret_cast<const uint8_t*>(data) + size);
  }
}

SafeDataBuffer::SafeDataBuffer(ConstByteSpan span) {
  validate_construction(span.data(), span.size());
  if (span.data() && span.size() > 0) {
    data_.assign(span.begin(), span.end());
  }
}

// Access methods
std::string SafeDataBuffer::as_string() const { return std::string(data_.begin(), data_.end()); }

ConstByteSpan SafeDataBuffer::as_span() const noexcept { return ConstByteSpan(data_.data(), data_.size()); }

const uint8_t* SafeDataBuffer::data() const noexcept { return data_.data(); }

size_t SafeDataBuffer::size() const noexcept { return data_.size(); }

bool SafeDataBuffer::empty() const noexcept { return data_.empty(); }

// Safe element access
const uint8_t& SafeDataBuffer::operator[](size_t index) const {
  validate_index(index);
  return data_[index];
}

const uint8_t& SafeDataBuffer::at(size_t index) const {
  validate_index(index);
  return data_.at(index);
}

// Comparison operators
bool SafeDataBuffer::operator==(const SafeDataBuffer& other) const noexcept { return data_ == other.data_; }

bool SafeDataBuffer::operator!=(const SafeDataBuffer& other) const noexcept { return data_ != other.data_; }

// Utility methods
void SafeDataBuffer::clear() noexcept { data_.clear(); }

void SafeDataBuffer::reserve(size_t capacity) { data_.reserve(capacity); }

void SafeDataBuffer::resize(size_t new_size) { data_.resize(new_size); }

// Validation
bool SafeDataBuffer::valid() const noexcept {
  return true;  // Always valid after construction
}

void SafeDataBuffer::validate() const {
  // Additional validation can be added here
  if (data_.size() > 1024 * 1024 * 100) {  // 100MB limit
    throw std::runtime_error("Buffer size exceeds maximum allowed size");
  }
}

// Private helper methods
void SafeDataBuffer::validate_index(size_t index) const {
  if (index >= data_.size()) {
    throw std::out_of_range(fmt::format("Index {} is out of range for buffer of size {}", index, data_.size()));
  }
}

void SafeDataBuffer::validate_construction(const void* ptr, size_t size) const {
  if (size > 0 && !ptr) {
    throw std::invalid_argument("Cannot construct buffer from null pointer with non-zero size");
  }
  if (size > 1024 * 1024 * 100) {  // 100MB limit
    throw std::invalid_argument("Buffer size exceeds maximum allowed size");
  }
}

// Factory functions
namespace safe_buffer_factory {

SafeDataBuffer from_string(const std::string& str) { return SafeDataBuffer(str); }

SafeDataBuffer from_c_string(const char* str) {
  if (!str) {
    return SafeDataBuffer(std::vector<uint8_t>{});
  }
  return SafeDataBuffer(str, std::strlen(str));
}

SafeDataBuffer from_vector(const std::vector<uint8_t>& vec) { return SafeDataBuffer(vec); }

SafeDataBuffer from_raw_data(const uint8_t* data, size_t size) { return SafeDataBuffer(data, size); }

SafeDataBuffer from_span(ConstByteSpan span) { return SafeDataBuffer(span); }

}  // namespace safe_buffer_factory

}  // namespace memory
}  // namespace wirestead
