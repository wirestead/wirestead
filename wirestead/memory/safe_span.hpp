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
#include <span>
#include <stdexcept>

namespace wirestead {
namespace memory {

/**
 * @brief C++20 std::span based memory view
 *
 * SafeSpan is now a specialized version of std::span that maintains
 * backward compatibility for at() and ensures subspan returns SafeSpan.
 */
template <typename T, std::size_t Extent = std::dynamic_extent>
class SafeSpan : public std::span<T, Extent> {
 public:
  using Base = std::span<T, Extent>;
  using Base::Base;

  // Polyfill for C++17 pointer+size constructor (std::span requires contiguous_iterator)
  constexpr SafeSpan(T* data, std::size_t size) noexcept : Base(data, size) {}

  // Constructor from std::span
  constexpr SafeSpan(Base s) noexcept : Base(s) {}

  // Polyfill for at() which is missing in std::span
  constexpr typename Base::reference at(std::size_t index) const {
    if (index >= this->size()) {
      throw std::out_of_range("SafeSpan index out of range");
    }
    return (*this)[index];
  }

  // Override subspan to return SafeSpan instead of std::span
  constexpr SafeSpan<T, std::dynamic_extent> subspan(std::size_t offset,
                                                     std::size_t count = std::dynamic_extent) const {
    return SafeSpan<T, std::dynamic_extent>(Base::subspan(offset, count));
  }

  constexpr SafeSpan<T, std::dynamic_extent> first(std::size_t count) const {
    return SafeSpan<T, std::dynamic_extent>(Base::first(count));
  }

  constexpr SafeSpan<T, std::dynamic_extent> last(std::size_t count) const {
    return SafeSpan<T, std::dynamic_extent>(Base::last(count));
  }
};

// Type aliases for common types
using ByteSpan = SafeSpan<uint8_t>;
using ConstByteSpan = SafeSpan<const uint8_t>;
using CharSpan = SafeSpan<char>;
using ConstCharSpan = SafeSpan<const char>;

}  // namespace memory
}  // namespace wirestead
