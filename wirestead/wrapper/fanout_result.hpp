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

#include <array>
#include <cstddef>

#include "wirestead/wrapper/send_result.hpp"

namespace wirestead::wrapper {

// Local admission over one fixed target set, never delivery confirmation.
// An empty result means no targets; it is neither acceptance nor rejection.
class [[nodiscard]] FanoutResult {
 public:
  constexpr std::size_t target_count() const noexcept { return accepted_ + rejected_; }
  constexpr std::size_t accepted_count() const noexcept { return accepted_; }
  constexpr std::size_t rejected_count() const noexcept { return rejected_; }
  constexpr std::size_t rejected_count(SendRejection reason) const noexcept {
    return reasons_[static_cast<std::size_t>(reason)];
  }
  constexpr bool empty() const noexcept { return target_count() == 0; }
  constexpr explicit operator bool() const noexcept { return accepted_ != 0; }

  // Add exactly one admission decision for each selected target.
  constexpr void add(SendResult result) noexcept {
    if (result.accepted()) {
      ++accepted_;
    } else {
      ++rejected_;
      ++reasons_[static_cast<std::size_t>(result.reason())];
    }
  }

 private:
  std::size_t accepted_ = 0;
  std::size_t rejected_ = 0;
  std::array<std::size_t, static_cast<std::size_t>(SendRejection::CancelledWhileWaiting) + 1> reasons_{};
};

}  // namespace wirestead::wrapper
