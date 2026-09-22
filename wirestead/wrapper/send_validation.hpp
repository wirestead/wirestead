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
#include <cstddef>
#include <optional>

#include "wirestead/base/constants.hpp"
#include "wirestead/wrapper/send_result.hpp"
namespace wirestead::wrapper::detail {
// Stage-1 size validation only: success means the payload passed this check,
// not that a transport accepted it. Line callers include their delimiter.
constexpr SendResult validate_payload_size(std::size_t size,
                                           std::optional<std::size_t> queue_limit = std::nullopt) noexcept {
  if (size == 0) return SendResult::reject(SendRejection::InvalidArgument);
  if (size > base::constants::MAX_BUFFER_SIZE || (queue_limit && size > *queue_limit)) {
    return SendResult::reject(SendRejection::TooLarge);
  }
  return SendResult::accept();
}

// Invalid sizes reach the transport without a capacity wait. It still owns
// final rejection, callbacks and accounting until public send APIs migrate.
constexpr bool payload_needs_capacity(std::size_t size,
                                      std::optional<std::size_t> queue_limit = std::nullopt) noexcept {
  return validate_payload_size(size, queue_limit).accepted();
}
}  // namespace wirestead::wrapper::detail
