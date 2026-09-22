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

#include <cassert>
#include <cstdint>

namespace wirestead::wrapper {

// Immediate send rejection only. Loss after acceptance belongs in statistics.
// The enum may grow; consumers should provide a default case when switching.
enum class SendRejection : std::uint8_t {
  NotStarted,
  Stopping,
  NotReady,
  WouldBlock,
  QueueFull,
  TooLarge,
  InvalidArgument,
  CancelledWhileWaiting
};

// An admission outcome, never a delivery receipt. Construct an explicit outcome
// through accept()/reject(); there is no default or implicit bool constructor.
class [[nodiscard]] SendResult {
 public:
  static constexpr SendResult accept() noexcept { return SendResult(true, SendRejection::NotStarted); }
  static constexpr SendResult reject(SendRejection reason) noexcept { return SendResult(false, reason); }

  constexpr bool accepted() const noexcept { return accepted_; }
  constexpr explicit operator bool() const noexcept { return accepted(); }

  // Precondition: !accepted(). The stored reason has no meaning on success.
  constexpr SendRejection reason() const noexcept {
    assert(!accepted_ && "SendResult::reason() requires a rejected result");
    return reason_;
  }

 private:
  constexpr SendResult(bool accepted, SendRejection reason) noexcept : accepted_(accepted), reason_(reason) {}
  bool accepted_;
  SendRejection reason_;
};

}  // namespace wirestead::wrapper
