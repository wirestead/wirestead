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

#include <optional>
#include <variant>

#include "wirestead/interface/result_channel.hpp"

namespace wirestead::interface {

/**
 * A single connection pinned for one Reliable send, including capacity retries.
 *
 * All methods return promptly and serialize with connection replacement, stop
 * and admission. No method invokes a Channel/user callback inline.
 * The handle keeps its connection record alive after reconnect; it must never
 * redirect a write to a replacement connection.
 */
class WIRESTEAD_API WriteConnection {
 public:
  virtual ~WriteConnection() = default;

  /**
   * nullopt: capacity is still blocked; acceptance: retry admission now.
   * Neither acceptance nor polling reserves capacity.
   * Connection loss ends a blocked wait with NotReady; cancellation by stop
   * ends it with CancelledWhileWaiting. Retain the first terminal cause on
   * the connection record, under the same lock as these events, even when
   * stop, loss and reconnect all happen before the next poll.
   */
  [[nodiscard]] virtual std::optional<wrapper::SendResult> poll_capacity() = 0;

  // Validate the pinned connection and admit atomically. WouldBlock is the only
  // retryable refusal. Rejected moves retain their input; rejected shared writes
  // retain no ownership. Acceptance means local queue admission, not delivery.
  [[nodiscard]] virtual wrapper::SendResult write_copy(memory::ConstByteSpan data) = 0;
  [[nodiscard]] virtual wrapper::SendResult write_move(std::vector<uint8_t>&& data) = 0;
  [[nodiscard]] virtual wrapper::SendResult write_shared(std::shared_ptr<const std::vector<uint8_t>> data) = 0;
};

/**
 * Optional custom Channel capability for connection-pinned Reliable sends.
 *
 * ResultChannel supplies nonblocking admission. This extension additionally
 * supplies atomic connection capture, capacity polling and pinned admission.
 * Legacy bool-only Channels remain supported without this stronger guarantee.
 */
class WIRESTEAD_API ConnectionChannel : public ResultChannel {
 public:
  using Connection = std::shared_ptr<WriteConnection>;
  using CaptureResult = std::variant<wrapper::SendRejection, Connection>;

  // Check readiness and capture the current connection in one decision. Return
  // an actual rejection or a non-null handle, never an unpinned success.
  [[nodiscard]] virtual CaptureResult capture_write_connection() = 0;

  // Called before wrapper stop releases waiters. Serialize with capture, loss
  // and admission; preserve an earlier terminal cause. Do not invoke callbacks
  // inline or wait for an executor. Channel::stop must cancel waits too when
  // the transport is stopped directly.
  virtual void cancel_write_waits() = 0;
};

}  // namespace wirestead::interface
