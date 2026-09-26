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
#include <cstdint>
#include <optional>

#include "wirestead/wrapper/send_accounting.hpp"

namespace wirestead {
namespace wrapper {

struct RuntimeStats {
  uint64_t bytes_accepted = 0;
  uint64_t messages_accepted = 0;

  uint64_t bytes_sent = 0;
  uint64_t messages_sent = 0;

  uint64_t bytes_received = 0;
  uint64_t messages_received = 0;

  uint64_t failed_sends = 0;

  uint64_t dropped_messages = 0;
  uint64_t dropped_bytes = 0;

  uint64_t backpressure_events = 0;

  size_t queued_bytes = 0;
  size_t pending_bytes = 0;
  size_t max_queued_bytes = 0;

  bool backpressure_active = false;

  // Milliseconds since bytes last arrived, or nullopt if none ever have.
  //
  // The signal a robot needs for "has this sensor gone quiet". A device that
  // stops streaming without erroring leaves every other counter looking
  // healthy and every state machine reporting Connected, because nothing has
  // gone wrong - the data simply stopped. Only the age says so.
  //
  // Passive on purpose: what to do about silence is the caller's decision and
  // differs per device, so this reports rather than acts. Compare it against
  // the slowest interval that device is expected to hit, with margin. Serial
  // additionally offers rx_idle_timeout, which does act, because there a
  // concrete recovery exists - reopening the port.
  std::optional<uint64_t> last_receive_age_ms;

  // Present only for transports with logical request accounting (currently
  // built-in TCP/UDS clients and server sessions/aggregates, Serial, UDP sockets and virtual sessions).
  // nullopt means unsupported, not zero loss.
  // Independent of legacy sent/dropped counters; see docs/tcp_send_accounting.md
  // and docs/udp_send_accounting.md (UDP aggregate and per-session scopes).
  std::optional<SendAccounting> send_accounting;
};

}  // namespace wrapper
}  // namespace wirestead
