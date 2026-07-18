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

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "wirestead/wrapper/runtime_stats.hpp"

namespace wirestead {
namespace diagnostics {

struct RuntimeStatsCounters {
  std::atomic<uint64_t> bytes_accepted{0};
  std::atomic<uint64_t> messages_accepted{0};

  std::atomic<uint64_t> bytes_sent{0};
  std::atomic<uint64_t> messages_sent{0};

  std::atomic<uint64_t> bytes_received{0};
  std::atomic<uint64_t> messages_received{0};

  std::atomic<uint64_t> failed_sends{0};

  std::atomic<uint64_t> dropped_messages{0};
  std::atomic<uint64_t> dropped_bytes{0};

  std::atomic<uint64_t> backpressure_events{0};

  std::atomic<size_t> max_queued_bytes{0};

  void record_accepted(size_t bytes) {
    messages_accepted.fetch_add(1, std::memory_order_relaxed);
    bytes_accepted.fetch_add(bytes, std::memory_order_relaxed);
  }

  void record_sent(size_t bytes) {
    messages_sent.fetch_add(1, std::memory_order_relaxed);
    bytes_sent.fetch_add(bytes, std::memory_order_relaxed);
  }

  void record_received(size_t bytes) {
    messages_received.fetch_add(1, std::memory_order_relaxed);
    bytes_received.fetch_add(bytes, std::memory_order_relaxed);
  }

  void record_failed_send() { failed_sends.fetch_add(1, std::memory_order_relaxed); }

  void record_dropped(size_t messages, size_t bytes) {
    dropped_messages.fetch_add(static_cast<uint64_t>(messages), std::memory_order_relaxed);
    dropped_bytes.fetch_add(static_cast<uint64_t>(bytes), std::memory_order_relaxed);
  }

  void record_backpressure_event() { backpressure_events.fetch_add(1, std::memory_order_relaxed); }

  void observe_queue(size_t queued) {
    size_t current = max_queued_bytes.load(std::memory_order_relaxed);
    while (queued > current && !max_queued_bytes.compare_exchange_weak(current, queued, std::memory_order_relaxed,
                                                                       std::memory_order_relaxed)) {
    }
  }

  wrapper::RuntimeStats snapshot(size_t queued_bytes, size_t pending_bytes, bool backpressure_active) const {
    wrapper::RuntimeStats stats;
    stats.bytes_accepted = bytes_accepted.load(std::memory_order_relaxed);
    stats.messages_accepted = messages_accepted.load(std::memory_order_relaxed);
    stats.bytes_sent = bytes_sent.load(std::memory_order_relaxed);
    stats.messages_sent = messages_sent.load(std::memory_order_relaxed);
    stats.bytes_received = bytes_received.load(std::memory_order_relaxed);
    stats.messages_received = messages_received.load(std::memory_order_relaxed);
    stats.failed_sends = failed_sends.load(std::memory_order_relaxed);
    stats.dropped_messages = dropped_messages.load(std::memory_order_relaxed);
    stats.dropped_bytes = dropped_bytes.load(std::memory_order_relaxed);
    stats.backpressure_events = backpressure_events.load(std::memory_order_relaxed);
    stats.queued_bytes = queued_bytes;
    stats.pending_bytes = pending_bytes;
    stats.max_queued_bytes = max_queued_bytes.load(std::memory_order_relaxed);
    stats.backpressure_active = backpressure_active;
    return stats;
  }

  void reset(size_t current_queued_bytes) {
    bytes_accepted.store(0, std::memory_order_relaxed);
    messages_accepted.store(0, std::memory_order_relaxed);
    bytes_sent.store(0, std::memory_order_relaxed);
    messages_sent.store(0, std::memory_order_relaxed);
    bytes_received.store(0, std::memory_order_relaxed);
    messages_received.store(0, std::memory_order_relaxed);
    failed_sends.store(0, std::memory_order_relaxed);
    dropped_messages.store(0, std::memory_order_relaxed);
    dropped_bytes.store(0, std::memory_order_relaxed);
    backpressure_events.store(0, std::memory_order_relaxed);
    max_queued_bytes.store(current_queued_bytes, std::memory_order_relaxed);
  }
};

}  // namespace diagnostics
}  // namespace wirestead
