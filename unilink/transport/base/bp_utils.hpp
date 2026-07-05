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
#include <memory>
#include <type_traits>
#include <variant>
#include <vector>

#include "unilink/base/constants.hpp"

namespace unilink {
namespace transport {
namespace queue_util {

struct DropAccounting {
  size_t messages = 0;
  size_t bytes = 0;

  bool any() const { return messages > 0 || bytes > 0; }
};

inline bool try_reserve_write_bytes(std::atomic<size_t>& queue_bytes, const std::atomic<size_t>& pending_bytes,
                                    const std::atomic<bool>& backpressure_active, size_t bytes, size_t bp_high,
                                    size_t bp_limit) {
  if (bytes == 0 || bytes > bp_limit || backpressure_active.load(std::memory_order_relaxed)) return false;

  size_t current = queue_bytes.load(std::memory_order_relaxed);
  for (;;) {
    const size_t pending = pending_bytes.load(std::memory_order_relaxed);
    if (current > bp_high || bytes > bp_high - current) return false;
    if (current > bp_limit || pending > bp_limit - current || bytes > bp_limit - current - pending) return false;

    if (queue_bytes.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
      return true;
    }
  }
}

inline void release_reserved_write_bytes(std::atomic<size_t>& queue_bytes, size_t bytes) {
  size_t current = queue_bytes.load(std::memory_order_relaxed);
  for (;;) {
    const size_t next = current > bytes ? current - bytes : 0;
    if (queue_bytes.compare_exchange_weak(current, next, std::memory_order_acq_rel, std::memory_order_relaxed)) return;
  }
}

// Atomically reserves `added` bytes against bp_limit for the plain
// (blocking-capable) async_write_* entry points, accounting for bytes
// already reserved-but-not-yet-routed via `inflight_bytes` in addition to
// `queue_bytes`/`pending_bytes`. Closes an accept-then-drop race where the
// caller-thread precheck used to only read queue_bytes+pending_bytes
// non-atomically without reserving space: concurrent callers could all pass
// the check, then get rejected once actually routed onto the strand, where
// the real combined total exceeded bp_limit (jwsung91/unilink#517).
// Deliberately omits the bp_high/backpressure_active gate that
// try_reserve_write_bytes() applies above - a plain write is allowed to land
// in pending_ while backpressure is active, unlike the non-blocking
// try_write path.
inline bool try_reserve_limit_bytes(const std::atomic<size_t>& queue_bytes, const std::atomic<size_t>& pending_bytes,
                                    std::atomic<size_t>& inflight_bytes, size_t added, size_t bp_limit) {
  if (added == 0 || added > bp_limit) return false;

  size_t current_inflight = inflight_bytes.load(std::memory_order_relaxed);
  for (;;) {
    const size_t queue = queue_bytes.load(std::memory_order_relaxed);
    const size_t pending = pending_bytes.load(std::memory_order_relaxed);
    if (queue > bp_limit || pending > bp_limit - queue || current_inflight > bp_limit - queue - pending ||
        added > bp_limit - queue - pending - current_inflight) {
      return false;
    }

    if (inflight_bytes.compare_exchange_weak(current_inflight, current_inflight + added, std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
      return true;
    }
  }
}

// Releases a reservation made by try_reserve_limit_bytes(). Callers that
// route the message into queue_bytes_/pending_bytes_ must increment that
// counter *before* calling this, so a concurrent try_reserve_limit_bytes()
// on another thread never observes a transient total lower than what's
// truly reserved (which would let it over-admit past bp_limit).
inline void release_reserved_limit_bytes(std::atomic<size_t>& inflight_bytes, size_t bytes) {
  size_t current = inflight_bytes.load(std::memory_order_relaxed);
  for (;;) {
    const size_t next = current > bytes ? current - bytes : 0;
    if (inflight_bytes.compare_exchange_weak(current, next, std::memory_order_acq_rel, std::memory_order_relaxed))
      return;
  }
}

// Returns the byte size of a buffer held in a transport BufferVariant alternative.
// shared_ptr<const vector<uint8_t>> goes through ->size(); everything else via .size().
template <typename T>
inline size_t variant_buffer_size(const T& buf) {
  if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
    return buf ? buf->size() : 0;
  } else {
    return buf.size();
  }
}

// Identity projection: the default for maybe_flush_for_keep_latest()'s `project`
// parameter below, used as-is by transports whose tx_ deque holds the
// BufferVariant directly. UDP's tx_ holds TxItem{BufferVariant, destination}
// instead, and supplies a projection extracting `.buffer` so this same
// trimming logic can still visit the variant inside.
struct IdentityProjection {
  template <typename T>
  constexpr T& operator()(T& x) const {
    return x;
  }
};

// BestEffort queue-trimming shared by all stream transports (TCP client/server, UDS client/server)
// and, via a projection, UDP.
// Must be called on the strand immediately before enqueueing a new buffer of `added` bytes.
//
// No-op for Reliable strategy.
// For BestEffort:
//   added >= bp_high  →  drop entire tx_ (full keep-latest replacement).
//   otherwise         →  pop oldest tx_ entries until queue_bytes + added <= bp_high.
template <typename Deque, typename Project = IdentityProjection>
inline DropAccounting maybe_flush_for_keep_latest(::unilink::base::constants::BackpressureStrategy bp_strategy,
                                                  size_t added, size_t bp_high, Deque& tx,
                                                  std::atomic<size_t>& queue_bytes,
                                                  const std::atomic<bool>& backpressure_active,
                                                  Project project = Project{}) {
  DropAccounting dropped;
  if (bp_strategy != ::unilink::base::constants::BackpressureStrategy::BestEffort) return dropped;

  if (added >= bp_high) {
    size_t removed_bytes = 0;
    for (auto& buf : tx) {
      removed_bytes += std::visit([](const auto& b) { return variant_buffer_size(b); }, project(buf));
    }
    dropped.messages = tx.size();
    dropped.bytes = removed_bytes;
    tx.clear();
    const size_t qb = queue_bytes.load(std::memory_order_relaxed);
    queue_bytes.store(qb > removed_bytes ? qb - removed_bytes : 0, std::memory_order_relaxed);
    return dropped;
  }

  if (backpressure_active.load(std::memory_order_relaxed) ||
      queue_bytes.load(std::memory_order_relaxed) + added > bp_high) {
    while (!tx.empty()) {
      const size_t qb = queue_bytes.load(std::memory_order_relaxed);
      if (qb + added <= bp_high) break;
      const size_t oldest = std::visit([](const auto& b) { return variant_buffer_size(b); }, project(tx.front()));
      queue_bytes.store(qb > oldest ? qb - oldest : 0, std::memory_order_relaxed);
      tx.pop_front();
      ++dropped.messages;
      dropped.bytes += oldest;
    }
  }
  return dropped;
}

}  // namespace queue_util
}  // namespace transport
}  // namespace unilink
