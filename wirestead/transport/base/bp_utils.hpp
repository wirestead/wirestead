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
#include <boost/asio/buffer.hpp>
#include <cstddef>
#include <memory>
#include <mutex>
#include <type_traits>
#include <variant>
#include <vector>

#include "wirestead/base/constants.hpp"

namespace wirestead {
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

// Reserves `added` bytes against bp_limit for the plain (blocking-capable)
// async_write_* entry points, accounting for bytes already
// reserved-but-not-yet-routed via `inflight_bytes` in addition to
// `queue_bytes`/`pending_bytes`. Closes an accept-then-drop race where the
// caller-thread precheck used to only read queue_bytes+pending_bytes
// non-atomically without reserving space: concurrent callers could all pass
// the check, then get rejected once actually routed onto the strand, where
// the real combined total exceeded bp_limit (jwsung91/wirestead#517).
//
// Guarded by `mtx` rather than a lock-free CAS on `inflight_bytes` alone:
// queue_bytes/pending_bytes/inflight_bytes are three independently-atomic
// counters, so no CAS retry loop touching only one of them can make a
// check spanning all three race-free - the strand's commit_reserved_limit_bytes()
// promoting inflight_bytes into queue_bytes/pending_bytes is itself two
// separate atomic ops, and a concurrent reservation's queue_bytes/pending_bytes
// reads can land in the gap between them while its CAS against a stale
// inflight_bytes snapshot still spuriously succeeds. This was caught by a
// CI run reproducing a 1-in-~10000 dropped message under this exact
// window. `mtx` must be the same mutex passed to commit_reserved_limit_bytes()
// for a given transport instance. Deliberately omits the
// bp_high/backpressure_active gate that try_reserve_write_bytes() applies
// above - a plain write is allowed to land in pending_ while backpressure
// is active, unlike the non-blocking try_write path.
inline bool try_reserve_limit_bytes(std::mutex& mtx, const std::atomic<size_t>& queue_bytes,
                                    const std::atomic<size_t>& pending_bytes, std::atomic<size_t>& inflight_bytes,
                                    size_t added, size_t bp_limit) {
  if (added == 0 || added > bp_limit) return false;

  std::lock_guard<std::mutex> lock(mtx);
  const size_t queue = queue_bytes.load(std::memory_order_relaxed);
  const size_t pending = pending_bytes.load(std::memory_order_relaxed);
  const size_t inflight = inflight_bytes.load(std::memory_order_relaxed);
  if (queue > bp_limit || pending > bp_limit - queue || inflight > bp_limit - queue - pending ||
      added > bp_limit - queue - pending - inflight) {
    return false;
  }
  inflight_bytes.fetch_add(added, std::memory_order_relaxed);
  return true;
}

// Releases a reservation made by try_reserve_limit_bytes(), for the case
// where the message never gets promoted into queue_bytes_/pending_bytes_
// (route_enqueued_buffer's Rejected branch - expected to be effectively
// unreachable given the reservation above, but kept as a safety net).
inline void release_reserved_limit_bytes(std::mutex& mtx, std::atomic<size_t>& inflight_bytes, size_t bytes) {
  std::lock_guard<std::mutex> lock(mtx);
  const size_t current = inflight_bytes.load(std::memory_order_relaxed);
  inflight_bytes.store(current > bytes ? current - bytes : 0, std::memory_order_relaxed);
}

// Promotes a reservation made by try_reserve_limit_bytes() into `counter`
// (queue_bytes_ or pending_bytes_) once route_enqueued_buffer() has decided
// where the message lands. Must run under the same `mtx` as
// try_reserve_limit_bytes() and perform both the increment and the
// inflight_bytes release as one critical section - doing them as two
// separately-locked steps would reopen the exact gap described above.
inline void commit_reserved_limit_bytes(std::mutex& mtx, std::atomic<size_t>& counter,
                                        std::atomic<size_t>& inflight_bytes, size_t bytes) {
  std::lock_guard<std::mutex> lock(mtx);
  counter.fetch_add(bytes, std::memory_order_relaxed);
  const size_t current = inflight_bytes.load(std::memory_order_relaxed);
  inflight_bytes.store(current > bytes ? current - bytes : 0, std::memory_order_relaxed);
}

// Locked increment for the rare plain-write path that skips
// try_reserve_limit_bytes() entirely (tcp_client's BestEffort fallback,
// which - unlike every other transport's plain path - has no precheck at
// all, matching its pre-existing behavior). Still must go through the same
// `mtx` as try_reserve_limit_bytes()/commit_reserved_limit_bytes() for this
// transport instance: an increment landing outside the lock could let a
// concurrent Reliable reservation's already-approved check get silently
// invalidated by bytes it never accounted for, reopening the same race for
// Reliable messages that this whole reservation scheme exists to close.
inline void commit_unreserved_limit_bytes(std::mutex& mtx, std::atomic<size_t>& counter, size_t bytes) {
  std::lock_guard<std::mutex> lock(mtx);
  counter.fetch_add(bytes, std::memory_order_relaxed);
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

// ---------------------------------------------------------------------------
// Gather writes
// ---------------------------------------------------------------------------
//
// Stream transports used to hand exactly one queued buffer to async_write(),
// so a backlog of N queued messages cost N send syscalls even though they were
// all ready at once. Draining several into one scatter-gather write collapses
// those into one.
//
// Caps on how much of tx_ a single gather write may take. These matter for
// BestEffort: buffers moved into the in-flight batch have left tx_ and can no
// longer be dropped by maybe_flush_for_keep_latest(), so an unbounded batch
// would let stale data survive a keep-latest trim that was supposed to discard
// it. They also bound how much gets re-queued when a write fails.
// 16, not an arbitrary round number: asio fills at most 16 buffers per
// prepared_buffers (detail/consuming_buffers.hpp, max_buffers), which is also
// the iovec count a single sendmsg gets. Staying at or under it keeps each
// gather write inside one prepare. Larger batches were measured to corrupt the
// stream - at 64 buffers, queued messages came out of order on the wire while
// byte counts still matched, so only an ordering check catches it. Anything
// above this needs that verified again, not assumed.
inline constexpr size_t kMaxGatherBuffers = 16;
inline constexpr size_t kMaxGatherBytes = 256 * 1024;

// Returns a const_buffer over a BufferVariant alternative.
template <typename T>
inline ::boost::asio::const_buffer variant_const_buffer(const T& buf) {
  if constexpr (std::is_same_v<T, std::shared_ptr<const std::vector<uint8_t>>>) {
    return buf ? ::boost::asio::const_buffer(buf->data(), buf->size()) : ::boost::asio::const_buffer();
  } else {
    return ::boost::asio::const_buffer(buf.data(), buf.size());
  }
}
// Identity for plain buffers; tracked/endpoint queue items project their payload.
struct IdentityProjection {
  template <typename T>
  constexpr T& operator()(T& x) const {
    return x;
  }
};

struct IgnoreDroppedBuffer {
  template <typename T>
  void operator()(const T&) const {}
};

// Moves buffers from the front of `tx` into `batch` (which is cleared first)
// up to the caps above, filling `views` with the matching const_buffers.
// Returns the total byte count, which is what the caller must subtract from
// queue_bytes_ once the write completes.
//
// Must run on the strand, with no write already in flight: `views` describes
// memory owned by `batch`, so neither may be touched until the write completes.
//
// `views` is handed to asio as an ordinary ConstBufferSequence, which asio
// copies into the composed operation. An earlier version passed a non-owning
// view to dodge that copy; asio's partial-write bookkeeping does not survive
// an aliasing sequence, and it silently duplicated or stalled queued buffers.
// Project extracts the payload while batch retains the complete queue item.
// One small copy per gather write is the right trade - it replaces N syscalls,
// and with several messages per write it is fewer allocations than before.
template <typename Deque, typename Batch, typename Project = IdentityProjection>
inline size_t take_gather_batch(Deque& tx, Batch& batch, std::vector<::boost::asio::const_buffer>& views,
                                Project project = Project{}) {
  batch.clear();
  views.clear();
  size_t total = 0;
  while (!tx.empty() && batch.size() < kMaxGatherBuffers && total < kMaxGatherBytes) {
    const size_t n = std::visit([](const auto& b) { return variant_buffer_size(b); }, project(tx.front()));
    batch.push_back(std::move(tx.front()));
    tx.pop_front();
    total += n;
  }
  views.reserve(batch.size());
  for (const auto& b : batch) {
    views.push_back(std::visit([](const auto& x) { return variant_const_buffer(x); }, project(b)));
  }
  return total;
}

// Returns a failed batch to the front of `tx` in its original order, so a
// retry/teardown sees the queue exactly as it was.
template <typename Deque, typename Batch>
inline void return_gather_batch(Deque& tx, Batch& batch) {
  for (auto it = batch.rbegin(); it != batch.rend(); ++it) {
    tx.push_front(std::move(*it));
  }
  batch.clear();
}

// BestEffort queue-trimming shared by all stream transports (TCP client/server, UDS client/server)
// and, via a projection, UDP.
// Must be called on the strand immediately before enqueueing a new buffer of `added` bytes.
//
// No-op for Reliable strategy.
// For BestEffort:
//   added >= bp_high  →  drop entire tx_ (full keep-latest replacement).
//   otherwise         →  pop oldest tx_ entries until queue_bytes + added <= bp_high.
// on_drop observes each removed item before destruction and must not throw.
template <typename Deque, typename Project = IdentityProjection, typename OnDrop = IgnoreDroppedBuffer>
inline DropAccounting maybe_flush_for_keep_latest(::wirestead::base::constants::BackpressureStrategy bp_strategy,
                                                  size_t added, size_t bp_high, Deque& tx,
                                                  std::atomic<size_t>& queue_bytes,
                                                  const std::atomic<bool>& backpressure_active,
                                                  Project project = Project{}, OnDrop on_drop = OnDrop{}) {
  DropAccounting dropped;
  if (bp_strategy != ::wirestead::base::constants::BackpressureStrategy::BestEffort) return dropped;

  if (added >= bp_high) {
    size_t removed_bytes = 0;
    for (auto& buf : tx) {
      on_drop(buf);
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
      on_drop(tx.front());
      tx.pop_front();
      ++dropped.messages;
      dropped.bytes += oldest;
    }
  }
  return dropped;
}

}  // namespace queue_util
}  // namespace transport
}  // namespace wirestead
