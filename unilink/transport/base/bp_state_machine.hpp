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

#include "unilink/base/constants.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/transport/base/bp_utils.hpp"

// Shared "pre-check -> route to tx/pending -> report backpressure -> drain on
// terminate" state machine, factored out of ~6 independently-maintained
// per-transport copies (jwsung91/unilink#434). Deliberately non-owning/
// ref-based rather than template-owning the queues themselves: buffer
// element types genuinely differ across transports (UDP's TxItem carries a
// destination endpoint that stream transports have no analogue for), so
// this header only decides what a transport's own enqueue/write-completion
// code should do, and calls back into transport-supplied hooks for the
// pieces that must stay transport-specific (moving a buffer element from
// pending_ into tx_, kicking off a write).
namespace unilink {
namespace transport {
namespace queue_util {

// The subset of a transport's Impl state this state machine needs to read
// and mutate. All fields are references into the transport's own members -
// this struct never owns anything and is cheap to construct per call.
struct BackpressureFields {
  std::atomic<size_t>& queue_bytes;
  std::atomic<size_t>& pending_bytes;
  std::atomic<bool>& backpressure_active;
  size_t bp_high;
  size_t bp_low;
  size_t bp_limit;
  ::unilink::base::constants::BackpressureStrategy strategy;
};

enum class EnqueueDecision {
  Immediate,  // push the buffer onto tx_ and proceed to write it now
  Pending,    // Reliable + backpressure already active: route to pending_ instead
  Rejected,   // over bp_limit_ even after BestEffort trimming; caller must drop it
};

// Decides how a new buffer of `added` bytes should be routed, mirroring the
// branch structure every transport's enqueue function hand-copied. Does NOT
// push `added` into any queue itself - the caller pushes its own
// buffer/destination element into tx_ or pending_ based on the returned
// decision, and is responsible for incrementing the corresponding byte
// counter (queue_bytes/pending_bytes) to match. For BestEffort, trims `tx`
// via the existing maybe_flush_for_keep_latest() before returning
// Immediate, recording anything dropped into `dropped_out`. `project`
// defaults to identity (tx's elements are the BufferVariant directly);
// UDP's tx_ holds TxItem{BufferVariant, destination} instead and supplies a
// projection extracting `.buffer`.
template <typename Deque, typename Project = IdentityProjection>
inline EnqueueDecision decide_enqueue(BackpressureFields& f, size_t added, Deque& tx, DropAccounting& dropped_out,
                                      Project project = Project{}) {
  using Strategy = ::unilink::base::constants::BackpressureStrategy;

  if (f.strategy == Strategy::Reliable && f.backpressure_active.load(std::memory_order_relaxed)) {
    if (f.queue_bytes.load(std::memory_order_relaxed) + f.pending_bytes.load(std::memory_order_relaxed) + added >
        f.bp_limit) {
      return EnqueueDecision::Rejected;
    }
    return EnqueueDecision::Pending;
  }

  if (f.strategy == Strategy::BestEffort && (f.backpressure_active.load(std::memory_order_relaxed) ||
                                             f.queue_bytes.load(std::memory_order_relaxed) + added > f.bp_high)) {
    dropped_out =
        maybe_flush_for_keep_latest(f.strategy, added, f.bp_high, tx, f.queue_bytes, f.backpressure_active, project);
  }

  if (f.queue_bytes.load(std::memory_order_relaxed) + added > f.bp_limit) {
    return EnqueueDecision::Rejected;
  }
  return EnqueueDecision::Immediate;
}

// Runs the ON / OFF-with-reflush / re-ARM state machine and fires on_bp
// accordingly, mirroring UdpChannel::report_backpressure() (the
// best-behaved existing copy). `flush_pending_into_tx` is called exactly
// once, only on the OFF transition, and must move every element out of the
// transport's own pending_ deque into tx_ (preserving whatever
// transport-specific fields those elements carry) and return the number of
// bytes moved; `kick_write` is called if the OFF transition leaves anything
// in tx_ and nothing is currently being written.
template <typename FlushFn, typename KickFn>
inline void report_backpressure(BackpressureFields& f, size_t queued_bytes, interface::Channel::OnBackpressure& on_bp,
                                diagnostics::RuntimeStatsCounters& stats, FlushFn&& flush_pending_into_tx,
                                KickFn&& kick_write) {
  if (!f.backpressure_active.load(std::memory_order_relaxed) && queued_bytes >= f.bp_high) {
    f.backpressure_active.store(true, std::memory_order_relaxed);
    stats.record_backpressure_event();
    if (on_bp) {
      try {
        on_bp(queued_bytes);
      } catch (...) {
      }
    }
    return;
  }

  if (f.backpressure_active.load(std::memory_order_relaxed) && queued_bytes <= f.bp_low) {
    const size_t moved = flush_pending_into_tx();
    f.queue_bytes.fetch_add(moved, std::memory_order_relaxed);
    f.backpressure_active.store(false, std::memory_order_relaxed);
    stats.record_backpressure_event();
    if (on_bp) {
      try {
        on_bp(queued_bytes);
      } catch (...) {
      }  // fire OFF with pre-flush queue size, matching every existing copy
    }

    const size_t post_flush = f.queue_bytes.load(std::memory_order_relaxed);
    if (post_flush >= f.bp_high) {
      f.backpressure_active.store(true, std::memory_order_relaxed);
      stats.record_backpressure_event();
      if (on_bp) {
        try {
          on_bp(post_flush);
        } catch (...) {
        }
      }
    }
    kick_write();
  }
}

// Drops all queued and pending writes and clears backpressure, firing on_bp
// once (with a final queued-bytes value of 0) if it was active - and doing
// nothing at all if it wasn't. This is the *terminal* drain path (stop/error),
// deliberately distinct from report_backpressure()'s normal-operation flush:
// report_backpressure() moves pending_ back into tx_ and can immediately
// re-arm backpressure_active_, which would strand a Reliable-mode sender
// blocked on a condition variable forever once nothing will ever call
// do_write() again (jwsung91/unilink#427, #452). `clear_queues` must clear
// every transport-specific queue (tx_, pending_, and their byte counters).
template <typename ClearFn>
inline void drain_and_clear_backpressure(BackpressureFields& f, interface::Channel::OnBackpressure& on_bp,
                                         ClearFn&& clear_queues) {
  clear_queues();
  const bool had_backpressure = f.backpressure_active.load(std::memory_order_relaxed);
  f.backpressure_active.store(false, std::memory_order_relaxed);
  if (!had_backpressure) return;
  if (on_bp) {
    try {
      on_bp(0);
    } catch (...) {
    }
  }
}

}  // namespace queue_util
}  // namespace transport
}  // namespace unilink
