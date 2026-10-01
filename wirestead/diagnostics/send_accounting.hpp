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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <unordered_map>

#include "wirestead/wrapper/send_accounting.hpp"

namespace wirestead::diagnostics {

// Server callers hold their session-map mutex while transferring or reading
// contributors. Each source is a coherent ledger snapshot.
inline void accumulate_send_accounting(wrapper::SendAccounting& total, const wrapper::SendAccounting& source) {
  const auto add = [](wrapper::SendRequestTotals& to, const wrapper::SendRequestTotals& from) {
    to.requests += from.requests;
    to.bytes += from.bytes;
  };
  const auto loss = [&](wrapper::SendLossTotals& to, const wrapper::SendLossTotals& from) {
    add(to.discarded_before_write, from.discarded_before_write);
    add(to.aborted_during_write, from.aborted_during_write);
  };
  add(total.accepted, source.accepted);
  add(total.written, source.written);
  add(total.outstanding, source.outstanding);
  loss(total.explicit_stop, source.explicit_stop);
  loss(total.connection_loss, source.connection_loss);
  loss(total.queue_pressure, source.queue_pressure);
  loss(total.session_expiry, source.session_expiry);
  total.confirmed_written_bytes += source.confirmed_written_bytes;
}

// The admitting thread and the executor each take the ledger lock per message
// or batch, for a few dozen nanoseconds. Sleeping on contention costs a futex
// round trip on both sides, far longer than the wait itself.
class BriefMutex {
 public:
  void lock() {
    for (int i = 0; i < 100; ++i)
      if (mutex_.try_lock()) return;
    mutex_.lock();
  }
  void unlock() { mutex_.unlock(); }
  bool try_lock() { return mutex_.try_lock(); }

 private:
  std::mutex mutex_;
};

// Shared by admission threads and the transport executor. IDs never repeat
// across reset; old completions cannot change a replacement measurement epoch.
class SendAccountingLedger {
 public:
  using Request = uint64_t;
  enum class Cause { ExplicitStop, ConnectionLoss, QueuePressure, SessionExpiry };

  // A contributor belongs to this ledger's measurement epoch. Entries retain
  // it while I/O is outstanding, even after a virtual session leaves its map.
  struct Group {
    wrapper::SendAccounting totals;
    uint64_t epoch = 0;
  };
  using GroupHandle = std::shared_ptr<Group>;

  Request admit(size_t bytes, GroupHandle group = {}) {
    std::lock_guard<BriefMutex> lock(mutex_);
    // Commit the ID only once the entry is stored: a failed insert changes nothing.
    const auto id = next_id_ + 1;
    if (group && group->epoch != epoch_) {
      group->totals = {};
      group->epoch = epoch_;
    }
    const Entry entry{bytes, epoch_, false, std::move(group)};
    entries_.emplace(id, entry);
    next_id_ = id;
    update(entry, [&](auto& totals) {
      add(totals.accepted, bytes);
      add(totals.outstanding, bytes);
    });
    return id;
  }

  // Roll back tracking if posting throws before the API returns acceptance.
  class Admission {
   public:
    Admission(SendAccountingLedger& ledger, size_t bytes, GroupHandle group = {})
        : ledger_(ledger), request_(ledger.admit(bytes, std::move(group))) {}
    Admission(const Admission&) = delete;
    Admission& operator=(const Admission&) = delete;
    ~Admission() {
      if (!committed_) ledger_.rollback(request_);
    }
    Request request() const { return request_; }
    void commit() { committed_ = true; }

   private:
    SendAccountingLedger& ledger_;
    Request request_;
    bool committed_ = false;
  };

  // Called immediately before handing a request to local I/O.
  bool begin(Request id) {
    std::lock_guard<BriefMutex> lock(mutex_);
    return begin_locked(id);
  }

  // Projections only read the caller-owned batch. They must not re-enter this
  // ledger. Returns false, activating nothing, if end() already terminated any
  // request: the caller must not write the batch. This makes the handoff
  // atomic with stop/loss without the caller's admission lock.
  template <typename Range, typename RequestOf>
  bool begin_batch(const Range& batch, RequestOf request_of) {
    std::lock_guard<BriefMutex> lock(mutex_);
    for (const auto& item : batch)
      if (!entries_.contains(request_of(item))) return false;
    for (const auto& item : batch) begin_locked(request_of(item));
    return true;
  }

  // One composed-operation completion supplies the prefix length for each
  // request. A fully covered request is written even if a later request fails.
  void complete(Request id, size_t confirmed_bytes) {
    std::lock_guard<BriefMutex> lock(mutex_);
    complete_locked(id, confirmed_bytes);
  }

  // Consume the wire prefix even for retired IDs or older measurement epochs.
  // Release the ledger mutex before the caller handles connection loss/stop.
  template <typename Range, typename RequestOf, typename SizeOf>
  void complete_batch(const Range& batch, size_t confirmed_bytes, RequestOf request_of, SizeOf size_of) {
    std::lock_guard<BriefMutex> lock(mutex_);
    for (const auto& item : batch) {
      const auto confirmed = std::min(confirmed_bytes, size_of(item));
      complete_locked(request_of(item), confirmed);
      confirmed_bytes -= confirmed;
    }
  }

  void discard(Request id, Cause cause) {
    std::lock_guard<BriefMutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    terminate(it->second, cause);
    entries_.erase(it);
  }

  bool contains(Request id) const {
    std::lock_guard<BriefMutex> lock(mutex_);
    return entries_.contains(id);
  }

  // Expiry races with handoff under the caller's admission mutex. Active
  // operations keep their actual outcome; unrelated contributors are untouched.
  void discard_waiting(const GroupHandle& group, Cause cause) {
    if (!group) return;
    std::lock_guard<BriefMutex> lock(mutex_);
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (it->second.group == group && !it->second.active) {
        terminate(it->second, cause);
        it = entries_.erase(it);
      } else {
        ++it;
      }
    }
  }

  // The caller serializes this boundary with admission. Clearing every entry
  // includes requests accepted before their enqueue handler has executed.
  void end(Cause cause) {
    std::lock_guard<BriefMutex> lock(mutex_);
    for (const auto& [id, entry] : entries_) terminate(entry, cause);
    entries_.clear();
  }

  wrapper::SendAccounting snapshot() const {
    std::lock_guard<BriefMutex> lock(mutex_);
    return totals_;
  }

  wrapper::SendAccounting snapshot(const GroupHandle& group) const {
    std::lock_guard<BriefMutex> lock(mutex_);
    return group && group->epoch == epoch_ ? group->totals : wrapper::SendAccounting{};
  }

  // A measurement epoch includes only requests accepted since reset. Retained
  // old requests still transmit but their later completions/cleanup are ignored.
  void reset() {
    std::lock_guard<BriefMutex> lock(mutex_);
    ++epoch_;
    totals_ = {};
  }

 private:
  bool begin_locked(Request id) {
    const auto it = entries_.find(id);
    if (it == entries_.end()) return false;
    it->second.active = true;
    return true;
  }

  void complete_locked(Request id, size_t confirmed_bytes) {
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    const auto entry = it->second;
    update(entry, [&](auto& totals) {
      const auto confirmed = std::min(confirmed_bytes, entry.bytes);
      totals.confirmed_written_bytes += confirmed;
      remove(totals.outstanding, entry.bytes);
      if (confirmed == entry.bytes) {
        add(totals.written, entry.bytes);
      } else {
        add(totals.connection_loss.aborted_during_write, entry.bytes);
      }
    });
    entries_.erase(it);
  }

  void rollback(Request id) {
    std::lock_guard<BriefMutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    update(it->second, [&](auto& totals) {
      remove(totals.accepted, it->second.bytes);
      remove(totals.outstanding, it->second.bytes);
    });
    entries_.erase(it);
  }

  struct Entry {
    size_t bytes;
    uint64_t epoch;
    bool active;
    GroupHandle group;
  };
  template <typename F>
  void update(const Entry& entry, F&& apply) {
    if (entry.epoch != epoch_) return;
    apply(totals_);
    if (entry.group) apply(entry.group->totals);
  }
  static void add(wrapper::SendRequestTotals& totals, size_t bytes) {
    ++totals.requests;
    totals.bytes += bytes;
  }
  static void remove(wrapper::SendRequestTotals& totals, size_t bytes) {
    --totals.requests;
    totals.bytes -= bytes;
  }
  void terminate(const Entry& entry, Cause cause) {
    update(entry, [&](auto& totals) {
      auto& loss = cause == Cause::ExplicitStop     ? totals.explicit_stop
                   : cause == Cause::ConnectionLoss ? totals.connection_loss
                   : cause == Cause::SessionExpiry  ? totals.session_expiry
                                                    : totals.queue_pressure;
      add(entry.active ? loss.aborted_during_write : loss.discarded_before_write, entry.bytes);
      remove(totals.outstanding, entry.bytes);
    });
  }

  mutable BriefMutex mutex_;
  // Every request inserts and erases one node under mutex_; recycling nodes
  // keeps the global allocator out of that critical section.
  std::pmr::unsynchronized_pool_resource node_pool_;
  std::pmr::unordered_map<Request, Entry> entries_{&node_pool_};
  Request next_id_ = 0;
  uint64_t epoch_ = 0;
  wrapper::SendAccounting totals_;
};

}  // namespace wirestead::diagnostics
