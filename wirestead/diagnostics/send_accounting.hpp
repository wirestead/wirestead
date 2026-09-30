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
#include <deque>
#include <memory>
#include <mutex>

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

// Every critical section below is O(1) or O(batch), yet the admitting thread
// and the executor each take it per message or batch. Sleeping on contention
// costs a futex round trip on both sides, far longer than the wait itself.
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
    const auto id = ++next_id_;
    if (group && group->epoch != epoch_) {
      group->totals = {};
      group->epoch = epoch_;
    }
    if (entries_.empty()) first_id_ = id;
    const auto& entry = entries_.emplace_back(Entry{bytes, epoch_, false, true, std::move(group)});
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
      if (!find(request_of(item))) return false;
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
    auto* entry = find(id);
    if (!entry) return;
    terminate(*entry, cause);
    erase(*entry);
  }

  bool contains(Request id) const {
    std::lock_guard<BriefMutex> lock(mutex_);
    return find(id) != nullptr;
  }

  // Expiry races with handoff under the caller's admission mutex. Active
  // operations keep their actual outcome; unrelated contributors are untouched.
  void discard_waiting(const GroupHandle& group, Cause cause) {
    if (!group) return;
    std::lock_guard<BriefMutex> lock(mutex_);
    for (auto& entry : entries_) {
      if (entry.live && entry.group == group && !entry.active) {
        terminate(entry, cause);
        entry.live = false;
        entry.group.reset();
      }
    }
    trim();
  }

  // The caller serializes this boundary with admission. Clearing every entry
  // includes requests accepted before their enqueue handler has executed.
  void end(Cause cause) {
    std::lock_guard<BriefMutex> lock(mutex_);
    for (const auto& entry : entries_)
      if (entry.live) terminate(entry, cause);
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
    auto* entry = find(id);
    if (!entry) return false;
    entry->active = true;
    return true;
  }

  void complete_locked(Request id, size_t confirmed_bytes) {
    auto* found = find(id);
    if (!found) return;
    const auto& entry = *found;
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
    erase(*found);
  }

  void rollback(Request id) {
    std::lock_guard<BriefMutex> lock(mutex_);
    auto* entry = find(id);
    if (!entry) return;
    update(*entry, [&](auto& totals) {
      remove(totals.accepted, entry->bytes);
      remove(totals.outstanding, entry->bytes);
    });
    erase(*entry);
  }

  struct Entry {
    size_t bytes;
    uint64_t epoch;
    bool active;
    bool live;
    GroupHandle group;
  };
  // Requests are admitted with consecutive IDs and leave in close to FIFO
  // order, so entries_[id - first_id_] replaces a hash lookup. Removed
  // entries stay as tombstones until they reach the front.
  Entry* find(Request id) {
    if (id < first_id_ || id - first_id_ >= entries_.size()) return nullptr;
    auto& entry = entries_[id - first_id_];
    return entry.live ? &entry : nullptr;
  }
  const Entry* find(Request id) const { return const_cast<SendAccountingLedger*>(this)->find(id); }
  void erase(Entry& entry) {
    entry.live = false;
    entry.group.reset();
    trim();
  }
  void trim() {
    while (!entries_.empty() && !entries_.front().live) {
      entries_.pop_front();
      ++first_id_;
    }
  }
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
  // The admission thread and the executor both take mutex_ per message or
  // batch; keep every critical section O(1) so neither sleeps on the other.
  std::deque<Entry> entries_;
  Request first_id_ = 1;
  Request next_id_ = 0;
  uint64_t epoch_ = 0;
  wrapper::SendAccounting totals_;
};

}  // namespace wirestead::diagnostics
