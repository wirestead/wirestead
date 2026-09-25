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
#include <mutex>
#include <unordered_map>

#include "wirestead/wrapper/send_accounting.hpp"

namespace wirestead::diagnostics {

// Shared by admission threads and the transport executor. IDs never repeat
// across reset; old completions cannot change a replacement measurement epoch.
class SendAccountingLedger {
 public:
  using Request = uint64_t;
  enum class Cause { ExplicitStop, ConnectionLoss, QueuePressure };

  Request admit(size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto id = ++next_id_;
    entries_.emplace(id, Entry{bytes, epoch_, false});
    add(totals_.accepted, bytes);
    add(totals_.outstanding, bytes);
    return id;
  }

  // Roll back tracking if posting throws before the API returns acceptance.
  class Admission {
   public:
    Admission(SendAccountingLedger& ledger, size_t bytes) : ledger_(ledger), request_(ledger.admit(bytes)) {}
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
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return false;
    it->second.active = true;
    return true;
  }

  // One composed-operation completion supplies the prefix length for each
  // request. A fully covered request is written even if a later request fails.
  void complete(Request id, size_t confirmed_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    const auto entry = it->second;
    if (entry.epoch == epoch_) {
      const auto confirmed = std::min(confirmed_bytes, entry.bytes);
      totals_.confirmed_written_bytes += confirmed;
      remove(totals_.outstanding, entry.bytes);
      if (confirmed == entry.bytes) {
        add(totals_.written, entry.bytes);
      } else {
        add(totals_.connection_loss.aborted_during_write, entry.bytes);
      }
    }
    entries_.erase(it);
  }

  void discard(Request id, Cause cause) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    terminate(it->second, cause);
    entries_.erase(it);
  }

  // The caller serializes this boundary with admission. Clearing every entry
  // includes requests accepted before their enqueue handler has executed.
  void end(Cause cause) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, entry] : entries_) terminate(entry, cause);
    entries_.clear();
  }

  wrapper::SendAccounting snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return totals_;
  }

  // A measurement epoch includes only requests accepted since reset. Retained
  // old requests still transmit but their later completions/cleanup are ignored.
  void reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++epoch_;
    totals_ = {};
  }

 private:
  void rollback(Request id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) return;
    if (it->second.epoch == epoch_) {
      remove(totals_.accepted, it->second.bytes);
      remove(totals_.outstanding, it->second.bytes);
    }
    entries_.erase(it);
  }

  struct Entry {
    size_t bytes;
    uint64_t epoch;
    bool active;
  };
  static void add(wrapper::SendRequestTotals& totals, size_t bytes) {
    ++totals.requests;
    totals.bytes += bytes;
  }
  static void remove(wrapper::SendRequestTotals& totals, size_t bytes) {
    --totals.requests;
    totals.bytes -= bytes;
  }
  void terminate(const Entry& entry, Cause cause) {
    if (entry.epoch != epoch_) return;
    auto& loss = cause == Cause::ExplicitStop     ? totals_.explicit_stop
                 : cause == Cause::ConnectionLoss ? totals_.connection_loss
                                                  : totals_.queue_pressure;
    add(entry.active ? loss.aborted_during_write : loss.discarded_before_write, entry.bytes);
    remove(totals_.outstanding, entry.bytes);
  }

  mutable std::mutex mutex_;
  std::unordered_map<Request, Entry> entries_;
  Request next_id_ = 0;
  uint64_t epoch_ = 0;
  wrapper::SendAccounting totals_;
};

}  // namespace wirestead::diagnostics
