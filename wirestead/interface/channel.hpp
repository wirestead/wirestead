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
#include <boost/asio/any_io_executor.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/wrapper/runtime_stats.hpp"

namespace wirestead {
namespace interface {
class WIRESTEAD_API Channel {
 public:
  using OnBytes = std::function<void(memory::ConstByteSpan)>;
  using OnState = std::function<void(base::LinkState)>;
  using OnBackpressure = std::function<void(size_t /*queued_bytes*/)>;

  virtual ~Channel();

  virtual void start() = 0;
  virtual void stop() = 0;
  virtual bool is_connected() const = 0;
  virtual bool is_backpressure_active() const = 0;

  // Hard byte limit of the entire write queue, not its currently free space.
  // nullopt means unavailable (including custom channels that do not report it).
  // A reported limit must match write admission; it is not an acceptance promise.
  virtual std::optional<size_t> write_queue_limit() const { return std::nullopt; }
  virtual wrapper::RuntimeStats stats() const { return {}; }
  virtual void reset_stats() {}

  // Most recent error recorded by this channel, if any. Default no-op
  // override (std::nullopt) so any transport not yet wired up to the
  // shared ErrorInfoHolder plumbing (#445) just reports "no detail
  // available" instead of failing to compile or requiring a stub
  // override everywhere.
  virtual std::optional<diagnostics::ErrorInfo> last_error_info() const { return std::nullopt; }

  virtual boost::asio::any_io_executor get_executor() = 0;

  // Single send API (copies into internal queue)
  virtual bool async_write_copy(memory::ConstByteSpan data) = 0;
  // Zero-copy APIs (ownership transfer or shared ownership)
  virtual bool async_write_move(std::vector<uint8_t>&& data) = 0;
  virtual bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) = 0;

  // Explicit non-blocking drop-if-full send APIs. These must not enqueue into
  // Reliable pending queues when backpressure is already active.
  virtual bool async_try_write_copy(memory::ConstByteSpan data) = 0;
  virtual bool async_try_write_move(std::vector<uint8_t>&& data) = 0;
  virtual bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) = 0;

  // Callbacks. Thread-safe: may be called at any time, including after
  // start(), from any thread. Replacing a callback is synchronized against
  // concurrent invocation on the io thread - the implementation either
  // guards storage with a mutex (copy-under-lock, invoke outside the lock)
  // or dispatches the assignment onto the same strand the io thread runs
  // on. Re-registering a callback takes effect for subsequent events; it
  // does not retroactively affect an invocation already in progress (#436).
  virtual void on_bytes(OnBytes cb) = 0;
  virtual void on_state(OnState cb) = 0;
  virtual void on_backpressure(OnBackpressure cb) = 0;
};

// Snapshot type for a callback an implementation hands out to its io thread.
//
// The storage discipline above is unchanged - the mutex still guards the
// member, and the callback is still invoked outside the lock. What changes is
// the cost of the snapshot: copying a std::function heap-allocates whenever
// the target does not fit its small-object buffer, and the receive path takes
// one such copy per received chunk. Sharing an immutable copy turns that into
// a refcount bump. The pointed-to callback is const, so a snapshot taken by
// the io thread stays valid and unchanged even while a setter installs a
// replacement, which is the same guarantee the copy provided (#436).
template <typename Callback>
using SharedCallback = std::shared_ptr<const Callback>;

// Returns null for an empty callback, so a non-null SharedCallback always
// holds something invocable and callers need only the pointer check.
// Call this outside the lock - it allocates.
template <typename Callback>
SharedCallback<Callback> share_callback(Callback cb) {
  if (!cb) return {};
  return std::make_shared<const Callback>(std::move(cb));
}
}  // namespace interface
}  // namespace wirestead
