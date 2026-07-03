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
#include <utility>
#include <vector>

#include "unilink/base/common.hpp"
#include "unilink/base/visibility.hpp"
#include "unilink/memory/safe_span.hpp"
#include "unilink/wrapper/runtime_stats.hpp"

namespace unilink {
namespace interface {
class UNILINK_API Channel {
 public:
  using OnBytes = std::function<void(memory::ConstByteSpan)>;
  using OnState = std::function<void(base::LinkState)>;
  using OnBackpressure = std::function<void(size_t /*queued_bytes*/)>;

  virtual ~Channel();

  virtual void start() = 0;
  virtual void stop() = 0;
  virtual bool is_connected() const = 0;
  virtual bool is_backpressure_active() const = 0;
  virtual wrapper::RuntimeStats stats() const { return {}; }
  virtual void reset_stats() {}

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
}  // namespace interface
}  // namespace unilink
