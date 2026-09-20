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
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "wirestead/diagnostics/logger.hpp"

// #449: a blocking send (Reliable-mode send()/send_blocking()/send_move()/
// send_shared()) called from inside a data/message callback deadlocks -
// clearing backpressure requires progress on the same io thread that a
// blocking wait would now be stuck on. This thread_local flag, set for the
// duration of any data/message callback dispatch, lets the blocking-send
// path detect that scenario and fail fast (return false) instead of
// blocking forever. It intentionally isn't scoped per-channel: if the
// current thread is inside ANY callback dispatch, that thread can't make
// progress on I/O regardless of which channel triggered the callback -
// including a second channel sharing the same io_context/thread.
namespace wirestead {
namespace wrapper {
namespace detail {

// A depth counter rather than a bool so that a nested/reentrant
// CallbackGuard on the same thread doesn't clear the flag out from under an
// outer guard that's still in scope: the inner guard's destructor would
// otherwise flip g_callback_depth back to "not in a callback" while the
// outer dispatch is still running, reopening the #449 deadlock this guard
// exists to prevent.
inline thread_local int g_callback_depth = 0;

class CallbackGuard {
 public:
  CallbackGuard() { ++g_callback_depth; }
  ~CallbackGuard() { --g_callback_depth; }
  CallbackGuard(const CallbackGuard&) = delete;
  CallbackGuard& operator=(const CallbackGuard&) = delete;
};

inline bool in_data_callback() { return g_callback_depth > 0; }

// Admission gate for one wrapper object's user callbacks (D-1 in
// docs/communication_contract_v0.10_decisions.md).
//
// Shutdown complete means "no user callback of this object is running, and
// none from that run will start". Counting running callbacks alone cannot
// answer that: a callback path that has already passed its liveness check can
// register itself after a stop() has looked at the count. So admission, the
// count and the closed flag are all decided under one mutex, and stop()
// closes the gate before it waits.
//
// Each run carries a generation. A callback admitted for an earlier run is
// refused after a restart, so a leftover handler cannot be counted against -
// or delivered during - the new run.
class CallbackGate {
 public:
  // Held for the duration of one callback. `admitted()` false means the gate
  // was closed, or the lease belongs to an earlier run: the caller must not
  // invoke the user callback.
  class Lease {
   public:
    Lease() = default;
    Lease(CallbackGate* gate, bool admitted) : gate_(admitted ? gate : nullptr) {}
    Lease(Lease&& other) noexcept : gate_(other.gate_) { other.gate_ = nullptr; }
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        release();
        gate_ = other.gate_;
        other.gate_ = nullptr;
      }
      return *this;
    }
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    ~Lease() { release(); }

    bool admitted() const { return gate_ != nullptr; }

   private:
    void release() {
      if (gate_ == nullptr) return;
      CallbackGate* gate = gate_;
      gate_ = nullptr;
      {
        std::lock_guard<std::mutex> lock(gate->mutex_);
        --gate->running_;
        auto it = std::find(gate->threads_.begin(), gate->threads_.end(), std::this_thread::get_id());
        if (it != gate->threads_.end()) gate->threads_.erase(it);
      }
      gate->idle_.notify_all();
    }

    CallbackGate* gate_ = nullptr;
  };

  // Admission and registration in one step: a callback that is admitted is
  // already counted, so no stop() can observe an empty gate and return while
  // this callback is about to run.
  Lease enter(uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || generation != generation_) return Lease(this, false);
    ++running_;
    threads_.push_back(std::this_thread::get_id());
    return Lease(this, true);
  }

  // Stops admitting. Callbacks already admitted keep running; wait_until_idle()
  // is what waits for them.
  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }

  // Opens the gate for a new run and returns that run's generation. Callbacks
  // left over from the previous run are refused by enter().
  uint64_t open_new_generation() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = false;
    return ++generation_;
  }

  // A restart that keeps the handlers it already has - an injected channel,
  // where the same handlers stay registered across runs - admits them again
  // without changing the generation.
  void reopen() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = false;
  }

  uint64_t generation() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return generation_;
  }

  // True when this thread is running a callback of this object: waiting for
  // the gate to drain from here would wait for itself.
  bool active_on_this_thread() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::find(threads_.begin(), threads_.end(), std::this_thread::get_id()) != threads_.end();
  }

  void wait_until_idle() {
    std::unique_lock<std::mutex> lock(mutex_);
    idle_.wait(lock, [this] { return running_ == 0; });
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable idle_;
  int running_ = 0;
  bool closed_ = false;
  uint64_t generation_ = 0;
  std::vector<std::thread::id> threads_;
};

// Invokes a user-supplied wrapper callback (on_data/on_message/on_connect/
// on_disconnect/on_error/on_backpressure and their batch variants) and
// prevents an exception escaping it from propagating further. All channels
// share one process-wide IoContextManager thread by default
// (concurrency/io_context_manager.cc); an uncaught exception here would
// otherwise escape the un-guarded handler call, propagate out of
// io_context::run(), and stop I/O for every channel sharing that context -
// not just the one whose callback misbehaved.
template <typename Callback, typename... Args>
void invoke_user_callback(std::string_view component, std::string_view operation, const Callback& callback,
                          Args&&... args) {
  if (!callback) return;
  try {
    callback(std::forward<Args>(args)...);
  } catch (const std::exception& e) {
    WIRESTEAD_LOG_ERROR(component, operation, "Uncaught exception in user callback: " + std::string(e.what()));
  } catch (...) {
    WIRESTEAD_LOG_ERROR(component, operation, "Uncaught non-standard exception in user callback");
  }
}

// Overload for a handler snapshotted as a shared pointer rather than copied
// out of its guarded member - see interface::SharedCallback. A null pointer
// means "not registered", exactly as an empty std::function does above.
template <typename Callback, typename... Args>
void invoke_user_callback(std::string_view component, std::string_view operation,
                          const std::shared_ptr<const Callback>& callback, Args&&... args) {
  if (!callback) return;
  invoke_user_callback(component, operation, *callback, std::forward<Args>(args)...);
}

}  // namespace detail
}  // namespace wrapper
}  // namespace wirestead
