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
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "wirestead/diagnostics/logger.hpp"

// D-2: a blocking send must not wait for capacity while the calling thread
// executes any user callback, including callbacks from another channel.
// The common invocation below marks callback scope; capacity available sends
// still follow their ordinary acceptance rules. Existing receive-path guards
// remain compatible because this counter supports nested scopes.
namespace wirestead {
namespace wrapper {
class SendResult;
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

// Historical internal name; covers every user-callback kind (D-2).
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
// Testing seam: called at the point the admission race lives - after a
// handler has checked that its object is alive, before it registers itself -
// so a test can park a callback exactly there instead of waiting for that
// window to happen by chance. Nothing in the library ever sets it.
using PreAdmissionHook = void (*)();
inline std::atomic<PreAdmissionHook> g_pre_admission_hook{nullptr};
// Tests may park a TCP capacity waiter before registering its timed wait.
inline std::atomic<PreAdmissionHook> g_udp_capacity_wait_hook{nullptr};
inline std::atomic<void (*)(const SendResult&)> g_udp_capacity_wait_result_hook{nullptr};
inline std::atomic<void (*)(const SendResult&)> g_udp_send_result_hook{nullptr};
inline std::atomic<void (*)(const SendResult&)> g_udp_server_send_result_hook{nullptr};
inline std::atomic<PreAdmissionHook> g_uds_capacity_wait_hook{nullptr};
inline std::atomic<void (*)(const SendResult&)> g_uds_capacity_wait_result_hook{nullptr};
inline std::atomic<void (*)(const SendResult&)> g_uds_send_result_hook{nullptr};
inline std::atomic<PreAdmissionHook> g_tcp_capacity_wait_hook{nullptr};
// Observes the frozen internal outcome after a capacity wait has ended.
inline std::atomic<void (*)(const SendResult&)> g_tcp_capacity_wait_result_hook{nullptr};

// Observes built-in TCP wrapper send outcomes at the bool boundary.
inline std::atomic<void (*)(const SendResult&)> g_tcp_send_result_hook{nullptr};

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
    if (auto hook = g_pre_admission_hook.load(std::memory_order_acquire)) hook();
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

  bool idle() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_ == 0;
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

// Invokes every wrapper callback under the D-2 nonwaiting-send guard and
// contains user exceptions so they cannot escape io_context::run(). The guard
// is restored before exception logging, including for nested callbacks.
template <typename Callback, typename... Args>
void invoke_user_callback(std::string_view component, std::string_view operation, const Callback& callback,
                          Args&&... args) {
  if (!callback) return;
  try {
    CallbackGuard guard;
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
