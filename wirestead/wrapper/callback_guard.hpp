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

// Counts the user callbacks of one wrapper object that are currently running,
// so stop() can wait for "no user callback of this object is running" - part
// of what the v0.10 contract calls shutdown complete
// (docs/communication_contract_v0.10_decisions.md, D-1).
//
// Joining the io thread answers this only when the object owns that thread. On
// an externally run io_context the callbacks run on threads the library never
// joins, so a stop() there has to wait for this count instead.
//
// A wait from inside one of the object's own callbacks would wait for itself;
// callers check that first (Scope::is_active_on_this_thread()) and take the
// request-only path.
class ActiveCallbacks {
 public:
  class Scope {
   public:
    explicit Scope(ActiveCallbacks& owner) : owner_(owner) {
      std::lock_guard<std::mutex> lock(owner_.mutex_);
      ++owner_.running_;
      owner_.threads_.push_back(std::this_thread::get_id());
    }
    ~Scope() {
      {
        std::lock_guard<std::mutex> lock(owner_.mutex_);
        --owner_.running_;
        auto it = std::find(owner_.threads_.begin(), owner_.threads_.end(), std::this_thread::get_id());
        if (it != owner_.threads_.end()) owner_.threads_.erase(it);
      }
      owner_.idle_.notify_all();
    }
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

   private:
    ActiveCallbacks& owner_;
  };

  // True when this thread is currently running a callback of this object, in
  // which case waiting for the count to reach zero would deadlock.
  bool is_active_on_this_thread() const {
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
