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
namespace unilink {
namespace wrapper {
namespace detail {

inline thread_local bool g_in_data_callback = false;

class CallbackGuard {
 public:
  CallbackGuard() { g_in_data_callback = true; }
  ~CallbackGuard() { g_in_data_callback = false; }
  CallbackGuard(const CallbackGuard&) = delete;
  CallbackGuard& operator=(const CallbackGuard&) = delete;
};

inline bool in_data_callback() { return g_in_data_callback; }

}  // namespace detail
}  // namespace wrapper
}  // namespace unilink
