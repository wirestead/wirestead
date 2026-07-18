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
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "wirestead/base/common.hpp"

namespace wirestead {
namespace concurrency {

/**
 * @brief Thread-safe state management class
 *
 * Provides thread-safe state management with read-write lock semantics.
 * Multiple readers can access the state simultaneously, but only one writer
 * can modify the state at a time.
 */
template <typename StateType>
class ThreadSafeState {
 public:
  using State = StateType;
  using StateCallback = std::function<void(const State&)>;
  using StateCallbackHandle = size_t;

  // Constructors
  explicit ThreadSafeState(const State& initial_state = State{});
  ThreadSafeState(const ThreadSafeState&) = delete;
  ThreadSafeState& operator=(const ThreadSafeState&) = delete;
  ThreadSafeState(ThreadSafeState&&) = delete;
  ThreadSafeState& operator=(ThreadSafeState&&) = delete;

  // State access methods
  State state() const;
  void set_state(const State& new_state);
  void set_state(State&& new_state);

  // Atomic state operations
  bool compare_and_set(const State& expected, const State& desired);
  State exchange(const State& new_state);

  // State change notifications
  StateCallbackHandle add_state_change_callback(StateCallback callback);
  void remove_state_change_callback(StateCallbackHandle handle);
  void clear_state_change_callbacks();

  // Wait for state change
  void wait_for_state(const State& expected_state, std::chrono::milliseconds timeout = std::chrono::milliseconds(1000));
  void wait_for_state_change(std::chrono::milliseconds timeout = std::chrono::milliseconds(1000));

  // Utility methods
  bool is_state(const State& expected_state) const;
  void notify_state_change();

 private:
  mutable std::shared_mutex state_mutex_;
  State state_;
  std::atomic<bool> state_changed_{false};

  struct CallbackInfo {
    StateCallbackHandle handle;
    StateCallback callback;
  };
  std::vector<CallbackInfo> callbacks_;
  StateCallbackHandle next_handle_{1};
  mutable std::mutex callbacks_mutex_;

  std::condition_variable_any state_cv_;

  void notify_callbacks(const State& new_state);
};

/**
 * @brief Thread-safe atomic state wrapper
 *
 * Lightweight wrapper for atomic state management when full thread-safe
 * state management is not needed.
 */
template <typename StateType>
class AtomicState {
 public:
  using State = StateType;

  explicit AtomicState(const State& initial_state = State{});

  State get() const noexcept;
  void set(const State& new_state) noexcept;
  void set(State&& new_state) noexcept;

  bool compare_and_set(const State& expected, const State& desired) noexcept;
  State exchange(const State& new_state) noexcept;

  bool is_state(const State& expected_state) const noexcept;

 private:
  std::atomic<State> state_;
};

/**
 * @brief Thread-safe counter with atomic operations
 */
class ThreadSafeCounter {
 public:
  explicit ThreadSafeCounter(int64_t initial_value = 0);

  int64_t get() const noexcept;
  int64_t increment() noexcept;
  int64_t decrement() noexcept;
  int64_t add(int64_t value) noexcept;
  int64_t subtract(int64_t value) noexcept;

  bool compare_and_set(int64_t expected, int64_t desired) noexcept;
  int64_t exchange(int64_t new_value) noexcept;

  void reset() noexcept;

 private:
  std::atomic<int64_t> value_;
};

/**
 * @brief Thread-safe flag with atomic operations
 */
class ThreadSafeFlag {
 public:
  explicit ThreadSafeFlag(bool initial_value = false);

  bool get() const noexcept;
  void set(bool value = true) noexcept;
  void clear() noexcept;

  bool test_and_set() noexcept;
  bool compare_and_set(bool expected, bool desired) noexcept;

  void wait_for_true(std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) const;
  void wait_for_false(std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) const;

 private:
  std::atomic<bool> flag_;
  mutable std::condition_variable cv_;
  mutable std::mutex cv_mutex_;
};

// Specialization for LinkState
using ThreadSafeLinkState = ThreadSafeState<base::LinkState>;
using AtomicLinkState = AtomicState<base::LinkState>;

// Template implementations (must be in header for template instantiation)
template <typename StateType>
ThreadSafeState<StateType>::ThreadSafeState(const State& initial_state) : state_(initial_state) {}

template <typename StateType>
StateType ThreadSafeState<StateType>::state() const {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  return state_;
}

template <typename StateType>
void ThreadSafeState<StateType>::set_state(const State& new_state) {
  {
    std::unique_lock<std::shared_mutex> lock(state_mutex_);
    state_ = new_state;
    state_changed_.store(true);
  }
  notify_callbacks(new_state);
  state_cv_.notify_all();
}

template <typename StateType>
void ThreadSafeState<StateType>::set_state(State&& new_state) {
  {
    std::unique_lock<std::shared_mutex> lock(state_mutex_);
    state_ = std::move(new_state);
    state_changed_.store(true);
  }
  notify_callbacks(state_);
  state_cv_.notify_all();
}

template <typename StateType>
bool ThreadSafeState<StateType>::compare_and_set(const State& expected, const State& desired) {
  std::unique_lock<std::shared_mutex> lock(state_mutex_);
  if (state_ == expected) {
    state_ = desired;
    state_changed_.store(true);
    lock.unlock();
    notify_callbacks(desired);
    state_cv_.notify_all();
    return true;
  }
  return false;
}

template <typename StateType>
StateType ThreadSafeState<StateType>::exchange(const State& new_state) {
  State old_state;
  {
    std::unique_lock<std::shared_mutex> lock(state_mutex_);
    old_state = state_;
    state_ = new_state;
    state_changed_.store(true);
  }
  notify_callbacks(new_state);
  state_cv_.notify_all();
  return old_state;
}

template <typename StateType>
typename ThreadSafeState<StateType>::StateCallbackHandle ThreadSafeState<StateType>::add_state_change_callback(
    StateCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  StateCallbackHandle handle = next_handle_++;
  callbacks_.push_back({handle, std::move(callback)});
  return handle;
}

template <typename StateType>
void ThreadSafeState<StateType>::remove_state_change_callback(StateCallbackHandle handle) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  callbacks_.erase(std::remove_if(callbacks_.begin(), callbacks_.end(),
                                  [handle](const CallbackInfo& info) { return info.handle == handle; }),
                   callbacks_.end());
}

template <typename StateType>
void ThreadSafeState<StateType>::clear_state_change_callbacks() {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  callbacks_.clear();
}

template <typename StateType>
void ThreadSafeState<StateType>::wait_for_state(const State& expected_state, std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(state_mutex_);
  state_cv_.wait_for(lock, timeout, [this, &expected_state] { return state_ == expected_state; });
}

template <typename StateType>
void ThreadSafeState<StateType>::wait_for_state_change(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(state_mutex_);
  state_cv_.wait_for(lock, timeout, [this] { return state_changed_.load(); });
  state_changed_.store(false);
}

template <typename StateType>
bool ThreadSafeState<StateType>::is_state(const State& expected_state) const {
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
  return state_ == expected_state;
}

template <typename StateType>
void ThreadSafeState<StateType>::notify_state_change() {
  state_cv_.notify_all();
}

template <typename StateType>
void ThreadSafeState<StateType>::notify_callbacks(const State& new_state) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  for (const auto& info : callbacks_) {
    try {
      info.callback(new_state);
    } catch (...) {
      // Ignore callback exceptions to prevent state corruption
    }
  }
}

// AtomicState template implementations
template <typename StateType>
AtomicState<StateType>::AtomicState(const State& initial_state) : state_(initial_state) {}

template <typename StateType>
StateType AtomicState<StateType>::get() const noexcept {
  return state_.load();
}

template <typename StateType>
void AtomicState<StateType>::set(const State& new_state) noexcept {
  state_.store(new_state);
}

template <typename StateType>
void AtomicState<StateType>::set(State&& new_state) noexcept {
  state_.store(new_state);
}

template <typename StateType>
bool AtomicState<StateType>::compare_and_set(const State& expected, const State& desired) noexcept {
  State expected_copy = expected;
  return state_.compare_exchange_strong(expected_copy, desired);
}

template <typename StateType>
StateType AtomicState<StateType>::exchange(const State& new_state) noexcept {
  return state_.exchange(new_state);
}

template <typename StateType>
bool AtomicState<StateType>::is_state(const State& expected_state) const noexcept {
  return state_.load() == expected_state;
}

// ThreadSafeCounter implementations
inline ThreadSafeCounter::ThreadSafeCounter(int64_t initial_value) : value_(initial_value) {}

inline int64_t ThreadSafeCounter::get() const noexcept { return value_.load(); }

inline int64_t ThreadSafeCounter::increment() noexcept { return value_.fetch_add(1) + 1; }

inline int64_t ThreadSafeCounter::decrement() noexcept { return value_.fetch_sub(1) - 1; }

inline int64_t ThreadSafeCounter::add(int64_t value) noexcept { return value_.fetch_add(value) + value; }

inline int64_t ThreadSafeCounter::subtract(int64_t value) noexcept { return value_.fetch_sub(value) - value; }

inline bool ThreadSafeCounter::compare_and_set(int64_t expected, int64_t desired) noexcept {
  return value_.compare_exchange_strong(expected, desired);
}

inline int64_t ThreadSafeCounter::exchange(int64_t new_value) noexcept { return value_.exchange(new_value); }

inline void ThreadSafeCounter::reset() noexcept { value_.store(0); }

// ThreadSafeFlag implementations
inline ThreadSafeFlag::ThreadSafeFlag(bool initial_value) : flag_(initial_value) {}

inline bool ThreadSafeFlag::get() const noexcept { return flag_.load(); }

inline void ThreadSafeFlag::set(bool value) noexcept {
  flag_.store(value);
  if (value) {
    cv_.notify_all();
  }
}

inline void ThreadSafeFlag::clear() noexcept { flag_.store(false); }

inline bool ThreadSafeFlag::test_and_set() noexcept { return flag_.exchange(true); }

inline bool ThreadSafeFlag::compare_and_set(bool expected, bool desired) noexcept {
  return flag_.compare_exchange_strong(expected, desired);
}

inline void ThreadSafeFlag::wait_for_true(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(cv_mutex_);
  cv_.wait_for(lock, timeout, [this] { return flag_.load(); });
}

inline void ThreadSafeFlag::wait_for_false(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(cv_mutex_);
  cv_.wait_for(lock, timeout, [this] { return !flag_.load(); });
}

}  // namespace concurrency
}  // namespace wirestead
