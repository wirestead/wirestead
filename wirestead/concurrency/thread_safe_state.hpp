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

#include <atomic>

#include "wirestead/base/common.hpp"

namespace wirestead {
namespace concurrency {

/**
 * @brief Thread-safe atomic state wrapper
 *
 * Lightweight wrapper for atomic state management.
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

// Specialization for LinkState
using AtomicLinkState = AtomicState<base::LinkState>;

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

}  // namespace concurrency
}  // namespace wirestead
