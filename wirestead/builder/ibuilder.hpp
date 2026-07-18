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

#include <concepts>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "wirestead/base/constants.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/framer/iframer.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/framer/packet_framer.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/wrapper/context.hpp"

namespace wirestead {
namespace builder {

/**
 * @brief Concepts for callback signature validation
 */
template <typename T>
concept DataHandler = std::invocable<T, const wrapper::MessageContext&>;

template <typename T>
concept BatchDataHandler = std::invocable<T, const std::vector<wrapper::MessageContext>&>;

template <typename T>
concept ErrorHandler = std::invocable<T, const wrapper::ErrorContext&>;

template <typename T>
concept ConnectionHandler = std::invocable<T, const wrapper::ConnectionContext&>;

/**
 * @brief Builder state bitmask for callback registration tracking.
 *
 * Callback registration is optional. The state is retained for fluent CRTP
 * rebinding and future extension points, not for mandatory build gating.
 */
enum BuilderState : uint32_t { None = 0, HasData = 1 << 0, HasError = 1 << 1, Ready = HasData | HasError };

/**
 * @brief Generic Builder interface for fluent API pattern
 */
template <typename T, typename Derived, uint32_t State = BuilderState::None>
class BuilderInterface {
 public:
  virtual ~BuilderInterface() = default;
  BuilderInterface() = default;
  BuilderInterface(const BuilderInterface&) = default;
  BuilderInterface(BuilderInterface&&) = default;
  BuilderInterface& operator=(const BuilderInterface&) = default;
  BuilderInterface& operator=(BuilderInterface&&) = default;

  /**
   * @brief Build and return the configured product
   */
  virtual std::unique_ptr<T> build() = 0;

  /**
   * @brief Enable auto-manage functionality
   */
  virtual Derived& auto_start(bool auto_start = true) = 0;

  /**
   * @brief Set data handler callback
   */
  template <DataHandler F>
  auto on_data(F&& handler) {
    on_data_ = std::forward<F>(handler);
    return typename Derived::template Rebind<State | BuilderState::HasData>(std::move(static_cast<Derived&>(*this)));
  }

  /**
   * @brief Set batched data handler callback
   */
  template <BatchDataHandler F>
  auto on_data_batch(F&& handler) {
    on_data_batch_ = std::forward<F>(handler);
    return typename Derived::template Rebind<State | BuilderState::HasData>(std::move(static_cast<Derived&>(*this)));
  }

  /**
   * @brief Set data handler callback using member function pointer
   */
  template <typename U, typename F>
  auto on_data(U* obj, F method) {
    return on_data([obj, method](const wrapper::MessageContext& ctx) { (obj->*method)(ctx); });
  }

  /**
   * @brief Set connection handler callback
   */
  template <ConnectionHandler F>
  Derived& on_connect(F&& handler) {
    on_connect_ = std::forward<F>(handler);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set connection handler callback using member function pointer
   */
  template <typename U, typename F>
  Derived& on_connect(U* obj, F method) {
    return on_connect([obj, method](const wrapper::ConnectionContext& ctx) { (obj->*method)(ctx); });
  }

  /**
   * @brief Set disconnection handler callback
   */
  template <ConnectionHandler F>
  Derived& on_disconnect(F&& handler) {
    on_disconnect_ = std::forward<F>(handler);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set disconnection handler callback using member function pointer
   */
  template <typename U, typename F>
  Derived& on_disconnect(U* obj, F method) {
    return on_disconnect([obj, method](const wrapper::ConnectionContext& ctx) { (obj->*method)(ctx); });
  }

  /**
   * @brief Set error handler callback
   */
  template <ErrorHandler F>
  auto on_error(F&& handler) {
    on_error_ = std::forward<F>(handler);
    return typename Derived::template Rebind<State | BuilderState::HasError>(std::move(static_cast<Derived&>(*this)));
  }

  /**
   * @brief Set error handler callback using member function pointer
   */
  template <typename U, typename F>
  auto on_error(U* obj, F method) {
    return on_error([obj, method](const wrapper::ErrorContext& ctx) { (obj->*method)(ctx); });
  }

  // Framing Support

  /**
   * @brief Set a custom framer factory
   */
  Derived& framer(std::function<std::unique_ptr<framer::IFramer>()> factory) {
    framer_factory_ = std::move(factory);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Activate line-delimited framing
   */
  Derived& use_line_framer(std::string_view delimiter = "\n", bool include_delimiter = false,
                           size_t max_length = 65536) {
    std::string delim(delimiter);
    framer_factory_ = [delim, include_delimiter, max_length]() {
      return std::make_unique<framer::LineFramer>(delim, include_delimiter, max_length);
    };
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Activate binary packet framing
   */
  Derived& use_packet_framer(const std::vector<uint8_t>& start_pattern, const std::vector<uint8_t>& end_pattern,
                             size_t max_length) {
    framer_factory_ = [start_pattern, end_pattern, max_length]() {
      return std::make_unique<framer::PacketFramer>(start_pattern, end_pattern, max_length);
    };
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set message handler callback
   */
  template <DataHandler F>
  auto on_message(F&& handler) {
    on_message_ = std::forward<F>(handler);
    return typename Derived::template Rebind<State | BuilderState::HasData>(std::move(static_cast<Derived&>(*this)));
  }

  /**
   * @brief Set batched framed-message handler callback
   */
  template <BatchDataHandler F>
  auto on_message_batch(F&& handler) {
    on_message_batch_ = std::forward<F>(handler);
    return typename Derived::template Rebind<State | BuilderState::HasData>(std::move(static_cast<Derived&>(*this)));
  }

  /**
   * @brief Set backpressure notification callback
   */
  Derived& on_backpressure(std::function<void(size_t)> handler) {
    on_backpressure_ = std::move(handler);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set message handler callback using member function pointer
   */
  template <typename U, typename F>
  auto on_message(U* obj, F method) {
    return on_message([obj, method](const wrapper::MessageContext& ctx) { (obj->*method)(ctx); });
  }

  /**
   * @brief Set the backpressure strategy
   */
  Derived& backpressure_strategy(base::constants::BackpressureStrategy strategy) {
    bp_strategy_ = strategy;
    bp_strategy_set_ = true;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set the backpressure threshold in bytes
   */
  Derived& backpressure_threshold(size_t threshold) {
    bp_threshold_ = threshold;
    bp_threshold_set_ = true;
    return static_cast<Derived&>(*this);
  }

 protected:
  size_t get_effective_backpressure_threshold() const {
    if (bp_threshold_set_) {
      return bp_threshold_;
    }
    if (bp_strategy_ == base::constants::BackpressureStrategy::BestEffort) {
      return base::constants::DEFAULT_THRESHOLD_BEST_EFFORT;
    }
    return base::constants::DEFAULT_THRESHOLD_RELIABLE;
  }

  std::function<std::unique_ptr<framer::IFramer>()> framer_factory_;
  std::function<void(const wrapper::MessageContext&)> on_data_;
  std::function<void(const wrapper::ConnectionContext&)> on_connect_;
  std::function<void(const wrapper::ConnectionContext&)> on_disconnect_;
  std::function<void(const wrapper::ErrorContext&)> on_error_;
  std::function<void(const wrapper::MessageContext&)> on_message_;
  std::function<void(const std::vector<wrapper::MessageContext>&)> on_data_batch_;
  std::function<void(const std::vector<wrapper::MessageContext>&)> on_message_batch_;
  std::function<void(size_t)> on_backpressure_;

  base::constants::BackpressureStrategy bp_strategy_{base::constants::BackpressureStrategy::Reliable};
  size_t bp_threshold_{0};
  bool bp_strategy_set_{false};
  bool bp_threshold_set_{false};
};

}  // namespace builder
}  // namespace wirestead
