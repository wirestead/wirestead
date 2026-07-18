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

#include <chrono>
#include <cstdint>
#include <string>

#include "wirestead/base/visibility.hpp"
#include "wirestead/builder/ibuilder.hpp"
#include "wirestead/wrapper/serial/serial.hpp"

namespace wirestead {
namespace builder {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

/**
 * @brief Modernized Builder for Serial
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API SerialBuilder : public BuilderInterface<wrapper::Serial, SerialBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = SerialBuilder<NewState>;

  SerialBuilder(const std::string& device, uint32_t baud_rate);

  // Allow conversion between states
  template <uint32_t OtherState>
  SerialBuilder(SerialBuilder<OtherState>&& other) noexcept
      : device_(std::move(other.device_)),
        baud_rate_(other.baud_rate_),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        shared_context_(other.shared_context_),
        retry_interval_ms_(other.retry_interval_ms_),
        retry_interval_set_(other.retry_interval_set_),
        char_size_(other.char_size_),
        char_size_set_(other.char_size_set_),
        stop_bits_(other.stop_bits_),
        stop_bits_set_(other.stop_bits_set_),
        parity_(other.parity_),
        parity_set_(other.parity_set_),
        flow_(other.flow_),
        flow_set_(other.flow_set_),
        reopen_on_error_(other.reopen_on_error_),
        reopen_on_error_set_(other.reopen_on_error_set_) {
    this->on_data_ = std::move(other.on_data_);
    this->on_error_ = std::move(other.on_error_);
    this->on_connect_ = std::move(other.on_connect_);
    this->on_disconnect_ = std::move(other.on_disconnect_);
    this->on_message_ = std::move(other.on_message_);
    this->on_data_batch_ = std::move(other.on_data_batch_);
    this->on_message_batch_ = std::move(other.on_message_batch_);
    this->on_backpressure_ = std::move(other.on_backpressure_);
    this->framer_factory_ = std::move(other.framer_factory_);
    this->bp_strategy_ = other.bp_strategy_;
    this->bp_threshold_ = other.bp_threshold_;
    this->bp_strategy_set_ = other.bp_strategy_set_;
    this->bp_threshold_set_ = other.bp_threshold_set_;
  }

  // Delete copy
  SerialBuilder(const SerialBuilder&) = delete;
  SerialBuilder& operator=(const SerialBuilder&) = delete;

  std::unique_ptr<wrapper::Serial> build() override;

  SerialBuilder<State>& auto_start(bool auto_start = true) override;
  SerialBuilder<State>& char_size(unsigned int size);
  SerialBuilder<State>& data_bits(unsigned int size) { return char_size(size); }
  SerialBuilder<State>& stop_bits(unsigned int bits);
  SerialBuilder<State>& parity(config::SerialConfig::Parity p);
  SerialBuilder<State>& parity(const std::string& p);
  SerialBuilder<State>& flow_control(config::SerialConfig::Flow f);
  SerialBuilder<State>& flow_control(const std::string& f);
  SerialBuilder<State>& reopen_on_error(bool enable = true);
  SerialBuilder<State>& retry_interval(std::chrono::milliseconds interval);
  SerialBuilder<State>& independent_context(bool use_independent = true);
  // Opt into the shared IoContextManager singleton instead of the default
  // dedicated io_context + thread (#440). Only meaningful for deliberately
  // trading per-instance parallelism for reduced thread/memory overhead
  // across many instances in one process; most callers should not need this.
  SerialBuilder<State>& shared_context(bool use_shared = true);

 private:
  template <uint32_t S>
  friend class SerialBuilder;

  std::string device_;
  uint32_t baud_rate_;
  bool auto_start_;
  bool independent_context_;
  bool shared_context_{false};

  uint32_t retry_interval_ms_;
  bool retry_interval_set_{false};
  unsigned int char_size_;
  bool char_size_set_{false};
  unsigned int stop_bits_;
  bool stop_bits_set_{false};
  config::SerialConfig::Parity parity_;
  bool parity_set_{false};
  config::SerialConfig::Flow flow_;
  bool flow_set_{false};
  bool reopen_on_error_;
  bool reopen_on_error_set_{false};
};

using SerialBuilderDefault = SerialBuilder<BuilderState::None>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
