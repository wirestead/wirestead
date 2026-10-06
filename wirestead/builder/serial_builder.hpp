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
class WIRESTEAD_API SerialBuilder : public BuilderInterface<wrapper::Serial, SerialBuilder> {
 public:
  SerialBuilder(const std::string& device, uint32_t baud_rate);

  // Delete copy
  SerialBuilder(const SerialBuilder&) = delete;
  SerialBuilder& operator=(const SerialBuilder&) = delete;

  std::unique_ptr<wrapper::Serial> build() override;

  SerialBuilder& auto_start(bool auto_start = true) override;
  SerialBuilder& char_size(unsigned int size);
  SerialBuilder& data_bits(unsigned int size) { return char_size(size); }
  SerialBuilder& stop_bits(unsigned int bits);
  SerialBuilder& parity(config::SerialConfig::Parity p);
  SerialBuilder& parity(const std::string& p);
  SerialBuilder& flow_control(config::SerialConfig::Flow f);
  SerialBuilder& flow_control(const std::string& f);
  // Bytes each read fills. The TCP and UDS builders call the same knob
  // read_buffer_size(); serial named it read_chunk before those existed.
  SerialBuilder& read_chunk(size_t bytes);
  // Ask the driver to deliver received bytes without waiting out its own
  // buffering timer (Linux ASYNC_LOW_LATENCY). On by default; see
  // SerialConfig::low_latency.
  SerialBuilder& low_latency(bool enable = true);
  // Half-duplex RS-485. Delays are milliseconds; see SerialConfig::Rs485.
  SerialBuilder& rs485(bool rts_on_send = true, bool rx_during_tx = false, unsigned delay_before_ms = 0,
                       unsigned delay_after_ms = 0);
  // Assert or clear DTR / RTS once the port is open. Not calling these leaves
  // the driver's default alone, which differs from passing false. Neither can
  // prevent a DTR-triggered board reset at open (#710).
  SerialBuilder& dtr(bool assert_line);
  SerialBuilder& rts(bool assert_line);
  SerialBuilder& reopen_on_error(bool enable = true);
  // Reopen the port when no data has been received for this long. Off by
  // default; see SerialConfig::rx_idle_timeout_ms.
  SerialBuilder& rx_idle_timeout(std::chrono::milliseconds timeout);
  SerialBuilder& retry_interval(std::chrono::milliseconds interval);
  SerialBuilder& independent_context(bool use_independent = true);
  // Opt into the shared IoContextManager singleton instead of the default
  // dedicated io_context + thread (#440). Only meaningful for deliberately
  // trading per-instance parallelism for reduced thread/memory overhead
  // across many instances in one process; most callers should not need this.
  SerialBuilder& shared_context(bool use_shared = true);

 private:
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
  size_t read_chunk_{base::constants::DEFAULT_READ_BUFFER_SIZE};
  bool read_chunk_set_{false};
  bool low_latency_{true};
  bool low_latency_set_{false};
  config::SerialConfig::Rs485 rs485_{};
  std::optional<bool> dtr_;
  std::optional<bool> rts_;
  std::chrono::milliseconds rx_idle_timeout_{0};
  bool rx_idle_timeout_set_{false};
};

using SerialBuilderDefault = SerialBuilder;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
