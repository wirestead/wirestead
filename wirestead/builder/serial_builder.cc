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

#include "wirestead/builder/serial_builder.hpp"

#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <cctype>

#include "wirestead/base/constants.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/config/validation.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace builder {

SerialBuilder::SerialBuilder(const std::string& device, uint32_t baud_rate)
    : device_(device),
      baud_rate_(baud_rate),
      auto_start_(false),
      independent_context_(false),
      shared_context_(false),
      retry_interval_ms_(base::constants::DEFAULT_RETRY_INTERVAL_MS),
      char_size_(8),
      stop_bits_(1),
      parity_(config::SerialConfig::Parity::None),
      flow_(config::SerialConfig::Flow::None),
      reopen_on_error_(true) {
  if (device.empty()) throw diagnostics::BuilderException("Device path cannot be empty");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

std::unique_ptr<wrapper::Serial> SerialBuilder::build() {
  std::unique_ptr<wrapper::Serial> serial;
  if (independent_context_) {
    serial = std::make_unique<wrapper::Serial>(device_, baud_rate_, std::make_shared<boost::asio::io_context>());
    serial->manage_external_context(true);
  } else {
    serial = std::make_unique<wrapper::Serial>(device_, baud_rate_);
  }
  if (shared_context_) serial->shared_context(true);

  if (this->on_data_) serial->on_data(this->on_data_);
  if (this->on_data_batch_) serial->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) serial->on_connect(this->on_connect_);
  if (this->on_disconnect_) serial->on_disconnect(this->on_disconnect_);
  if (this->on_error_) serial->on_error(this->on_error_);
  if (this->on_backpressure_) serial->on_backpressure(this->on_backpressure_);

  if (char_size_set_) serial->data_bits(static_cast<int>(char_size_));
  if (stop_bits_set_) serial->stop_bits(static_cast<int>(stop_bits_));

  // Note: wrapper::Serial setters use strings for enum types
  if (parity_set_) {
    std::string p_str = "none";
    if (parity_ == config::SerialConfig::Parity::Even)
      p_str = "even";
    else if (parity_ == config::SerialConfig::Parity::Odd)
      p_str = "odd";
    serial->parity(p_str);
  }

  if (flow_set_) {
    std::string f_str = "none";
    if (flow_ == config::SerialConfig::Flow::Software)
      f_str = "software";
    else if (flow_ == config::SerialConfig::Flow::Hardware)
      f_str = "hardware";
    serial->flow_control(f_str);
  }

  if (reopen_on_error_set_) serial->reopen_on_error(reopen_on_error_);
  if (read_chunk_set_) serial->read_chunk(read_chunk_);
  if (low_latency_set_) serial->low_latency(low_latency_);
  if (rs485_.enabled) {
    serial->rs485(rs485_.rts_on_send, rs485_.rx_during_tx, rs485_.delay_rts_before_send_ms,
                  rs485_.delay_rts_after_send_ms);
  }
  if (dtr_) serial->dtr(*dtr_);
  if (rts_) serial->rts(*rts_);
  if (rx_idle_timeout_set_) serial->rx_idle_timeout(rx_idle_timeout_);
  if (retry_interval_set_) serial->retry_interval(std::chrono::milliseconds(retry_interval_ms_));

  if (this->bp_strategy_set_) serial->backpressure_strategy(this->bp_strategy_);
  serial->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    serial->framer(this->framer_factory_());
  }
  if (this->on_message_) {
    serial->on_message(this->on_message_);
  }
  if (this->on_message_batch_) {
    serial->on_message_batch(this->on_message_batch_);
  }

  if (auto_start_) {
    serial->auto_start(true);
  }

  return serial;
}

SerialBuilder& SerialBuilder::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

SerialBuilder& SerialBuilder::char_size(unsigned int size) {
  char_size_ = size;
  char_size_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::stop_bits(unsigned int bits) {
  config::detail::range(bits, base::constants::MIN_STOP_BITS, base::constants::MAX_STOP_BITS, "invalid stop_bits");
  stop_bits_ = bits;
  stop_bits_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::parity(config::SerialConfig::Parity p) {
  config::detail::require(p == config::SerialConfig::Parity::None || p == config::SerialConfig::Parity::Even ||
                              p == config::SerialConfig::Parity::Odd,
                          "invalid parity");
  parity_ = p;
  parity_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::parity(const std::string& p) {
  config::detail::text_option(p, {"none", "even", "odd"}, "invalid parity");
  std::string value = p;
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

  if (value == "even") {
    parity_ = config::SerialConfig::Parity::Even;
  } else if (value == "odd") {
    parity_ = config::SerialConfig::Parity::Odd;
  } else {
    parity_ = config::SerialConfig::Parity::None;
  }
  parity_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::flow_control(config::SerialConfig::Flow f) {
  config::detail::require(f == config::SerialConfig::Flow::None || f == config::SerialConfig::Flow::Software ||
                              f == config::SerialConfig::Flow::Hardware,
                          "invalid flow_control");
  flow_ = f;
  flow_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::flow_control(const std::string& f) {
  config::detail::text_option(f, {"none", "software", "hardware"}, "invalid flow control");
  std::string value = f;
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

  if (value == "software") {
    flow_ = config::SerialConfig::Flow::Software;
  } else if (value == "hardware") {
    flow_ = config::SerialConfig::Flow::Hardware;
  } else {
    flow_ = config::SerialConfig::Flow::None;
  }
  flow_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::read_chunk(size_t bytes) {
  config::detail::range(bytes, base::constants::MIN_READ_BUFFER_SIZE, base::constants::MAX_READ_BUFFER_SIZE,
                        "invalid read_chunk");
  read_chunk_ = bytes;
  read_chunk_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::low_latency(bool enable) {
  low_latency_ = enable;
  low_latency_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::rs485(bool rts_on_send, bool rx_during_tx, unsigned delay_before_ms,
                                    unsigned delay_after_ms) {
  rs485_.enabled = true;
  rs485_.rts_on_send = rts_on_send;
  rs485_.rx_during_tx = rx_during_tx;
  rs485_.delay_rts_before_send_ms = delay_before_ms;
  rs485_.delay_rts_after_send_ms = delay_after_ms;
  return *this;
}

SerialBuilder& SerialBuilder::dtr(bool assert_line) {
  dtr_ = assert_line;
  return *this;
}

SerialBuilder& SerialBuilder::rts(bool assert_line) {
  rts_ = assert_line;
  return *this;
}

SerialBuilder& SerialBuilder::rx_idle_timeout(std::chrono::milliseconds timeout) {
  config::detail::duration(timeout, base::constants::MIN_IDLE_TIMEOUT_MS, base::constants::MAX_IDLE_TIMEOUT_MS, true,
                           "invalid rx_idle_timeout");
  rx_idle_timeout_ = timeout;
  rx_idle_timeout_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::reopen_on_error(bool enable) {
  reopen_on_error_ = enable;
  reopen_on_error_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::retry_interval(std::chrono::milliseconds interval) {
  config::detail::duration(interval, base::constants::MIN_RETRY_INTERVAL_MS, base::constants::MAX_RETRY_INTERVAL_MS,
                           false, "invalid retry_interval");
  retry_interval_ms_ = static_cast<uint32_t>(interval.count());
  retry_interval_set_ = true;
  return *this;
}

SerialBuilder& SerialBuilder::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

SerialBuilder& SerialBuilder::shared_context(bool use_shared) {
  shared_context_ = use_shared;
  return *this;
}

// Explicit template instantiations

}  // namespace builder
}  // namespace wirestead
