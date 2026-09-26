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

#include <optional>
#include <string>

#include "wirestead/base/constants.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace config {

struct SerialConfig {
#ifdef _WIN32
  std::string device = "COM1";
#else
  std::string device = "/dev/ttyUSB0";
#endif
  unsigned baud_rate = 115200;
  unsigned char_size = 8;  // 5,6,7,8
  enum class Parity { None, Even, Odd } parity = Parity::None;
  unsigned stop_bits = 1;  // 1 or 2
  enum class Flow { None, Software, Hardware } flow = Flow::None;

  size_t read_chunk = base::constants::DEFAULT_READ_BUFFER_SIZE;

  // Ask the kernel driver to hand bytes up as soon as they arrive instead of
  // waiting out its buffering timer. On Linux this is ASYNC_LOW_LATENCY, and
  // it matters most on USB serial adapters: an FTDI defaults to a 16 ms
  // latency timer, so a 1 ms packet at 115200 baud can still reach the
  // callback 16 ms late — enough to break a control loop on its own.
  //
  // Best effort by design. Drivers that have no such timer (CDC-ACM, most
  // native UARTs) reject the request, and that is not an error: the port opens
  // and runs normally either way. Set false to leave the driver's default
  // alone, which trades latency for fewer wakeups.
  bool low_latency = true;

  // Half-duplex RS-485, the wiring behind Dynamixel servos, Modbus RTU and a
  // good deal of industrial sensing. The driver has to drive the transceiver's
  // direction pin around each write, which is not something a caller can do
  // from userspace at the right instant - hence a setting rather than advice.
  //
  // Linux only (TIOCSRS485). Adapters that switch direction in hardware need
  // none of this and will refuse it; that refusal is not an error.
  struct Rs485 {
    bool enabled = false;
    // Logical level RTS takes while transmitting. The usual wiring wants it
    // high to send, but the opposite exists and produces a link that looks
    // dead in one direction only.
    bool rts_on_send = true;
    // Whether the receiver stays on during transmit. Off for true half duplex,
    // which is what stops a device hearing its own echo.
    bool rx_during_tx = false;
    // Milliseconds the driver holds the direction pin either side of the
    // frame. Both default to 0 because most transceivers need nothing; slow
    // ones need a millisecond or two, and no model can guess which - this is
    // the knob for the hardware in front of you.
    unsigned delay_rts_before_send_ms = 0;
    unsigned delay_rts_after_send_ms = 0;
  } rs485;

  // Modem control lines, engaged only when set. std::nullopt means "leave the
  // driver's default alone", which is a different request from "drive it low":
  // an Arduino resets when DTR is asserted at open, so a driver that must not
  // reboot the board sets dtr=false explicitly, while one that has no opinion
  // leaves it unset.
  std::optional<bool> dtr;
  std::optional<bool> rts;

  bool reopen_on_error = true;  // Attempt to reopen on device disconnection/error

  // Reopen the port when no data has been *received* for this long. 0 disables
  // it, which is the default.
  //
  // The failure this catches is a device that stops streaming without ever
  // reporting an error - a wedged USB adapter, a sensor that stopped talking -
  // where every other mechanism here sees a perfectly healthy open port and
  // waits forever.
  //
  // Receive-only on purpose, unlike the TCP idle timeout, which any traffic in
  // either direction resets. A driver polling a mute device writes on schedule
  // and would keep a bidirectional timer alive forever, which is exactly the
  // case worth catching.
  //
  // Expiry runs the same path as a read error, so reopen_on_error decides
  // whether the port is reopened or the link goes to Error.
  unsigned rx_idle_timeout_ms = 0;
  size_t backpressure_threshold = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;
  base::constants::BackpressureStrategy backpressure_strategy = base::constants::BackpressureStrategy::Reliable;
  bool enable_memory_pool = true;
  // Receive callback exceptions request quiet stop when true; false logs and continues receiving.
  bool stop_on_callback_exception = false;

  unsigned retry_interval_ms = base::constants::DEFAULT_RETRY_INTERVAL_MS;
  int max_retries = base::constants::DEFAULT_MAX_RETRIES;

  // Opt into the shared IoContextManager singleton instead of a dedicated
  // io_context + thread (the default since #440). Only meaningful for
  // deliberately trading per-instance parallelism for reduced thread/memory
  // overhead across many instances in one process.
  bool use_shared_context = false;

  // Validation methods
  bool is_valid() const {
    return read_chunk >= base::constants::MIN_READ_BUFFER_SIZE && read_chunk <= base::constants::MAX_READ_BUFFER_SIZE &&
           util::InputValidator::is_valid_device_path(device) && baud_rate >= base::constants::MIN_BAUD_RATE &&
           baud_rate <= base::constants::MAX_BAUD_RATE && char_size >= 5 && char_size <= 8 &&
           (stop_bits == 1 || stop_bits == 2) && retry_interval_ms >= base::constants::MIN_RETRY_INTERVAL_MS &&
           retry_interval_ms <= base::constants::MAX_RETRY_INTERVAL_MS &&
           backpressure_threshold >= base::constants::MIN_BACKPRESSURE_THRESHOLD &&
           backpressure_threshold <= base::constants::MAX_BACKPRESSURE_THRESHOLD &&
           (rx_idle_timeout_ms == 0 || (rx_idle_timeout_ms >= base::constants::MIN_IDLE_TIMEOUT_MS &&
                                        rx_idle_timeout_ms <= base::constants::MAX_IDLE_TIMEOUT_MS)) &&
           (max_retries == -1 || (max_retries >= 0 && max_retries <= base::constants::MAX_RETRIES_LIMIT));
  }

  // Apply validation and clamp values to valid ranges
  void validate_and_clamp() {
    // Same bounds the TCP and UDS configs apply to read_buffer_size: this is
    // the per-connection userspace read buffer under a different name, and an
    // unclamped 0 reaches the transport as rx_.resize(0).
    if (read_chunk < base::constants::MIN_READ_BUFFER_SIZE) {
      read_chunk = base::constants::MIN_READ_BUFFER_SIZE;
    } else if (read_chunk > base::constants::MAX_READ_BUFFER_SIZE) {
      read_chunk = base::constants::MAX_READ_BUFFER_SIZE;
    }

    if (baud_rate < base::constants::MIN_BAUD_RATE) {
      baud_rate = base::constants::MIN_BAUD_RATE;
    } else if (baud_rate > base::constants::MAX_BAUD_RATE) {
      baud_rate = base::constants::MAX_BAUD_RATE;
    }

    if (char_size < 5)
      char_size = 5;
    else if (char_size > 8)
      char_size = 8;

    if (stop_bits != 1 && stop_bits != 2) stop_bits = 1;

    if (retry_interval_ms < base::constants::MIN_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
    } else if (retry_interval_ms > base::constants::MAX_RETRY_INTERVAL_MS) {
      retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS;
    }

    if (backpressure_threshold < base::constants::MIN_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;
    } else if (backpressure_threshold > base::constants::MAX_BACKPRESSURE_THRESHOLD) {
      backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD;
    }

    if (rx_idle_timeout_ms != 0 && rx_idle_timeout_ms > base::constants::MAX_IDLE_TIMEOUT_MS) {
      rx_idle_timeout_ms = base::constants::MAX_IDLE_TIMEOUT_MS;
    }

    if (max_retries != -1 && max_retries > base::constants::MAX_RETRIES_LIMIT) {
      max_retries = base::constants::MAX_RETRIES_LIMIT;
    }
  }
};

}  // namespace config
}  // namespace wirestead
