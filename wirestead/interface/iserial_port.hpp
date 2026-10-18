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

#include <boost/asio.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace interface {

namespace net = boost::asio;

/**
 * @brief An interface abstracting Boost.Asio's serial_port for testability.
 * This is an internal interface used for dependency injection and mocking.
 */
class WIRESTEAD_API SerialPortInterface {
 public:
  virtual ~SerialPortInterface() = default;

  virtual void open(const std::string& device, boost::system::error_code& ec) = 0;
  virtual bool is_open() const = 0;
  virtual void close(boost::system::error_code& ec) = 0;

  virtual void set_option(const net::serial_port_base::baud_rate& option, boost::system::error_code& ec) = 0;
  virtual void set_option(const net::serial_port_base::character_size& option, boost::system::error_code& ec) = 0;
  virtual void set_option(const net::serial_port_base::stop_bits& option, boost::system::error_code& ec) = 0;
  virtual void set_option(const net::serial_port_base::parity& option, boost::system::error_code& ec) = 0;
  virtual void set_option(const net::serial_port_base::flow_control& option, boost::system::error_code& ec) = 0;

  // Ask the driver to stop batching received bytes behind its own timer.
  // Returns whether the request took effect. Not an error when it does not -
  // only some drivers (notably USB serial) have such a timer to disable, so
  // the caller logs the outcome and carries on. Defaults to "unsupported" so
  // test doubles inherit it for free.
  virtual bool set_low_latency() { return false; }

  // Put the port into half-duplex RS-485, letting the driver drive the
  // transceiver's direction pin around each write. Delays are milliseconds.
  // Returns whether the request took effect; adapters that switch direction in
  // hardware refuse it, and that is not an error. Primitives rather than a
  // struct so this interface stays independent of the config layer - there is
  // exactly one caller.
  virtual bool set_rs485(bool rts_on_send, bool rx_during_tx, unsigned delay_before_ms, unsigned delay_after_ms) {
    (void)rts_on_send;
    (void)rx_during_tx;
    (void)delay_before_ms;
    (void)delay_after_ms;
    return false;
  }

  // Assert or clear DTR/RTS. std::nullopt leaves a line untouched, which is
  // distinct from driving it low. Returns whether every requested change was
  // applied.
  virtual bool set_modem_lines(std::optional<bool> dtr, std::optional<bool> rts) {
    (void)dtr;
    (void)rts;
    return false;
  }

  // Linux: assert DTR and disable hangup-on-close. A refusal must not be
  // reported as Connected when the caller explicitly requested retention.
  virtual bool keep_dtr_on_close() { return false; }

  virtual void async_read_some(const net::mutable_buffer& buffer,
                               std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;
  virtual void async_write(const net::const_buffer& buffer,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;

  // Scatter-gather write: sends every buffer in `buffers` as one operation,
  // completing once with the total byte count or the first error. Draining
  // several queued messages this way turns N send syscalls into one.
  //
  // The memory the buffers point at must stay valid until the handler runs.
  //
  // The default flattens into one buffer and delegates to the single-buffer
  // overload above: correct, but it copies, so implementations backed by a real
  // socket override it. Test doubles can rely on the default.
  virtual void async_write(const std::vector<net::const_buffer>& buffers,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler);
};

// Flattens into one contiguous buffer and delegates. Copies, which is why a
// socket-backed implementation overrides this; kept here so test doubles and
// any not-yet-converted implementation stay correct for free. `flat` is
// owned by the completion lambda, so it outlives the delegated write.
inline void SerialPortInterface::async_write(
    const std::vector<net::const_buffer>& buffers,
    std::function<void(const boost::system::error_code&, std::size_t)> handler) {
  auto flat = std::make_shared<std::vector<unsigned char>>();
  std::size_t total = 0;
  for (const auto& b : buffers) total += b.size();
  flat->reserve(total);
  for (const auto& b : buffers) {
    const auto* p = static_cast<const unsigned char*>(b.data());
    flat->insert(flat->end(), p, p + b.size());
  }
  async_write(net::const_buffer(flat->data(), flat->size()),
              [flat, handler = std::move(handler)](const boost::system::error_code& ec, std::size_t n) {
                if (handler) handler(ec, n);
              });
}

}  // namespace interface
}  // namespace wirestead
