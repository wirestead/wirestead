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

#include "wirestead/base/visibility.hpp"
#include "wirestead/interface/iserial_port.hpp"

#ifdef __linux__
#include <linux/serial.h>
#include <sys/ioctl.h>
#endif
#if defined(__APPLE__)
#include <sys/ioctl.h>
#endif

#include <optional>

namespace wirestead {
namespace transport {

namespace net = boost::asio;

class WIRESTEAD_API BoostSerialPort : public interface::SerialPortInterface {
 public:
  explicit BoostSerialPort(net::io_context& ioc) : port_(ioc) {}

  void open(const std::string& device, boost::system::error_code& ec) override { port_.open(device, ec); }
  bool is_open() const override { return port_.is_open(); }
  void close(boost::system::error_code& ec) override { port_.close(ec); }

  void set_option(const net::serial_port_base::baud_rate& option, boost::system::error_code& ec) override {
    port_.set_option(option, ec);
  }
  void set_option(const net::serial_port_base::character_size& option, boost::system::error_code& ec) override {
    port_.set_option(option, ec);
  }
  void set_option(const net::serial_port_base::stop_bits& option, boost::system::error_code& ec) override {
    port_.set_option(option, ec);
  }
  void set_option(const net::serial_port_base::parity& option, boost::system::error_code& ec) override {
    port_.set_option(option, ec);
  }
  void set_option(const net::serial_port_base::flow_control& option, boost::system::error_code& ec) override {
    port_.set_option(option, ec);
  }

  // Linux only. TIOCGSERIAL fails with ENOTTY on drivers that have no latency
  // timer to clear, which is the common case for native UARTs and CDC-ACM, so
  // the caller treats false as "nothing to do" rather than as a failure.
  bool set_low_latency() override {
#ifdef __linux__
    struct serial_struct info;
    const int fd = port_.native_handle();
    if (::ioctl(fd, TIOCGSERIAL, &info) != 0) return false;
    const auto low_latency = static_cast<decltype(info.flags)>(ASYNC_LOW_LATENCY);
    if (info.flags & low_latency) return true;
    info.flags |= low_latency;
    return ::ioctl(fd, TIOCSSERIAL, &info) == 0;
#else
    return false;
#endif
  }

  // Linux only. A driver with no RS-485 support answers ENOTTY, which the
  // caller treats as "this adapter does the switching itself".
  bool set_rs485(bool rts_on_send, bool rx_during_tx, unsigned delay_before_ms, unsigned delay_after_ms) override {
#ifdef __linux__
    struct serial_rs485 rs485 {};
    const int fd = port_.native_handle();
    // Read first: some drivers carry board-specific bits in flags that a
    // blind write would clear.
    if (::ioctl(fd, TIOCGRS485, &rs485) != 0) return false;
    rs485.flags |= SER_RS485_ENABLED;
    if (rts_on_send) {
      rs485.flags |= SER_RS485_RTS_ON_SEND;
      rs485.flags &= ~static_cast<decltype(rs485.flags)>(SER_RS485_RTS_AFTER_SEND);
    } else {
      rs485.flags &= ~static_cast<decltype(rs485.flags)>(SER_RS485_RTS_ON_SEND);
      rs485.flags |= SER_RS485_RTS_AFTER_SEND;
    }
    if (rx_during_tx) {
      rs485.flags |= SER_RS485_RX_DURING_TX;
    } else {
      rs485.flags &= ~static_cast<decltype(rs485.flags)>(SER_RS485_RX_DURING_TX);
    }
    rs485.delay_rts_before_send = delay_before_ms;
    rs485.delay_rts_after_send = delay_after_ms;
    return ::ioctl(fd, TIOCSRS485, &rs485) == 0;
#else
    (void)rts_on_send;
    (void)rx_during_tx;
    (void)delay_before_ms;
    (void)delay_after_ms;
    return false;
#endif
  }

  bool set_modem_lines(std::optional<bool> dtr, std::optional<bool> rts) override {
#if defined(__linux__) || defined(__APPLE__)
    const int fd = port_.native_handle();
    bool ok = true;
    auto apply = [&](std::optional<bool> want, int bit) {
      if (!want) return;
      int lines = bit;
      if (::ioctl(fd, *want ? TIOCMBIS : TIOCMBIC, &lines) != 0) ok = false;
    };
    apply(dtr, TIOCM_DTR);
    apply(rts, TIOCM_RTS);
    return ok;
#else
    (void)dtr;
    (void)rts;
    return false;
#endif
  }

  void async_read_some(const net::mutable_buffer& buffer,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    port_.async_read_some(buffer, std::move(handler));
  }

  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    net::async_write(port_, buffer, std::move(handler));
  }

  // Real scatter-gather write: hands the sequence to asio, which issues one
  // sendmsg/writev instead of one send per queued buffer.
  void async_write(const std::vector<net::const_buffer>& buffers,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    net::async_write(port_, buffers, std::move(handler));
  }

 private:
  net::serial_port port_;
};

}  // namespace transport
}  // namespace wirestead
