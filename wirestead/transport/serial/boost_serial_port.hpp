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

  void async_read_some(const net::mutable_buffer& buffer,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    port_.async_read_some(buffer, std::move(handler));
  }

  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    net::async_write(port_, buffer, std::move(handler));
  }

 private:
  net::serial_port port_;
};

}  // namespace transport
}  // namespace wirestead
