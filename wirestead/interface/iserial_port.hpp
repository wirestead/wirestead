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
#include <string>

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

  virtual void async_read_some(const net::mutable_buffer& buffer,
                               std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;
  virtual void async_write(const net::const_buffer& buffer,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;
};

}  // namespace interface
}  // namespace wirestead
