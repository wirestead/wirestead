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

#include <gmock/gmock.h>

#include <boost/asio.hpp>
#include <functional>
#include <memory>

#include "wirestead/base/platform.hpp"

namespace wirestead {
namespace test {
namespace mocks {

/**
 * @brief Mock implementation of TCP socket for testing
 *
 * This mock allows us to simulate network behavior without actual network operations,
 * making tests faster, more reliable, and environment-independent.
 */
class MockTcpSocket {
 public:
  using ConnectCallback = std::function<void(const boost::system::error_code&)>;
  using ReadCallback = std::function<void(const boost::system::error_code&, size_t)>;
  using WriteCallback = std::function<void(const boost::system::error_code&, size_t)>;
  using AcceptCallback = std::function<void(const boost::system::error_code&)>;

  // Connection operations
  MOCK_METHOD(void, async_connect, (const boost::asio::ip::tcp::endpoint&, ConnectCallback), ());

  // Read operations
  MOCK_METHOD(void, async_read_some, (const boost::asio::mutable_buffer&, ReadCallback), ());

  MOCK_METHOD(void, async_read, (const boost::asio::mutable_buffer&, ReadCallback), ());

  // Write operations
  MOCK_METHOD(void, async_write, (const boost::asio::const_buffer&, WriteCallback), ());

  MOCK_METHOD(void, async_write_some, (const boost::asio::const_buffer&, WriteCallback), ());

  // Socket management
  MOCK_METHOD(void, close, (), ());
  MOCK_METHOD(bool, is_open, (), (const));
  MOCK_METHOD(boost::asio::ip::tcp::endpoint, remote_endpoint, (), (const));
  MOCK_METHOD(boost::asio::ip::tcp::endpoint, local_endpoint, (), (const));

  // Socket options
  MOCK_METHOD(void, set_option, (const boost::asio::socket_base::reuse_address&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::socket_base::keep_alive&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::ip::tcp::no_delay&), ());

  // Error handling
  MOCK_METHOD(boost::system::error_code, get_error, (), (const));

  virtual ~MockTcpSocket() = default;
};

/**
 * @brief Mock implementation of TCP acceptor for testing
 */
class MockTcpAcceptor {
 public:
  using AcceptCallback = std::function<void(const boost::system::error_code&)>;

  // Acceptor operations
  MOCK_METHOD(void, async_accept, (MockTcpSocket&, AcceptCallback), ());

  MOCK_METHOD(void, bind, (const boost::asio::ip::tcp::endpoint&), ());

  MOCK_METHOD(void, listen, (), ());
  MOCK_METHOD(void, close, (), ());

  // Acceptor state
  MOCK_METHOD(bool, is_open, (), (const));
  MOCK_METHOD(boost::asio::ip::tcp::endpoint, local_endpoint, (), (const));

  // Acceptor options
  MOCK_METHOD(void, set_option, (const boost::asio::socket_base::reuse_address&), ());

  virtual ~MockTcpAcceptor() = default;
};

/**
 * @brief Mock implementation of serial port for testing
 */
class MockSerialPort {
 public:
  using ReadCallback = std::function<void(const boost::system::error_code&, size_t)>;
  using WriteCallback = std::function<void(const boost::system::error_code&, size_t)>;
  using OpenCallback = std::function<void(const boost::system::error_code&)>;

  // Serial port operations
  MOCK_METHOD(void, async_read_some, (const boost::asio::mutable_buffer&, ReadCallback), ());

  MOCK_METHOD(void, async_write, (const boost::asio::const_buffer&, WriteCallback), ());

  MOCK_METHOD(void, open, (const std::string&), ());
  MOCK_METHOD(void, close, (), ());

  // Serial port state
  MOCK_METHOD(bool, is_open, (), (const));

  // Serial port options
  MOCK_METHOD(void, set_option, (const boost::asio::serial_port_base::baud_rate&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::serial_port_base::character_size&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::serial_port_base::flow_control&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::serial_port_base::parity&), ());
  MOCK_METHOD(void, set_option, (const boost::asio::serial_port_base::stop_bits&), ());

  virtual ~MockSerialPort() = default;
};

}  // namespace mocks
}  // namespace test
}  // namespace wirestead
