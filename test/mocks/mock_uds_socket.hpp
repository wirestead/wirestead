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

#include "wirestead/interface/iuds_socket.hpp"

namespace wirestead {
namespace test {
namespace mocks {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

class MockUdsSocket : public interface::UdsSocketInterface {
 public:
  MOCK_METHOD(void, async_read_some,
              (const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>),
              (override));
  MOCK_METHOD(void, async_write,
              (const net::const_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>),
              (override));
  MOCK_METHOD(void, async_connect, (const uds::endpoint&, std::function<void(const boost::system::error_code&)>),
              (override));
  MOCK_METHOD(void, shutdown, (uds::socket::shutdown_type, boost::system::error_code&), (override));
  MOCK_METHOD(void, close, (boost::system::error_code&), (override));
  MOCK_METHOD(uds::endpoint, remote_endpoint, (boost::system::error_code&), (const, override));
};

}  // namespace mocks
}  // namespace test
}  // namespace wirestead
