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
#include <memory>

#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"
#include "wirestead/interface/itcp_socket.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

/**
 * @brief Boost.Asio implementation of ITcpSocket interface.
 * This is the real implementation used in production.
 */
class WIRESTEAD_API BoostTcpSocket : public interface::TcpSocketInterface {
 public:
  explicit BoostTcpSocket(tcp::socket sock);
  ~BoostTcpSocket() override = default;

  void async_read_some(const net::mutable_buffer& buffer,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override;
  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override;
  void shutdown(tcp::socket::shutdown_type what, boost::system::error_code& ec) override;
  void close(boost::system::error_code& ec) override;
  tcp::endpoint remote_endpoint(boost::system::error_code& ec) const override;

 private:
  tcp::socket socket_;
};

}  // namespace transport
}  // namespace wirestead
