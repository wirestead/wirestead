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
#include "wirestead/interface/iuds_socket.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

/**
 * @brief Boost.Asio implementation of UdsSocketInterface interface.
 */
class WIRESTEAD_API BoostUdsSocket : public interface::UdsSocketInterface {
 public:
  explicit BoostUdsSocket(uds::socket sock);
  ~BoostUdsSocket() override = default;

  void async_read_some(const net::mutable_buffer& buffer,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override;
  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override;
  void async_connect(const uds::endpoint& endpoint,
                     std::function<void(const boost::system::error_code&)> handler) override;
  void shutdown(uds::socket::shutdown_type what, boost::system::error_code& ec) override;
  void close(boost::system::error_code& ec) override;
  uds::endpoint remote_endpoint(boost::system::error_code& ec) const override;

 private:
  uds::socket socket_;
};

}  // namespace transport
}  // namespace wirestead
