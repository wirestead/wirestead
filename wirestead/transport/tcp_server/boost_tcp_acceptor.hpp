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
#include "wirestead/interface/itcp_acceptor.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;

/**
 * @brief Boost.Asio implementation of ITcpAcceptor interface.
 * This is the real implementation used in production.
 */
class WIRESTEAD_API BoostTcpAcceptor : public interface::TcpAcceptorInterface {
 public:
  explicit BoostTcpAcceptor(net::io_context& ioc);
  ~BoostTcpAcceptor() override = default;

  void open(const net::ip::tcp& protocol, boost::system::error_code& ec) override;
  void bind(const net::ip::tcp::endpoint& endpoint, boost::system::error_code& ec) override;
  void listen(int backlog, boost::system::error_code& ec) override;
  bool is_open() const override;
  void close(boost::system::error_code& ec) override;

  void async_accept(std::function<void(const boost::system::error_code&, net::ip::tcp::socket)> handler) override;

 private:
  net::ip::tcp::acceptor acceptor_;
};

}  // namespace transport
}  // namespace wirestead
