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

#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;

BoostTcpAcceptor::BoostTcpAcceptor(net::io_context& ioc) : acceptor_(ioc) {}

void BoostTcpAcceptor::open(const net::ip::tcp& protocol, boost::system::error_code& ec) {
  acceptor_.open(protocol, ec);
  if (!ec) {
    // Set SO_REUSEADDR to allow immediate port reuse after server shutdown
    // This prevents "Address already in use" errors in tests and quick restarts
    acceptor_.set_option(net::socket_base::reuse_address(true), ec);
  }
}

void BoostTcpAcceptor::bind(const net::ip::tcp::endpoint& endpoint, boost::system::error_code& ec) {
  acceptor_.bind(endpoint, ec);
}

void BoostTcpAcceptor::listen(int backlog, boost::system::error_code& ec) { acceptor_.listen(backlog, ec); }

bool BoostTcpAcceptor::is_open() const { return acceptor_.is_open(); }

void BoostTcpAcceptor::close(boost::system::error_code& ec) { acceptor_.close(ec); }

void BoostTcpAcceptor::async_accept(
    std::function<void(const boost::system::error_code&, net::ip::tcp::socket)> handler) {
  acceptor_.async_accept(std::move(handler));
}

}  // namespace transport
}  // namespace wirestead
