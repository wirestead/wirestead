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

#include "wirestead/transport/uds/boost_uds_acceptor.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

BoostUdsAcceptor::BoostUdsAcceptor(boost::asio::io_context& ioc) : acceptor_(ioc) {}

void BoostUdsAcceptor::open(const uds& protocol, boost::system::error_code& ec) { acceptor_.open(protocol, ec); }

void BoostUdsAcceptor::bind(const uds::endpoint& endpoint, boost::system::error_code& ec) {
  acceptor_.bind(endpoint, ec);
}

void BoostUdsAcceptor::listen(int backlog, boost::system::error_code& ec) { acceptor_.listen(backlog, ec); }

bool BoostUdsAcceptor::is_open() const { return acceptor_.is_open(); }

void BoostUdsAcceptor::close(boost::system::error_code& ec) { acceptor_.close(ec); }

void BoostUdsAcceptor::async_accept(std::function<void(const boost::system::error_code&, uds::socket)> handler) {
  acceptor_.async_accept(std::move(handler));
}

}  // namespace transport
}  // namespace wirestead
