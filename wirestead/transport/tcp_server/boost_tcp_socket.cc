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

#include "wirestead/transport/tcp_server/boost_tcp_socket.hpp"

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/socket.h>
#include <sys/types.h>
#endif

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

BoostTcpSocket::BoostTcpSocket(tcp::socket sock) : socket_(std::move(sock)) {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
  int yes = 1;
  (void)::setsockopt(static_cast<int>(socket_.native_handle()), SOL_SOCKET, SO_NOSIGPIPE, &yes,
                     static_cast<socklen_t>(sizeof(yes)));
#endif
}

void BoostTcpSocket::async_read_some(const net::mutable_buffer& buffer,
                                     std::function<void(const boost::system::error_code&, std::size_t)> handler) {
  socket_.async_read_some(buffer, std::move(handler));
}

void BoostTcpSocket::async_write(const net::const_buffer& buffer,
                                 std::function<void(const boost::system::error_code&, std::size_t)> handler) {
  net::async_write(socket_, buffer, std::move(handler));
}

void BoostTcpSocket::shutdown(tcp::socket::shutdown_type what, boost::system::error_code& ec) {
  socket_.shutdown(what, ec);
}

void BoostTcpSocket::close(boost::system::error_code& ec) { socket_.close(ec); }

tcp::endpoint BoostTcpSocket::remote_endpoint(boost::system::error_code& ec) const {
  return socket_.remote_endpoint(ec);
}

}  // namespace transport
}  // namespace wirestead
