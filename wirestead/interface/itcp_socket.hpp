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
#include <memory>
#include <vector>

#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace interface {

namespace net = boost::asio;

/**
 * @brief An interface abstracting Boost.Asio's tcp::socket for testability.
 * This is an internal interface used for dependency injection and mocking.
 */
class WIRESTEAD_API TcpSocketInterface {
 public:
  virtual ~TcpSocketInterface();

  virtual void async_read_some(const net::mutable_buffer& buffer,
                               std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;
  virtual void async_write(const net::const_buffer& buffer,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler) = 0;

  // Scatter-gather write: sends every buffer in `buffers` as one operation,
  // completing once with the total byte count or the first error. Draining
  // several queued messages this way turns N send syscalls into one.
  //
  // The memory the buffers point at must stay valid until the handler runs.
  //
  // The default flattens into one buffer and delegates to the single-buffer
  // overload above: correct, but it copies, so implementations backed by a real
  // socket override it. Test doubles can rely on the default.
  virtual void async_write(const std::vector<net::const_buffer>& buffers,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler);
  virtual void shutdown(net::ip::tcp::socket::shutdown_type what, boost::system::error_code& ec) = 0;
  virtual void close(boost::system::error_code& ec) = 0;
  virtual net::ip::tcp::endpoint remote_endpoint(boost::system::error_code& ec) const = 0;

  // Runs whatever has to happen after accept and before the first read. A plain
  // socket has nothing to do, so the default reports success and the caller
  // proceeds exactly as it did before this existed; a TLS socket performs the
  // handshake here. Failure is the session's cue to close rather than read.
  //
  // This std::function interface does not preserve associated executors.
  // Callers must explicitly dispatch/post state changes onto their strand.
  virtual void async_handshake(std::function<void(const boost::system::error_code&)> handler);
};

// Nothing to negotiate on a plain socket. Posting rather than calling inline
// would need an executor this interface does not carry, and every caller
// already dispatches onto its own strand before getting here.
inline void TcpSocketInterface::async_handshake(std::function<void(const boost::system::error_code&)> handler) {
  if (handler) handler(boost::system::error_code());
}

// Flattens into one contiguous buffer and delegates. Copies, which is why a
// socket-backed implementation overrides this; kept here so test doubles and
// any not-yet-converted implementation stay correct for free. `flat` is
// owned by the completion lambda, so it outlives the delegated write.
inline void TcpSocketInterface::async_write(
    const std::vector<net::const_buffer>& buffers,
    std::function<void(const boost::system::error_code&, std::size_t)> handler) {
  auto flat = std::make_shared<std::vector<unsigned char>>();
  std::size_t total = 0;
  for (const auto& b : buffers) total += b.size();
  flat->reserve(total);
  for (const auto& b : buffers) {
    const auto* p = static_cast<const unsigned char*>(b.data());
    flat->insert(flat->end(), p, p + b.size());
  }
  async_write(net::const_buffer(flat->data(), flat->size()),
              [flat, handler = std::move(handler)](const boost::system::error_code& ec, std::size_t n) {
                if (handler) handler(ec, n);
              });
}

}  // namespace interface
}  // namespace wirestead
