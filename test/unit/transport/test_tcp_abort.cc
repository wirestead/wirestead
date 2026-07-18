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

#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <memory>
#include <vector>

#include "wirestead/interface/itcp_socket.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace std::chrono_literals;

namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

class FakeTcpSocket : public interface::TcpSocketInterface {
 public:
  explicit FakeTcpSocket(net::io_context& ioc) : ioc_(ioc) {}

  void async_read_some(const net::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    write_handler_ = std::move(handler);
    write_size_ = buffer.size();
  }

  void emit_read(std::size_t n = 1, const boost::system::error_code& ec = {}) {
    if (!read_handler_) return;
    auto handler = std::move(read_handler_);
    net::post(ioc_, [handler = std::move(handler), ec, n]() { handler(ec, n); });
  }

  void complete_write(const boost::system::error_code& ec = {}) {
    if (!write_handler_) return;
    auto handler = std::move(write_handler_);
    auto size = write_size_;
    net::post(ioc_, [handler = std::move(handler), ec, size]() { handler(ec, size); });
  }

  void shutdown(tcp::socket::shutdown_type, boost::system::error_code& ec) override { ec.clear(); }

  void close(boost::system::error_code& ec) override {
    ec.clear();
    // Simulate Asio behavior: closing socket cancels pending operations
    if (read_handler_) {
      auto handler = std::move(read_handler_);
      net::post(ioc_, [handler = std::move(handler)]() { handler(boost::asio::error::operation_aborted, 0); });
    }
    if (write_handler_) {
      auto handler = std::move(write_handler_);
      net::post(ioc_, [handler = std::move(handler)]() { handler(boost::asio::error::operation_aborted, 0); });
    }
  }

  tcp::endpoint remote_endpoint(boost::system::error_code& ec) const override {
    ec.clear();
    return tcp::endpoint(net::ip::make_address("127.0.0.1"), 12345);
  }

 private:
  net::io_context& ioc_;
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
  std::function<void(const boost::system::error_code&, std::size_t)> write_handler_;
  std::size_t write_size_ = 0;
};

}  // namespace

TEST(TransportTcpServerSessionTest, AbortDuringWrite) {
  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);
  size_t bp_threshold = 20 * 1024 * 1024;  // 20MB to ensure we don't hit queue limit with 10MB

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  // Keep a raw pointer to control the mock
  // auto* socket_raw = socket.get(); // Suppress unused variable warning by not assigning if not needed
  // But we might need it if we wanted to verify socket state. Here strictly relying on session behavior.

  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> closed{false};
  session->on_close([&]() { closed = true; });

  session->start();
  EXPECT_TRUE(session->alive());

  // Queue a large write (10MB)
  std::vector<uint8_t> large_data(10 * 1024 * 1024, 0xAB);
  session->async_write_copy(memory::ConstByteSpan(large_data.data(), large_data.size()));

  // Run io_context briefly to let async_write propagate to the socket mock
  // With FakeTcpSocket, async_write posts a task.
  ioc.run_for(10ms);

  // Now stop the session while write is pending
  session->stop();

  // Run io_context to process the stop and the resulting cancellation
  ioc.run_for(50ms);

  // Note: TcpServerSession::stop() clears callbacks before closing, so on_close is not expected.
  EXPECT_FALSE(session->alive());

  // Verify no crash occurred and we reached this point
}
