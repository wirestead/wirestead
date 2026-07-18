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
#include <random>
#include <vector>

#include "fake_tcp_socket.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using wirestead::test::FakeTcpSocket;
using namespace std::chrono_literals;

namespace {

namespace net = boost::asio;

TEST(TransportTcpFuzzTest, FuzzingData) {
  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);
  size_t bp_threshold = 65536;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> closed{false};
  session->on_close([&]() { closed = true; });

  // Simple echo or no-op parser
  session->on_bytes([&](memory::ConstByteSpan) {
    // Process data normally
  });

  session->start();
  // Allow start_read to register handler
  while (!socket_raw->has_handler()) {
    ioc.run_for(1ms);
  }
  EXPECT_TRUE(session->alive());

  std::mt19937 gen(12345);
  std::uniform_int_distribution<size_t> size_dist(1, 4096);

  // Send 100 random packets
  for (int i = 0; i < 100; ++i) {
    if (!session->alive()) break;

    size_t size = size_dist(gen);
    // Data is random garbage as per FakeTcpSocket implementation discussion

    socket_raw->emit_read(size);
    ioc.run_for(1ms);
  }

  EXPECT_TRUE(session->alive());
  session->stop();
  ioc.run_for(10ms);
  EXPECT_FALSE(session->alive());
}

TEST(TransportTcpFuzzTest, MockParserCrash) {
  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);
  size_t bp_threshold = 65536;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<bool> closed{false};
  session->on_close([&]() { closed = true; });

  // Mock parser that throws on specific "bad" length
  session->on_bytes([&](memory::ConstByteSpan span) {
    if (span.size() == 13) {  // Unlucky number triggers crash
      throw std::runtime_error("Protocol violation");
    }
  });

  session->start();
  // Allow start_read to register handler
  while (!socket_raw->has_handler()) {
    ioc.run_for(1ms);
  }
  EXPECT_TRUE(session->alive());

  // Send safe data
  socket_raw->emit_read(10);
  ioc.run_for(5ms);
  EXPECT_TRUE(session->alive());

  // Send "malformed" packet
  socket_raw->emit_read(13);
  ioc.run_for(5ms);

  EXPECT_TRUE(closed.load());
  EXPECT_FALSE(session->alive());
}

TEST(TransportTcpFuzzTest, PacketSegmentation) {
  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);
  size_t bp_threshold = 65536;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<size_t> received_bytes{0};
  session->on_bytes([&](memory::ConstByteSpan span) { received_bytes += span.size(); });

  session->start();
  while (!socket_raw->has_handler()) {
    ioc.run_for(1ms);
  }

  // Send a 1000-byte packet in tiny chunks of 7 bytes
  size_t total_to_send = 1000;
  size_t sent = 0;
  while (sent < total_to_send) {
    size_t chunk = std::min<size_t>(7, total_to_send - sent);
    socket_raw->emit_read(chunk);
    sent += chunk;
    ioc.run_for(1ms);
  }

  EXPECT_EQ(received_bytes.load(), total_to_send);
  EXPECT_TRUE(session->alive());
  session->stop();
}

TEST(TransportTcpFuzzTest, PacketCoalescing) {
  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);
  size_t bp_threshold = 65536;

  auto socket = std::make_unique<FakeTcpSocket>(ioc);
  auto* socket_raw = socket.get();
  auto session = std::make_shared<TcpServerSession>(ioc, std::move(socket), bp_threshold);

  std::atomic<size_t> call_count{0};
  session->on_bytes([&](memory::ConstByteSpan) { call_count++; });

  session->start();
  while (!socket_raw->has_handler()) {
    ioc.run_for(1ms);
  }

  // FakeTcpSocket::emit_read(N) simulates N bytes ready on the socket.
  // In a real socket, multiple application packets might be delivered in one read.
  socket_raw->emit_read(1024);  // Large chunk
  ioc.run_for(5ms);

  EXPECT_GT(call_count.load(), 0);
  EXPECT_TRUE(session->alive());
  session->stop();
}

}  // namespace
