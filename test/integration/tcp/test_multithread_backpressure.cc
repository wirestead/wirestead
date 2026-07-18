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

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;

namespace net = boost::asio;
using tcp = net::ip::tcp;

class BackpressureIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

/**
 * @brief Ensure TcpServerSession stays thread-safe under multi-threaded io_context
 */
TEST_F(BackpressureIntegrationTest, TcpServerSessionBackpressureMultithreadedIoContext) {
  net::io_context ioc;
  auto guard = net::make_work_guard(ioc);

  std::thread t1([&]() { ioc.run(); });
  std::thread t2([&]() { ioc.run(); });

  const uint16_t port = TestUtils::getAvailableTestPort();
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), port));

  std::mutex mutex;
  std::condition_variable cv;
  std::shared_ptr<TcpServerSession> session;
  std::vector<size_t> bp_events;
  bool connected = false;

  acceptor.async_accept([&](const boost::system::error_code& ec, tcp::socket sock) {
    if (ec) return;
    session = std::make_shared<TcpServerSession>(ioc, std::move(sock), 256 * 1024);
    session->on_backpressure([&](size_t queued) {
      std::lock_guard<std::mutex> lock(mutex);
      bp_events.push_back(queued);
      cv.notify_all();
    });
    session->start();
    {
      std::lock_guard<std::mutex> lock(mutex);
      connected = true;
    }
    cv.notify_all();
  });

  tcp::socket client(ioc);
  client.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return connected; }));
  }

  ASSERT_TRUE(session);

  std::atomic<bool> draining{true};
  std::thread drain_thread([&] {
    std::array<uint8_t, 64 * 1024> buf{};
    while (draining.load()) {
      boost::system::error_code ec;
      auto n = client.read_some(net::buffer(buf), ec);
      if (ec || n == 0) break;
    }
  });

  std::vector<uint8_t> payload(512 * 1024, 0x5A);
  session->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] { return bp_events.size() >= 2; }));
  }

  draining = false;
  boost::system::error_code ec;
  client.shutdown(tcp::socket::shutdown_both, ec);
  client.close(ec);
  if (drain_thread.joinable()) drain_thread.join();

  guard.reset();
  ioc.stop();
  if (t1.joinable()) t1.join();
  if (t2.joinable()) t2.join();

  ASSERT_GE(bp_events.size(), 2u);
}
