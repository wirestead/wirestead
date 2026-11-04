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
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>

#include "wirestead/interface/itcp_socket.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace std::chrono_literals;

namespace {

namespace net = boost::asio;
using Clock = std::chrono::steady_clock;
using Handler = std::function<void(const boost::system::error_code&, std::size_t)>;

// Reads complete only when the test delivers bytes. Writes run on the session
// strand (do_write) and can hold it for a while, which models a strand that is
// busy at the moment the idle deadline passes.
class StrandHoldingSocket : public interface::TcpSocketInterface {
 public:
  void async_read_some(const net::mutable_buffer& buffer, Handler handler) override {
    std::lock_guard<std::mutex> lock(mutex_);
    read_buffer_ = buffer;
    read_handler_ = std::move(handler);
    cv_.notify_all();
  }

  void async_write(const net::const_buffer& buffer, Handler handler) override {
    write_started_ = true;
    std::this_thread::sleep_for(write_hold_);
    handler({}, buffer.size());
  }

  void shutdown(net::ip::tcp::socket::shutdown_type, boost::system::error_code&) override {}
  void close(boost::system::error_code&) override {}
  net::ip::tcp::endpoint remote_endpoint(boost::system::error_code&) const override { return {}; }

  void wait_for_read() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return static_cast<bool>(read_handler_); });
  }

  // Completes the pending read from a non-strand thread, as a real socket does.
  void deliver(const char* bytes) {
    Handler handler;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return static_cast<bool>(read_handler_); });
      handler = std::move(read_handler_);
      read_handler_ = nullptr;
      std::memcpy(read_buffer_.data(), bytes, std::strlen(bytes));
    }
    handler({}, std::strlen(bytes));
  }

  std::chrono::milliseconds write_hold_{0};
  std::atomic<bool> write_started_{false};

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  net::mutable_buffer read_buffer_;
  Handler read_handler_;
};

// An idle expiry already queued on the strand cannot be aborted by cancel().
// Data that arrived before the deadline, and was handled while that expiry
// waited behind it, used to be followed immediately by an idle close.
TEST(TcpSessionIdleTimeoutTest, ActivityBeforeDeadlineIsNotClosedByQueuedExpiry) {
  constexpr auto kIdle = 200ms;
  net::io_context ioc;
  auto guard = net::make_work_guard(ioc);
  std::thread io1([&] { ioc.run(); });
  std::thread io2([&] { ioc.run(); });

  auto socket = std::make_unique<StrandHoldingSocket>();
  auto* sock = socket.get();
  sock->write_hold_ = 2 * kIdle;
  auto session = std::make_shared<TcpServerSession>(
      ioc, std::move(socket), base::constants::DEFAULT_BACKPRESSURE_THRESHOLD, static_cast<int>(kIdle.count()));

  std::mutex mutex;
  Clock::time_point last_rx{};
  Clock::time_point closed_at{};
  session->on_bytes([&](memory::ConstByteSpan) {
    std::lock_guard<std::mutex> lock(mutex);
    last_rx = Clock::now();
  });
  session->on_close([&] {
    std::lock_guard<std::mutex> lock(mutex);
    closed_at = Clock::now();
  });
  session->start();
  sock->wait_for_read();

  // Hold the strand across the idle deadline, and deliver data before it.
  const uint8_t w = 'w';
  ASSERT_TRUE(session->async_write_copy(memory::ConstByteSpan(&w, 1)));
  while (!sock->write_started_) std::this_thread::yield();
  std::this_thread::sleep_for(kIdle / 2);
  sock->deliver("hello");

  std::this_thread::sleep_for(4 * kIdle);
  {
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_NE(last_rx, Clock::time_point{}) << "the delivered bytes never reached on_bytes";
    if (closed_at != Clock::time_point{}) {
      // Closing is fine once the connection has really been idle that long.
      EXPECT_GE(closed_at - last_rx, kIdle - 20ms)
          << "closed " << std::chrono::duration_cast<std::chrono::milliseconds>(closed_at - last_rx).count()
          << " ms after the last received data";
    }
  }

  session->stop();
  guard.reset();
  ioc.stop();
  io1.join();
  io2.join();
}

}  // namespace
