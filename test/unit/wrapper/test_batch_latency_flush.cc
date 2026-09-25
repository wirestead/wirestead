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
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "test_connection_channel.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/wrapper/serial/serial.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/uds_client/uds_client.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;
namespace net = boost::asio;

namespace {

// Batch delivery has two triggers and only one of them was ever tested. The
// size trigger fires inline from the receive path, so a test that emits
// batch_size() messages exercises it without an io_context going anywhere.
// The latency trigger is different: the first message into an empty queue arms
// a steady_timer on the channel's executor, and flush_batches() runs only from
// that timer's completion handler. The shared FakeChannel hands out an executor
// for an io_context nobody runs, so the timer never fires there and
// flush_batches() went uncovered in all seven wrappers.
//
// This channel therefore owns a real io_context and runs it, which is the
// whole point of the fixture - a partial batch has to reach the user when the
// device goes quiet, and that is the only path that delivers it.
class RunningFakeChannel : public wirestead::test::TestConnectionChannel {
 public:
  RunningFakeChannel() : work_(net::make_work_guard(ioc_)), thread_([this] { ioc_.run(); }) {}

  ~RunningFakeChannel() override {
    work_.reset();
    ioc_.stop();
    if (thread_.joinable()) thread_.join();
  }

  void start() override {
    connected_ = true;
    connection_opened();
  }
  void stop() override {
    connected_ = false;
    connection_lost();
  }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return false; }

  net::any_io_executor get_executor() override { return ioc_.get_executor(); }

  SendResult async_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }

  void on_bytes(OnBytes cb) override {
    std::lock_guard<std::mutex> lock(mutex_);
    on_bytes_ = std::move(cb);
  }
  void on_state(OnState cb) override {
    std::lock_guard<std::mutex> lock(mutex_);
    on_state_ = std::move(cb);
  }
  void on_backpressure(OnBackpressure) override {}

  // Delivered on the io thread, the way a real transport delivers it, so the
  // queue mutation and the timer that flushes it share a thread here too.
  void emit_bytes(std::string_view text) {
    std::vector<uint8_t> bytes(text.begin(), text.end());
    net::post(ioc_, [this, bytes = std::move(bytes)]() {
      OnBytes handler;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        handler = on_bytes_;
      }
      if (handler) handler(memory::ConstByteSpan(bytes.data(), bytes.size()));
    });
  }

  void emit_state(base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected_ = true;
      connection_opened();
    }
    OnState handler;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      handler = on_state_;
    }
    if (handler) handler(state);
  }

 private:
  net::io_context ioc_;
  net::executor_work_guard<net::io_context::executor_type> work_;
  std::thread thread_;
  bool connected_{false};
  std::mutex mutex_;
  OnBytes on_bytes_;
  OnState on_state_;
};

// One message, a batch size it cannot reach, and a latency it must wait out.
// Both queues inside flush_batches() are loaded: the raw bytes land in the data
// batch queue and the framer turns the same bytes into one framed message.
template <typename Wrapper>
void expect_partial_batch_flushes_after_latency() {
  auto channel = std::make_shared<RunningFakeChannel>();
  Wrapper wrapper(std::static_pointer_cast<interface::Channel>(channel));

  std::mutex mutex;
  std::vector<std::string> data_payloads;
  std::vector<std::string> message_payloads;
  std::atomic<int> data_batches{0};
  std::atomic<int> message_batches{0};

  wrapper.batch_size(10).batch_latency(50ms);
  wrapper.framer(std::make_unique<framer::LineFramer>());
  wrapper.on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& ctx : batch) data_payloads.push_back(ctx.data_as_string());
    data_batches++;
  });
  wrapper.on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& ctx : batch) message_payloads.push_back(ctx.data_as_string());
    message_batches++;
  });

  auto started = wrapper.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());

  channel->emit_bytes("only\n");

  // Nothing else arrives, so the queues stay at one entry each - far below the
  // batch size of 10. Only the latency timer can deliver them.
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return data_batches.load() > 0 && message_batches.load() > 0; }, 2000))
      << "partial batch was never flushed: data_batches=" << data_batches.load()
      << " message_batches=" << message_batches.load();

  std::lock_guard<std::mutex> lock(mutex);
  EXPECT_EQ(data_batches.load(), 1);
  EXPECT_EQ(message_batches.load(), 1);
  ASSERT_EQ(data_payloads.size(), 1U);
  ASSERT_EQ(message_payloads.size(), 1U);
  EXPECT_EQ(data_payloads[0], "only\n");
  EXPECT_EQ(message_payloads[0], "only");

  wrapper.stop();
}

}  // namespace

// The server wrappers are covered in
// test/integration/wrapper/test_server_batch_latency_flush.cc instead: their
// receive path hangs off transport_server->on_multi_data(), reached through a
// dynamic_pointer_cast to the concrete transport, so an interface::Channel
// fake never delivers a byte to them and they need a real loopback.

TEST(BatchLatencyFlushTest, TcpClientFlushesAPartialBatch) {
  expect_partial_batch_flushes_after_latency<wrapper::TcpClient>();
}

TEST(BatchLatencyFlushTest, UdpClientFlushesAPartialBatch) {
  expect_partial_batch_flushes_after_latency<wrapper::UdpClient>();
}

TEST(BatchLatencyFlushTest, UdsClientFlushesAPartialBatch) {
  expect_partial_batch_flushes_after_latency<wrapper::UdsClient>();
}

TEST(BatchLatencyFlushTest, SerialFlushesAPartialBatch) {
  expect_partial_batch_flushes_after_latency<wrapper::Serial>();
}
