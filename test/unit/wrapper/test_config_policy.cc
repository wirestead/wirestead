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
#include <future>
#include <limits>
#include <memory>
#include <thread>

#include "test_connection_channel.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
using namespace wirestead;
using namespace std::chrono_literals;
namespace {
class CallbackChannel : public wirestead::test::TestConnectionChannel {
 public:
  boost::asio::io_context io;
  bool ready = false;
  OnState state;
  OnBytes bytes;
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void emit_bytes() {
    const uint8_t data[] = {120, 10};
    if (bytes) bytes(data);
  }
  void start() override {}
  void stop() override {
    ready = false;
    connection_lost();
  }
  bool is_connected() const override { return ready; }
  bool is_backpressure_active() const override { return false; }
  void on_backpressure(OnBackpressure) override {}
  SendResult async_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan b) override { return async_write_copy_result(b); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&& b) override {
    return async_write_move_result(std::move(b));
  }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> b) override {
    return async_write_shared_result(std::move(b));
  }

  boost::asio::any_io_executor get_executor() override { return io.get_executor(); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void emit_state(base::LinkState s) {
    ready = s == base::LinkState::Connected;
    if (ready)
      connection_opened();
    else
      connection_lost();
    if (state) state(s);
  }
};
template <class W>
class ConfigPolicyTest : public ::testing::Test {};
using Clients = ::testing::Types<wrapper::TcpClient, wrapper::UdsClient, wrapper::UdpClient, wrapper::Serial>;
TYPED_TEST_SUITE(ConfigPolicyTest, Clients);

TYPED_TEST(ConfigPolicyTest, InvalidValuesDoNotReplacePreviousSettings) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  w.backpressure_threshold(2048).batch_size(2);
  EXPECT_THROW(w.backpressure_threshold(0), std::invalid_argument);
  EXPECT_THROW(w.backpressure_threshold(std::numeric_limits<size_t>::max()), std::invalid_argument);
  EXPECT_EQ(w.backpressure_threshold(), 2048u);
  EXPECT_THROW(w.backpressure_strategy(static_cast<base::constants::BackpressureStrategy>(99)), std::invalid_argument);
  EXPECT_THROW(w.batch_size(0), std::invalid_argument);
  EXPECT_THROW(w.batch_latency(-1ms), std::invalid_argument);
  if constexpr (requires { w.retry_interval(100ms); }) {
    EXPECT_THROW(w.retry_interval(-1ms), std::invalid_argument);
    EXPECT_THROW(w.retry_interval(std::chrono::milliseconds::max()), std::invalid_argument);
  }
  int batches = 0;
  w.on_data_batch([&](const auto&) { ++batches; });
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_bytes();
  EXPECT_EQ(batches, 0);
  ch->emit_bytes();
  EXPECT_EQ(batches, 1);
  w.stop();
}
TYPED_TEST(ConfigPolicyTest, OnlyAllowlistedSettingsChangeDuringRun) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  const auto prior = w.backpressure_threshold();
  EXPECT_THROW(w.backpressure_threshold(2048), std::logic_error);
  EXPECT_EQ(w.backpressure_threshold(), prior);
  EXPECT_THROW(w.manage_external_context(true), std::logic_error);
  EXPECT_NO_THROW(w.batch_size(2).batch_latency(10ms));
  EXPECT_NO_THROW(w.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort));
  if constexpr (requires { w.retry_interval(100ms); }) EXPECT_NO_THROW(w.retry_interval(100ms));
  w.stop();
  EXPECT_NO_THROW(w.backpressure_threshold(2048));
  EXPECT_EQ(w.backpressure_threshold(), 2048u);
}
TYPED_TEST(ConfigPolicyTest, CallbackStopIsNotACompletedConfigurationBoundary) {
  auto ch = std::make_shared<CallbackChannel>();
  TypeParam w(ch);
  w.on_data([&](const auto&) {
    w.stop();
    EXPECT_THROW(w.backpressure_threshold(2048), std::logic_error);
  });
  auto start = w.start();
  ch->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(start.get());
  ch->emit_bytes();
  w.stop();
  EXPECT_NO_THROW(w.backpressure_threshold(2048));
}
TEST(ConfigPolicy, BuildersRejectInvalidSettingsBeforeBuild) {
  auto tcp = wirestead::tcp_client("127.0.0.1", 12345);
  EXPECT_THROW(tcp.retry_interval(-1ms), std::invalid_argument);
  EXPECT_THROW(tcp.read_buffer_size(0), std::invalid_argument);
  EXPECT_THROW(tcp.max_retries(-2), std::invalid_argument);
  auto serial = wirestead::serial("/dev/ttyUSB0", 115200);
  EXPECT_THROW(serial.parity("invalid"), std::invalid_argument);
  EXPECT_NO_THROW(serial.parity("EVEN").flow_control("Hardware"));
  auto udp = wirestead::udp_client(0);
  EXPECT_THROW(udp.send_buffer_size(std::numeric_limits<size_t>::max()), std::invalid_argument);
  EXPECT_THROW(udp.remote_endpoint("127.0.0.1", 0), std::invalid_argument);
  EXPECT_THROW(udp.multicast_group("127.0.0.1"), std::invalid_argument);
  EXPECT_THROW(udp.multicast_group("239.1.2.3", "bad"), std::invalid_argument);
  auto server = wirestead::tcp_server(12345);
  EXPECT_THROW(server.tls("cert", ""), std::invalid_argument);
}
TEST(ConfigPolicy, MalformedIdentitiesFailBeforeStart) {
  EXPECT_THROW(wrapper::TcpClient("", 9000), std::invalid_argument);
  EXPECT_THROW(wrapper::UdsClient(""), std::invalid_argument);
  EXPECT_THROW(wrapper::UdsServer(""), std::invalid_argument);
  EXPECT_THROW(wrapper::Serial("", 115200), std::invalid_argument);
  config::UdpConfig udp;
  udp.remote_address = "127.0.0.1";
  EXPECT_THROW(wrapper::UdpClient{udp}, std::invalid_argument);
}
}  // namespace
