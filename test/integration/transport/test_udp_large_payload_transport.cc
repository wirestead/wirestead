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
#include <cstdlib>
#include <future>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/udp/udp.hpp"

namespace wirestead {
namespace test {
namespace {

using wirestead::base::constants::BackpressureStrategy;
using namespace std::chrono_literals;
namespace net = boost::asio;
using udp = net::ip::udp;

constexpr size_t kControlPayloadSize = 1024;

struct UdpLargePayloadTransportParam {
  size_t payload_size;
  BackpressureStrategy strategy;
};

struct ReceiveState {
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<uint8_t> received;
  udp::endpoint sender;
  bool ready = false;
};

uint16_t allocate_udp_port() {
  net::io_context ioc;
  udp::socket socket(ioc);
  socket.open(udp::v4());
  socket.bind({net::ip::address_v4::loopback(), 0});
  const auto port = socket.local_endpoint().port();
  socket.close();
  return port;
}

std::vector<uint8_t> make_payload(size_t size, uint64_t seed) {
  std::vector<uint8_t> payload(size);
  for (size_t i = 0; i < size; ++i) {
    payload[i] = static_cast<uint8_t>((i * 131 + seed * 17) & 0xFF);
  }
  return payload;
}

bool wait_for_receive(ReceiveState& state, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(state.mutex);
  return state.cv.wait_for(lock, timeout, [&] { return state.ready; });
}

bool large_payload_diagnostics_enabled() {
  const char* value = std::getenv("WIRESTEAD_RUN_UDP_LARGE_PAYLOAD_DIAGNOSTICS");
  return value != nullptr && std::string_view(value) == "1";
}

std::string strategy_name(BackpressureStrategy strategy) {
  switch (strategy) {
    case BackpressureStrategy::Reliable:
      return "Reliable";
    case BackpressureStrategy::BestEffort:
      return "BestEffort";
  }
  return "Unknown";
}

void PrintTo(const UdpLargePayloadTransportParam& param, std::ostream* os) {
  *os << strategy_name(param.strategy) << "_" << param.payload_size << "Bytes";
}

std::string test_param_name(const ::testing::TestParamInfo<UdpLargePayloadTransportParam>& info) {
  return strategy_name(info.param.strategy) + "_" + std::to_string(info.param.payload_size) + "Bytes";
}

std::string format_stats(const char* label, const wrapper::RuntimeStats& stats) {
  std::ostringstream out;
  out << label << "{accepted=" << stats.messages_accepted << "/" << stats.bytes_accepted
      << ", sent=" << stats.messages_sent << "/" << stats.bytes_sent << ", received=" << stats.messages_received << "/"
      << stats.bytes_received << ", failed_sends=" << stats.failed_sends << ", dropped=" << stats.dropped_messages
      << "/" << stats.dropped_bytes << ", queued=" << stats.queued_bytes << ", pending=" << stats.pending_bytes << "}";
  return out.str();
}

config::UdpConfig make_receiver_config(uint16_t port, BackpressureStrategy strategy) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

}  // namespace

class UdpLargePayloadTransportTest : public ::testing::TestWithParam<UdpLargePayloadTransportParam> {};

TEST_P(UdpLargePayloadTransportTest, RawDatagramReachesUdpChannel) {
  const auto param = GetParam();
  if (param.payload_size > kControlPayloadSize && !large_payload_diagnostics_enabled()) {
    GTEST_SKIP() << "set WIRESTEAD_RUN_UDP_LARGE_PAYLOAD_DIAGNOSTICS=1 to run large UDP payload diagnostics";
  }

  const auto payload = make_payload(param.payload_size, 7);
  const auto port = allocate_udp_port();
  auto receiver = transport::UdpChannel::create(make_receiver_config(port, param.strategy));
  ReceiveState state;
  std::atomic<bool> ready{false};

  receiver->on_state([&](base::LinkState link_state) {
    if (link_state == base::LinkState::Listening || link_state == base::LinkState::Connected) {
      ready = true;
    }
  });
  receiver->on_bytes_from([&](memory::ConstByteSpan data, const udp::endpoint& sender) {
    std::vector<uint8_t> copy(data.begin(), data.end());
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.received = std::move(copy);
      state.sender = sender;
      state.ready = true;
    }
    state.cv.notify_one();
  });

  receiver->start();
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return ready.load(); }, 1000));

  net::io_context ioc;
  udp::socket sender(ioc, udp::endpoint(udp::v4(), 0));
  sender.send_to(net::buffer(payload), udp::endpoint(net::ip::make_address("127.0.0.1"), port));

  const bool received = wait_for_receive(state, 3s);
  const auto receiver_stats = receiver->stats();
  ASSERT_TRUE(received) << format_stats("receiver", receiver_stats);

  std::vector<uint8_t> actual;
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    actual = state.received;
  }

  EXPECT_EQ(actual, payload);
  EXPECT_GE(receiver_stats.messages_received, 1u);
  EXPECT_GE(receiver_stats.bytes_received, payload.size());

  receiver->stop();
}

INSTANTIATE_TEST_SUITE_P(
    AllStrategiesAndPayloads, UdpLargePayloadTransportTest,
    ::testing::Values(UdpLargePayloadTransportParam{kControlPayloadSize, BackpressureStrategy::Reliable},
                      UdpLargePayloadTransportParam{4096, BackpressureStrategy::Reliable},
                      UdpLargePayloadTransportParam{16384, BackpressureStrategy::Reliable},
                      UdpLargePayloadTransportParam{kControlPayloadSize, BackpressureStrategy::BestEffort},
                      UdpLargePayloadTransportParam{4096, BackpressureStrategy::BestEffort},
                      UdpLargePayloadTransportParam{16384, BackpressureStrategy::BestEffort}),
    test_param_name);

}  // namespace test
}  // namespace wirestead
