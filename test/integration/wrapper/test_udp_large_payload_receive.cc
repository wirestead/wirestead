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

#include "wirestead/base/constants.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"

namespace wirestead {
namespace test {
namespace {

using wirestead::base::constants::BackpressureStrategy;
using namespace std::chrono_literals;

constexpr size_t kControlPayloadSize = 1024;

struct UdpLargePayloadParam {
  size_t payload_size;
  BackpressureStrategy strategy;
};

struct ReceiveState {
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<uint8_t> received;
  ClientId client_id = 0;
  bool ready = false;
};

uint16_t allocate_udp_port() {
  boost::asio::io_context ioc;
  boost::asio::ip::udp::socket socket(ioc);
  socket.open(boost::asio::ip::udp::v4());
  socket.bind({boost::asio::ip::address_v4::loopback(), 0});
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

bool wait_for_start(std::future<bool>& started, std::chrono::milliseconds timeout) {
  if (started.wait_for(timeout) != std::future_status::ready) {
    return false;
  }
  return started.get();
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

void PrintTo(const UdpLargePayloadParam& param, std::ostream* os) {
  *os << strategy_name(param.strategy) << "_" << param.payload_size << "Bytes";
}

std::string test_param_name(const ::testing::TestParamInfo<UdpLargePayloadParam>& info) {
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

config::UdpConfig make_server_config(uint16_t port, BackpressureStrategy strategy) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = port;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

config::UdpConfig make_client_config(uint16_t port, BackpressureStrategy strategy) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = port;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

std::string_view payload_view(const std::vector<uint8_t>& payload) {
  return std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size());
}

}  // namespace

class UdpLargePayloadReceiveTest : public ::testing::TestWithParam<UdpLargePayloadParam> {};

TEST_P(UdpLargePayloadReceiveTest, ClientToServerPayloadArrives) {
  const auto param = GetParam();
  if (param.payload_size > kControlPayloadSize && !large_payload_diagnostics_enabled()) {
    GTEST_SKIP() << "set WIRESTEAD_RUN_UDP_LARGE_PAYLOAD_DIAGNOSTICS=1 to run large UDP payload diagnostics";
  }

  const auto payload = make_payload(param.payload_size, 42);
  const auto port = allocate_udp_port();
  ReceiveState state;

  wrapper::UdpServer server(make_server_config(port, param.strategy));
  server.on_data([&](const wrapper::MessageContext& ctx) {
    auto data = ctx.data_as_vector();
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.received = std::move(data);
      state.client_id = ctx.client_id();
      state.ready = true;
    }
    state.cv.notify_one();
  });

  auto server_started = server.start();
  ASSERT_TRUE(wait_for_start(server_started, 2s));

  wrapper::UdpClient client(make_client_config(port, param.strategy));
  auto client_started = client.start();
  ASSERT_TRUE(wait_for_start(client_started, 2s));

  ASSERT_TRUE(client.send_blocking(payload_view(payload)));

  const bool received = wait_for_receive(state, 3s);
  const auto client_stats = client.stats();
  const auto server_stats = server.stats();
  ASSERT_TRUE(received) << format_stats("client", client_stats) << " " << format_stats("server", server_stats);

  std::vector<uint8_t> actual;
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    actual = state.received;
  }

  EXPECT_EQ(actual, payload);
  EXPECT_EQ(client_stats.failed_sends, 0u);
  EXPECT_EQ(client_stats.dropped_messages, 0u);
  EXPECT_GE(server_stats.messages_received, 1u);
  EXPECT_GE(server_stats.bytes_received, payload.size());

  client.stop();
  server.stop();
}

INSTANTIATE_TEST_SUITE_P(AllStrategiesAndPayloads, UdpLargePayloadReceiveTest,
                         ::testing::Values(UdpLargePayloadParam{kControlPayloadSize, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{4096, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{16384, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{kControlPayloadSize, BackpressureStrategy::BestEffort},
                                           UdpLargePayloadParam{4096, BackpressureStrategy::BestEffort},
                                           UdpLargePayloadParam{16384, BackpressureStrategy::BestEffort}),
                         test_param_name);

class UdpLargePayloadEchoTest : public ::testing::TestWithParam<UdpLargePayloadParam> {};

TEST_P(UdpLargePayloadEchoTest, ServerEchoReturnsPayloadToClient) {
  const auto param = GetParam();
  if (param.payload_size > kControlPayloadSize && !large_payload_diagnostics_enabled()) {
    GTEST_SKIP() << "set WIRESTEAD_RUN_UDP_LARGE_PAYLOAD_DIAGNOSTICS=1 to run large UDP payload diagnostics";
  }

  const auto payload = make_payload(param.payload_size, 99);
  const auto port = allocate_udp_port();
  ReceiveState server_state;
  ReceiveState client_state;

  wrapper::UdpServer server(make_server_config(port, param.strategy));
  server.on_data([&](const wrapper::MessageContext& ctx) {
    auto data = ctx.data_as_vector();
    {
      std::lock_guard<std::mutex> lock(server_state.mutex);
      server_state.received = data;
      server_state.client_id = ctx.client_id();
      server_state.ready = true;
    }
    server_state.cv.notify_one();
    const std::string_view echo_view(reinterpret_cast<const char*>(data.data()), data.size());
    (void)server.send_to(ctx.client_id(), echo_view);
  });

  auto server_started = server.start();
  ASSERT_TRUE(wait_for_start(server_started, 2s));

  wrapper::UdpClient client(make_client_config(port, param.strategy));
  client.on_data([&](const wrapper::MessageContext& ctx) {
    auto data = ctx.data_as_vector();
    {
      std::lock_guard<std::mutex> lock(client_state.mutex);
      client_state.received = std::move(data);
      client_state.ready = true;
    }
    client_state.cv.notify_one();
  });

  auto client_started = client.start();
  ASSERT_TRUE(wait_for_start(client_started, 2s));

  ASSERT_TRUE(client.send_blocking(payload_view(payload)));

  const bool server_received = wait_for_receive(server_state, 3s);
  auto client_stats = client.stats();
  auto server_stats = server.stats();
  ASSERT_TRUE(server_received) << format_stats("client", client_stats) << " " << format_stats("server", server_stats);

  const bool client_received = wait_for_receive(client_state, 3s);
  client_stats = client.stats();
  server_stats = server.stats();
  ASSERT_TRUE(client_received) << format_stats("client", client_stats) << " " << format_stats("server", server_stats);

  std::vector<uint8_t> server_actual;
  std::vector<uint8_t> client_actual;
  {
    std::lock_guard<std::mutex> lock(server_state.mutex);
    server_actual = server_state.received;
  }
  {
    std::lock_guard<std::mutex> lock(client_state.mutex);
    client_actual = client_state.received;
  }

  EXPECT_EQ(server_actual, payload);
  EXPECT_EQ(client_actual, payload);
  EXPECT_EQ(client_stats.failed_sends, 0u);
  EXPECT_EQ(client_stats.dropped_messages, 0u);
  EXPECT_GE(client_stats.messages_received, 1u);
  EXPECT_GE(client_stats.bytes_received, payload.size());
  EXPECT_GE(server_stats.messages_received, 1u);
  EXPECT_GE(server_stats.bytes_received, payload.size());

  client.stop();
  server.stop();
}

INSTANTIATE_TEST_SUITE_P(AllStrategiesAndPayloads, UdpLargePayloadEchoTest,
                         ::testing::Values(UdpLargePayloadParam{kControlPayloadSize, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{4096, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{16384, BackpressureStrategy::Reliable},
                                           UdpLargePayloadParam{kControlPayloadSize, BackpressureStrategy::BestEffort},
                                           UdpLargePayloadParam{4096, BackpressureStrategy::BestEffort},
                                           UdpLargePayloadParam{16384, BackpressureStrategy::BestEffort}),
                         test_param_name);

}  // namespace test
}  // namespace wirestead
