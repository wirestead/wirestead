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
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "server_reliable_result_test.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
#include "wirestead/wirestead.hpp"
#include "wrapper_contract_test_utils.hpp"

namespace {

using namespace std::chrono_literals;
using wirestead::test::TestUtils;

void expect_fast_broadcast(std::string_view transport, const std::function<wirestead::FanoutResult()>& broadcast) {
  SCOPED_TRACE(std::string(transport));
  const auto start = std::chrono::steady_clock::now();
  const auto result = broadcast();
  EXPECT_EQ(result.target_count(), 1u);
  EXPECT_EQ(result.accepted_count(), 1u);
  EXPECT_EQ(result.rejected_count(), 0u);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, 100ms);
}

bool is_port_allocation_failure(const std::exception& ex) {
  return std::string_view(ex.what()).find("Unable to find available test port") != std::string_view::npos;
}

}  // namespace

TEST(ServerBroadcastContractTest, TcpBroadcastIsNonBlockingFanoutAndDeliversToConnectedClients) {
  std::unique_ptr<wirestead::test::wrapper_support::TcpServerLoopbackHarness> harness;
  try {
    harness = std::make_unique<wirestead::test::wrapper_support::TcpServerLoopbackHarness>();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "TCP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = harness->start_server();
  std::atomic<int> received{0};

  auto client = harness->connect_client();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  ASSERT_TRUE(harness->wait_for_client_count(1));

  expect_fast_broadcast("tcp", [&]() { return server->broadcast("tcp-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("tcp-try", [&]() { return server->try_broadcast("tcp-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}

TEST(ServerBroadcastContractTest, UdpBroadcastIsNonBlockingFanoutAndDeliversToKnownClients) {
  std::unique_ptr<wirestead::test::wrapper_support::UdpServerLoopbackHarness> harness;
  try {
    harness = std::make_unique<wirestead::test::wrapper_support::UdpServerLoopbackHarness>();
  } catch (const std::exception& ex) {
    if (is_port_allocation_failure(ex)) {
      GTEST_SKIP() << "UDP port allocation unavailable in this environment: " << ex.what();
    }
    throw;
  }

  auto server = harness->start_server();
  std::atomic<int> received{0};

  auto client = harness->start_sender();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  harness->send_from_client("register");
  ASSERT_TRUE(harness->wait_for_client_count(1));

  expect_fast_broadcast("udp", [&]() { return server->broadcast("udp-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("udp-try", [&]() { return server->try_broadcast("udp-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}

#ifndef _WIN32
TEST(ServerBroadcastContractTest, UdsBroadcastIsNonBlockingFanoutAndDeliversToConnectedClients) {
  wirestead::test::wrapper_support::UdsServerLoopbackHarness harness("broadcast-contract");
  std::shared_ptr<wirestead::wrapper::UdsServer> server;
  try {
    server = harness.start_server();
  } catch (const std::exception& ex) {
    GTEST_SKIP() << "UDS unavailable in this environment: " << ex.what();
  }

  std::atomic<int> received{0};
  auto client = harness.connect_client();
  client->on_data([&](const wirestead::MessageContext&) { received.fetch_add(1); });
  ASSERT_TRUE(harness.wait_for_client_count(1));

  expect_fast_broadcast("uds", [&]() { return server->broadcast("uds-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 1; }, 5000));

  expect_fast_broadcast("uds-try", [&]() { return server->try_broadcast("uds-try-broadcast"); });
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 2; }, 5000));
}
#endif

namespace {
using namespace wirestead;
template <typename Wrapper, typename Native, typename Socket, typename Connect>
void check_fanout(int mode, boost::asio::io_context& io, std::shared_ptr<Native> native, Connect connect,
                  std::atomic<void (*)()>& hook) {
  Wrapper server(native);
  std::atomic<int> connections{0};
  server.on_connect([&](const auto&) { ++connections; });
  server.backpressure_strategy(mode >= 6 ? base::constants::BackpressureStrategy::BestEffort
                                         : base::constants::BackpressureStrategy::Reliable);
  mode %= 6;
  auto work = boost::asio::make_work_guard(io);
  Socket first(io), second(io), late(io);
  test::server_wait::Observation observation;
  std::future<FanoutResult> writer;
  std::thread runner;
  test::server_wait::OnExit cleanup{[&] {
    observation.entry.release.notify();
    if (!runner.joinable())
      runner = std::thread([&] {
        io.restart();
        io.run();
      });
    if (writer.valid()) writer.wait();
    hook = nullptr;
    test::server_wait::observation = nullptr;
    server.stop();
    work.reset();
    io.stop();
    runner.join();
  }};
  auto pump = [&](auto predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  };
  auto ready = server.start();
  ASSERT_TRUE(pump([&] { return ready.wait_for(0s) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  EXPECT_TRUE(server.broadcast("no targets").empty());
  connect(first);
  // Complete connection initialization before parking the snapshot caller
  // under the wrapper lock and exercising concurrent membership changes.
  ASSERT_TRUE(pump([&] { return native->client_count() == 1 && connections == 1; }));
  const auto first_id = native->connected_clients().front();
  if (mode < 4) {
    connect(second);
    ASSERT_TRUE(pump([&] { return native->client_count() == 2; }));
    for (auto send : {&wrapper::ServerInterface::broadcast, &wrapper::ServerInterface::try_broadcast}) {
      const auto invalid = (server.*send)("");
      EXPECT_EQ(invalid.target_count(), 2u);
      EXPECT_EQ(invalid.rejected_count(SendRejection::InvalidArgument), 2u);
    }
    const std::string oversized(base::constants::MAX_BUFFER_SIZE + 1, 'x');
    const auto too_large = server.try_broadcast(oversized);
    EXPECT_EQ(too_large.rejected_count(SendRejection::TooLarge), 2u);
    EXPECT_EQ(too_large.accepted_count(), 0u);
    const auto hard_limit = native->write_queue_limit(first_id);
    ASSERT_TRUE(hard_limit.has_value());
    const auto exceeds_queue = server.try_broadcast(std::string(*hard_limit + 1, 'q'));
    EXPECT_EQ(exceeds_queue.rejected_count(SendRejection::TooLarge), 2u);
    const auto line_exceeds_queue = server.try_broadcast_line(std::string(*hard_limit, 'q'));
    EXPECT_EQ(line_exceeds_queue.rejected_count(SendRejection::TooLarge), 2u);
    // Publish pressure on only one target, then leave the executor paused.
    ASSERT_TRUE(native->send_to_client(first_id, std::string(1024, 'p')));
    ASSERT_TRUE(pump([&] { return native->is_backpressure_active(first_id); }));
    const auto before = std::chrono::steady_clock::now();
    const auto result = mode == 0   ? server.broadcast("mixed")
                        : mode == 1 ? server.try_broadcast("mixed")
                        : mode == 2 ? server.broadcast_line("mixed")
                                    : server.try_broadcast_line("mixed");
    EXPECT_LT(std::chrono::steady_clock::now() - before, 100ms);
    EXPECT_EQ(result.target_count(), 2u);
    EXPECT_EQ(result.accepted_count(), 1u);
    EXPECT_EQ(result.rejected_count(), 1u);
    EXPECT_EQ(result.rejected_count(SendRejection::WouldBlock), 1u);
    EXPECT_TRUE(result);
    return;
  }
  test::server_wait::observation = &observation;
  hook = test::server_wait::park_entry;
  writer = std::async(std::launch::async, [&] { return server.broadcast("snapshot"); });
  ASSERT_TRUE(observation.entry.entered.wait());
  runner = std::thread([&] { io.run(); });
  if (mode == 4) {
    connect(late);
    ASSERT_TRUE(TestUtils::waitForCondition([&] { return native->client_count() == 2; }, 5000));
  } else {
    first.close();
    ASSERT_TRUE(TestUtils::waitForCondition([&] { return native->client_count() == 0; }, 5000));
  }
  observation.entry.release.notify();
  ASSERT_EQ(writer.wait_for(5s), std::future_status::ready);
  const auto result = writer.get();
  EXPECT_EQ(result.target_count(), 1u);
  EXPECT_EQ(result.accepted_count(), mode == 4 ? 1u : 0u);
  EXPECT_EQ(result.rejected_count(SendRejection::NotReady), mode == 4 ? 0u : 1u);
}
class ServerFanoutResultTest : public ::testing::TestWithParam<int> {};
TEST_P(ServerFanoutResultTest, TcpCountsFixedTargetsAndPartialAdmission) {
  boost::asio::io_context io;
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.backpressure_threshold = 1024;
  cfg.backpressure_strategy = GetParam() >= 6 ? base::constants::BackpressureStrategy::BestEffort
                                              : base::constants::BackpressureStrategy::Reliable;
  auto native = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
  check_fanout<wrapper::TcpServer, transport::TcpServer, boost::asio::ip::tcp::socket>(
      GetParam(), io, native,
      [&](auto& socket) { socket.connect({boost::asio::ip::make_address("127.0.0.1"), cfg.port}); },
      transport::detail::g_tcp_fanout_snapshot_hook);
}
#ifndef _WIN32
TEST_P(ServerFanoutResultTest, UdsCountsFixedTargetsAndPartialAdmission) {
  boost::asio::io_context io;
  config::UdsServerConfig cfg;
  cfg.socket_path = TestUtils::makeUniqueUdsSocketPath("fanout-result").string();
  cfg.backpressure_threshold = 1024;
  cfg.backpressure_strategy = GetParam() >= 6 ? base::constants::BackpressureStrategy::BestEffort
                                              : base::constants::BackpressureStrategy::Reliable;
  test::server_wait::OnExit remove{[&] { TestUtils::removeFileIfExists(cfg.socket_path); }};
  auto native = transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
  check_fanout<wrapper::UdsServer, transport::UdsServer, boost::asio::local::stream_protocol::socket>(
      GetParam(), io, native,
      [&](auto& socket) { socket.connect(boost::asio::local::stream_protocol::endpoint(cfg.socket_path)); },
      transport::detail::g_uds_fanout_snapshot_hook);
}
#endif
INSTANTIATE_TEST_SUITE_P(FormsAndSnapshots, ServerFanoutResultTest, ::testing::Range(0, 12));
}  // namespace

TEST(ServerBroadcastContractTest, UdpCountsKnownTargetsAndValidationForAllForms) {
  wirestead::test::wrapper_support::UdpServerLoopbackHarness harness;
  auto server = harness.start_server();
  EXPECT_TRUE(server->broadcast("empty").empty());
  auto first = harness.start_sender();
  ASSERT_TRUE(first->send("register-first"));
  ASSERT_TRUE(harness.wait_for_client_count(1));
  auto second = harness.start_sender();
  ASSERT_TRUE(second->send("register-second"));
  ASSERT_TRUE(harness.wait_for_client_count(2));
  for (auto send :
       {&wirestead::wrapper::ServerInterface::broadcast, &wirestead::wrapper::ServerInterface::try_broadcast,
        &wirestead::wrapper::ServerInterface::broadcast_line,
        &wirestead::wrapper::ServerInterface::try_broadcast_line}) {
    const auto result = (server.get()->*send)("data");
    EXPECT_EQ(result.target_count(), 2u);
    EXPECT_EQ(result.accepted_count(), 2u);
    EXPECT_EQ(result.rejected_count(), 0u);
    const auto invalid = (server.get()->*send)(std::string(wirestead::base::constants::MAX_BUFFER_SIZE + 1, 'x'));
    EXPECT_EQ(invalid.target_count(), 2u);
    EXPECT_EQ(invalid.rejected_count(wirestead::SendRejection::TooLarge), 2u);
  }
  EXPECT_EQ(server->broadcast("").rejected_count(wirestead::SendRejection::InvalidArgument), 2u);
  EXPECT_EQ(server->try_broadcast("").rejected_count(wirestead::SendRejection::InvalidArgument), 2u);
  EXPECT_EQ(server->broadcast_line("").accepted_count(), 2u);
  EXPECT_EQ(server->try_broadcast_line("").accepted_count(), 2u);
  server->stop();
  EXPECT_TRUE(server->broadcast("stopped").empty());
  first->stop();
  second->stop();
}

namespace {
class UdpFanoutPressureTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpFanoutPressureTest, CountsPartialAndTotalRejectionOnSharedQueueWithoutWaiting) {
  boost::asio::io_context io;
  using udp = boost::asio::ip::udp;
  udp::socket first(io, {udp::v4(), 0}), second(io, {udp::v4(), 0});
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = TestUtils::getAvailableTestPort();
  cfg.backpressure_threshold = 1024;
  cfg.backpressure_strategy = GetParam() >= 4 ? base::constants::BackpressureStrategy::BestEffort
                                              : base::constants::BackpressureStrategy::Reliable;
  auto native = transport::UdpChannel::create(cfg, io);
  wrapper::UdpServer server(native);
  server.backpressure_strategy(cfg.backpressure_strategy);
  auto work = boost::asio::make_work_guard(io);
  test::server_wait::OnExit cleanup{[&] {
    std::thread runner([&] {
      io.restart();
      io.run();
    });
    server.stop();
    work.reset();
    io.stop();
    runner.join();
  }};
  auto pump = [&](auto predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  };
  auto ready = server.start();
  ASSERT_TRUE(pump([&] { return ready.wait_for(0s) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  const udp::endpoint destination(boost::asio::ip::make_address("127.0.0.1"), cfg.local_port);
  first.send_to(boost::asio::buffer("one", 3), destination);
  second.send_to(boost::asio::buffer("two", 3), destination);
  ASSERT_TRUE(pump([&] { return server.client_count() == 2; }));
  const int form = GetParam() % 4;
  const std::string payload(form < 2 ? 1024 : 1023, 'p');
  auto send = [&] {
    if (form == 0) return server.broadcast(payload);
    if (form == 1) return server.try_broadcast(payload);
    if (form == 2) return server.broadcast_line(payload);
    return server.try_broadcast_line(payload);
  };
  const auto before = std::chrono::steady_clock::now();
  const auto mixed = send();
  EXPECT_LT(std::chrono::steady_clock::now() - before, 100ms);
  EXPECT_EQ(mixed.target_count(), 2u);
  EXPECT_EQ(mixed.accepted_count(), 1u);
  EXPECT_EQ(mixed.rejected_count(SendRejection::WouldBlock), 1u);
  const auto rejected = send();
  EXPECT_EQ(rejected.target_count(), 2u);
  EXPECT_EQ(rejected.accepted_count(), 0u);
  EXPECT_EQ(rejected.rejected_count(SendRejection::WouldBlock), 2u);
  EXPECT_FALSE(rejected.empty());
  EXPECT_FALSE(rejected);
}
INSTANTIATE_TEST_SUITE_P(FormsAndStrategies, UdpFanoutPressureTest, ::testing::Range(0, 8));
}  // namespace
