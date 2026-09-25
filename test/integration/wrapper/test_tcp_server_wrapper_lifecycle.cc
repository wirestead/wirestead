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
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "server_reliable_result_test.hpp"
#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wrapper_contract_test_utils.hpp"

namespace {

using namespace wirestead;
using namespace wirestead::test;

class TcpServerWrapperLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_port_ = TestUtils::getAvailableTestPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  uint16_t test_port_;
  std::shared_ptr<wrapper::TcpServer> server_;
};

TEST_F(TcpServerWrapperLifecycleTest, ServerStartStopMultipleTimes) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  for (int i = 0; i < 3; ++i) {
    auto f = server_->start();
    EXPECT_TRUE(f.get());
    EXPECT_TRUE(server_->listening());
    server_->stop();
    EXPECT_FALSE(server_->listening());
  }
}

// #440: TcpServer now defaults to a dedicated io_context + thread instead of
// the shared IoContextManager singleton. Two default-constructed servers
// must run their callbacks on distinct threads.
TEST_F(TcpServerWrapperLifecycleTest, DefaultUsesDistinctThreadsPerInstance) {
  uint16_t port2 = TestUtils::getAvailableTestPort();

  std::atomic<std::thread::id> thread1{};
  std::atomic<std::thread::id> thread2{};
  auto server1 = wirestead::tcp_server(test_port_).on_error([](auto&&) {}).build();
  auto server2 = wirestead::tcp_server(port2).on_error([](auto&&) {}).build();

  server1->on_connect([&](const wrapper::ConnectionContext&) { thread1 = std::this_thread::get_id(); });
  server2->on_connect([&](const wrapper::ConnectionContext&) { thread2 = std::this_thread::get_id(); });

  auto f1 = server1->start();
  auto f2 = server2->start();
  EXPECT_TRUE(f1.get());
  EXPECT_TRUE(f2.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server1->listening() && server2->listening(); }, 1000));

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = wirestead::tcp_client("127.0.0.1", port2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  client1->start();
  client2->start();

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] { return thread1.load() != std::thread::id{} && thread2.load() != std::thread::id{}; }, 2000));
  EXPECT_NE(thread1.load(), thread2.load());

  client1->stop();
  client2->stop();
  server1->stop();
  server2->stop();
}

// #440: .shared_context(true) opts back into the shared IoContextManager
// singleton - two such servers should end up driven by the same thread.
TEST_F(TcpServerWrapperLifecycleTest, SharedContextOptInUsesOneThread) {
  uint16_t port2 = TestUtils::getAvailableTestPort();

  std::atomic<std::thread::id> thread1{};
  std::atomic<std::thread::id> thread2{};
  auto server1 = wirestead::tcp_server(test_port_).shared_context(true).on_error([](auto&&) {}).build();
  auto server2 = wirestead::tcp_server(port2).shared_context(true).on_error([](auto&&) {}).build();

  server1->on_connect([&](const wrapper::ConnectionContext&) { thread1 = std::this_thread::get_id(); });
  server2->on_connect([&](const wrapper::ConnectionContext&) { thread2 = std::this_thread::get_id(); });

  auto f1 = server1->start();
  auto f2 = server2->start();
  EXPECT_TRUE(f1.get());
  EXPECT_TRUE(f2.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server1->listening() && server2->listening(); }, 1000));

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = wirestead::tcp_client("127.0.0.1", port2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  client1->start();
  client2->start();

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] { return thread1.load() != std::thread::id{} && thread2.load() != std::thread::id{}; }, 2000));
  EXPECT_EQ(thread1.load(), thread2.load());

  client1->stop();
  client2->stop();
  server1->stop();
  server2->stop();
}

TEST_F(TcpServerWrapperLifecycleTest, ExternalContextNotStoppedWhenNotManaged) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  // Critical: Keep ioc running even when server stops
  auto work = boost::asio::make_work_guard(*ioc);

  server_ = std::make_shared<wrapper::TcpServer>(test_port_, ioc);
  auto f = server_->start();

  std::thread t([&]() { ioc->run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  server_->stop();
  // Server should not stop the external context
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(ioc->stopped());

  work.reset();
  ioc->stop();
  if (t.joinable()) t.join();
}

TEST_F(TcpServerWrapperLifecycleTest, ExternalContextManagedRunsAndStops) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  server_ = std::make_shared<wrapper::TcpServer>(test_port_, ioc);
  server_->manage_external_context(true);
  auto f = server_->start();

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_TRUE(server_->listening());

  server_->stop();
  EXPECT_TRUE(ioc->stopped());
}

TEST_F(TcpServerWrapperLifecycleTest, ManagedExternalContextRestartsStoppedIoContext) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  ioc->stop();

  server_ = std::make_shared<wrapper::TcpServer>(test_port_, ioc);
  server_->manage_external_context(true);

  auto started = server_->start();
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->listening(); }, 5000));

  server_->stop();
  EXPECT_TRUE(ioc->stopped());
}

// #444-remainder: full end-to-end restart correctness - stop() then start()
// again must behave like a clean, fresh server. No leaked client count or
// session state from the stopped instance's transport_cache_/framers_
// carries over into the second instance's client_count()/connected_clients().
TEST_F(TcpServerWrapperLifecycleTest, StopClearsTransportCacheAndFramersBeforeRestart) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto f1 = server_->start();
  ASSERT_TRUE(f1.get());

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client1->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  client1->stop();
  server_->stop();

  // transport_cache_ must be reset by now - client_count() must not report
  // stale state from the now-stopped transport.
  EXPECT_EQ(server_->client_count(), 0u);
  EXPECT_TRUE(server_->connected_clients().empty());

  auto f2 = server_->start();
  ASSERT_TRUE(f2.get());
  EXPECT_TRUE(server_->listening());

  auto client2 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client2->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));
  // Exactly one live client after the restart - no leaked session/framer
  // state from before the stop() carried over.
  EXPECT_EQ(server_->client_count(), 1u);

  client2->stop();
}

// #444-remainder: max_clients() reaches transport_cache_ directly
// (`if (impl_->transport_cache_) impl_->transport_cache_->set_client_limit(max)`)
// - calling it while stopped must not crash on a stale cached pointer, and
// the new limit must still take effect via the wrapper's retained config on
// the next start() regardless of whether transport_cache_ was live to
// receive the call.
TEST_F(TcpServerWrapperLifecycleTest, MaxClientsWhileStoppedAppliesOnNextStart) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(server_->start().get());
  server_->stop();

  // Must not crash even if transport_cache_ is stale/reset at this point.
  server_->max_clients(1);

  ASSERT_TRUE(server_->start().get());

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client1->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  client2->start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // The max_clients(1) set while stopped must be honored: the second
  // connection is rejected, so client_count() stays at 1.
  EXPECT_EQ(server_->client_count(), 1u);

  client1->stop();
  client2->stop();
}

// #437: an over-limit connection used to pause the accept loop entirely
// (paused_accept_), leaving any client whose TCP handshake completed while
// paused connected but silent until a slot freed up. Now the server accepts
// and immediately closes over-limit connections, and keeps accepting
// afterward instead of pausing.
TEST_F(TcpServerWrapperLifecycleTest, OverLimitConnectionIsClosedPromptlyAndAcceptLoopKeepsRunning) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->max_clients(1);
  ASSERT_TRUE(server_->start().get());

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client1->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  // Second client is over the limit - must be closed promptly (not left
  // connected-but-silent). Its own on_disconnect doesn't reliably fire here
  // (an unlimited-retry client transitions Connected->Connecting again
  // without a distinct Closed notification), so observe promptness via
  // repeated connect attempts instead: the server accepting-then-closing
  // each attempt drives the client's retry loop, which wouldn't advance at
  // all if the connection were instead left open and silent.
  auto client2 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  std::atomic<int> client2_connects{0};
  client2->on_connect([&](const wrapper::ConnectionContext&) { client2_connects++; });
  client2->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return client2_connects.load() >= 3; }, 3000))
      << "Over-limit client's retry loop did not advance - connection may have been left open and silent";
  EXPECT_EQ(server_->client_count(), 1u);

  // The accept loop must not have paused: after client1 disconnects and
  // frees the only slot, a new client must still be able to connect without
  // needing anything to explicitly "resume" accepting.
  client1->stop();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 0; }, 5000));

  auto client3 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client3->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  client2->stop();
  client3->stop();
}

TEST_F(TcpServerWrapperLifecycleTest, SendAndCountReflectLiveClientsAndReturnStatus) {
  std::vector<size_t> ids;
  std::mutex ids_mutex;

  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->on_connect([&](const wrapper::ConnectionContext& ctx) {
    std::lock_guard<std::mutex> lk(ids_mutex);
    ids.push_back(ctx.client_id());
  });
  auto server_start_fut = server_->start();
  ASSERT_TRUE(server_start_fut.get());

  auto client1 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  std::atomic<int> client_received{0};
  client1->on_data([&](const wrapper::MessageContext&) { client_received++; });
  client2->on_data([&](const wrapper::MessageContext&) { client_received++; });

  auto c1_fut = client1->start();
  auto c2_fut = client2->start();
  ASSERT_TRUE(c1_fut.get());
  ASSERT_TRUE(c2_fut.get());

  // Wait for connections to stabilize
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 2; }, 10000));
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&]() {
        std::lock_guard<std::mutex> lk(ids_mutex);
        return ids.size() >= 2;
      },
      5000));

  // Small extra delay for transport session readiness
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  size_t target_id = 0;
  {
    std::lock_guard<std::mutex> lk(ids_mutex);
    if (!ids.empty()) target_id = ids.front();
  }

  // Send once first
  if (target_id != 0) {
    EXPECT_TRUE(server_->send_to(target_id, "ping"));
  }
  server_->broadcast("ping");

  // Final check
  bool success = TestUtils::waitForCondition(
      [&]() {
        if (client_received.load() > 0) return true;
        // Periodic retry if not received yet
        server_->broadcast("ping");
        return false;
      },
      5000);

  EXPECT_TRUE(success);
  server_->stop();
}

TEST_F(TcpServerWrapperLifecycleTest, PortRetryConfiguration) {
  server_ =
      wirestead::tcp_server(test_port_).port_retry(true, 5, 100).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto f = server_->start();
  EXPECT_TRUE(f.get());
  EXPECT_TRUE(server_->listening());
}

// D-1 permits concurrent stops. Restart waits until ALL those calls have
// returned; overlapping start/stop is explicitly a caller precondition.
TEST_F(TcpServerWrapperLifecycleTest, RestartAfterConcurrentStops) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  for (int cycle = 0; cycle < 5; ++cycle) {
    ASSERT_TRUE(server_->start_sync());
    std::vector<std::jthread> stoppers;
    for (int caller = 0; caller < 2; ++caller) stoppers.emplace_back([this] { server_->stop(); });
    for (auto& stopper : stoppers) stopper.join();
    EXPECT_FALSE(server_->listening());
  }
}

TEST_F(TcpServerWrapperLifecycleTest, HandlerReplacement) {
  std::atomic<int> count{0};
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->on_connect([&](const wrapper::ConnectionContext&) { count = 1; });
  server_->on_connect([&](const wrapper::ConnectionContext&) { count = 2; });

  auto f = server_->start();
  auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  client->start();

  TestUtils::waitForCondition([&]() { return count.load() > 0; }, 5000);
  EXPECT_EQ(count.load(), 2);
}

TEST_F(TcpServerWrapperLifecycleTest, DisconnectHandlerReplacementUsesLatestCallback) {
  std::atomic<int> count{0};
  wrapper_support::TcpServerLoopbackHarness harness;
  server_ = harness.start_server();
  server_->on_disconnect([&](const wrapper::ConnectionContext&) { count = 1; });
  server_->on_disconnect([&](const wrapper::ConnectionContext&) { count = 2; });

  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));

  client->stop();

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return count.load() > 0; }, 5000));
  EXPECT_EQ(count.load(), 2);
}

TEST(TcpServerWrapperContractTest, InjectedChannelStateAndFallbackOperations) {
  auto fake_channel = std::make_shared<wrapper_support::FakeChannel>();
  wrapper::TcpServer server(std::static_pointer_cast<interface::Channel>(fake_channel));

  std::atomic<int> errors{0};
  server.on_error([&](const wrapper::ErrorContext&) { errors++; });

  auto started = server.start();
  fake_channel->emit_state(base::LinkState::Listening);

  ASSERT_EQ(started.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(server.listening());

  auto second_start = server.start();
  ASSERT_EQ(second_start.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_TRUE(second_start.get());

  EXPECT_EQ(server.client_count(), 0U);
  EXPECT_TRUE(server.connected_clients().empty());
  EXPECT_FALSE(server.broadcast("payload"));
  EXPECT_FALSE(server.try_broadcast("payload"));
  EXPECT_FALSE(server.send_to(1, "payload"));
  EXPECT_FALSE(server.try_send_to(1, "payload"));
  EXPECT_FALSE(server.send_to_blocking(1, "payload"));
  EXPECT_FALSE(server.broadcast_line("line"));
  EXPECT_FALSE(server.send_to_line(1, "line"));
  EXPECT_FALSE(server.try_broadcast_line("line"));
  EXPECT_FALSE(server.try_send_to_line(1, "line"));

  fake_channel->emit_state(base::LinkState::Error);
  EXPECT_FALSE(server.listening());
  EXPECT_EQ(errors.load(), 1);

  server.stop();
  fake_channel->emit_state(base::LinkState::Listening);
  EXPECT_FALSE(server.listening());
}

TEST_F(TcpServerWrapperLifecycleTest, RawDataBatchFlushesByLatency) {
  std::atomic<int> batch_count{0};
  std::vector<std::string> payloads;
  std::mutex payloads_mutex;

  server_ = std::make_shared<wrapper::TcpServer>(test_port_);
  server_->batch_size(100).batch_latency(std::chrono::milliseconds(20));
  server_->on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(payloads_mutex);
    batch_count++;
    for (const auto& ctx : batch) payloads.push_back(ctx.data_as_string());
  });

  ASSERT_TRUE(server_->start().get());

  auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 1; }, 5000));

  ASSERT_TRUE(client->send("raw-batch"));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return batch_count.load() == 1; }, 5000));
  std::lock_guard<std::mutex> lock(payloads_mutex);
  ASSERT_EQ(payloads.size(), 1U);
  EXPECT_EQ(payloads[0], "raw-batch");
}

TEST_F(TcpServerWrapperLifecycleTest, FramedMessageBatchFlushesAtBatchSize) {
  std::atomic<int> batch_count{0};
  std::vector<std::string> messages;
  std::mutex messages_mutex;

  server_ = std::make_shared<wrapper::TcpServer>(test_port_);
  server_->batch_size(2).batch_latency(std::chrono::seconds(1));
  server_->framer([]() { return std::make_unique<framer::LineFramer>(); });
  server_->on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(messages_mutex);
    batch_count++;
    for (const auto& ctx : batch) messages.push_back(ctx.data_as_string());
  });

  ASSERT_TRUE(server_->start().get());

  auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 1; }, 5000));

  ASSERT_TRUE(client->send("first\nsecond\n"));

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return batch_count.load() == 1; }, 5000));
  std::lock_guard<std::mutex> lock(messages_mutex);
  ASSERT_EQ(messages.size(), 2U);
  EXPECT_EQ(messages[0], "first");
  EXPECT_EQ(messages[1], "second");
}

TEST_F(TcpServerWrapperLifecycleTest, LineSendingVariantsReachConnectedClients) {
  server_ = std::make_shared<wrapper::TcpServer>(test_port_);
  server_->backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);
  ASSERT_TRUE(server_->start().get());

  std::atomic<int> received{0};
  std::string received_data;
  std::mutex received_mutex;
  auto client = wirestead::tcp_client("127.0.0.1", test_port_)
                    .on_data([&](const wrapper::MessageContext& ctx) {
                      std::lock_guard<std::mutex> lock(received_mutex);
                      received++;
                      received_data += ctx.data_as_string();
                    })
                    .on_error([](auto&&) {})
                    .build();
  ASSERT_TRUE(client->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 1; }, 5000));

  auto clients = server_->connected_clients();
  ASSERT_EQ(clients.size(), 1U);
  const auto client_id = clients.front();

  EXPECT_TRUE(server_->broadcast_line("broadcast"));
  EXPECT_TRUE(server_->try_broadcast_line("try-broadcast"));
  EXPECT_TRUE(server_->send_to_line(client_id, "send-to"));
  EXPECT_TRUE(server_->try_send_to_line(client_id, "try-send-to"));

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() {
        std::lock_guard<std::mutex> lock(received_mutex);
        return received_data.find("broadcast\n") != std::string::npos &&
               received_data.find("try-broadcast\n") != std::string::npos &&
               received_data.find("send-to\n") != std::string::npos &&
               received_data.find("try-send-to\n") != std::string::npos;
      },
      5000));
  std::lock_guard<std::mutex> lock(received_mutex);
  EXPECT_NE(received_data.find("broadcast\n"), std::string::npos);
  EXPECT_NE(received_data.find("try-broadcast\n"), std::string::npos);
  EXPECT_NE(received_data.find("send-to\n"), std::string::npos);
  EXPECT_NE(received_data.find("try-send-to\n"), std::string::npos);
}

// A session's counters used to vanish with the session, so anything sampling
// stats() on an interval watched server throughput collapse to zero on every
// disconnect. The cumulative fields now survive the connection that produced
// them; the instantaneous ones still describe live sessions only.
TEST_F(TcpServerWrapperLifecycleTest, CumulativeStatsSurviveClientDisconnect) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(server_->start().get());

  const std::string payload = "stats-survive-probe\n";

  auto client = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 10000));

  ASSERT_TRUE(client->send(payload));
  ASSERT_TRUE(server_->broadcast(payload));
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() {
        const auto s = server_->stats();
        return s.bytes_received >= payload.size() && s.bytes_accepted >= payload.size();
      },
      10000));

  const auto connected = server_->stats();
  ASSERT_GT(connected.bytes_received, 0u);
  ASSERT_GT(connected.bytes_accepted, 0u);

  client->stop();
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 0; }, 10000));

  // client_count() stops counting a session as soon as it is no longer alive,
  // which is before the server erases it, so there is no observable moment to
  // sample "just after the erase". Assert the negative instead: give the
  // teardown room to run and require that the counters never collapse. Without
  // the session's totals being carried over they drop to zero almost at once,
  // so this is what discriminates.
  const bool collapsed = TestUtils::waitForCondition([&]() { return server_->stats().bytes_received == 0; }, 2000);
  EXPECT_FALSE(collapsed) << "cumulative counters vanished along with the disconnected session";

  const auto after = server_->stats();
  EXPECT_GE(after.bytes_received, connected.bytes_received);
  EXPECT_GE(after.messages_received, connected.messages_received);
  EXPECT_GE(after.bytes_accepted, connected.bytes_accepted);
  EXPECT_GE(after.messages_accepted, connected.messages_accepted);

  // The queue is gone with the session, so the instantaneous fields do drop.
  EXPECT_EQ(after.queued_bytes, 0u);
  EXPECT_FALSE(after.backpressure_active);

  // A restart is a fresh lifecycle - see the restart contract on IServer.
  server_->stop();
  ASSERT_TRUE(server_->start().get());
  const auto restarted = server_->stats();
  EXPECT_EQ(restarted.bytes_received, 0u);
  EXPECT_EQ(restarted.bytes_accepted, 0u);

  client->stop();
}

// The aggregate cannot tell you which client is responsible for the traffic it
// reports. client_stats() answers that for the sessions that have their own
// counters, and says so honestly when it cannot.
TEST_F(TcpServerWrapperLifecycleTest, ClientStatsAreReportedPerSession) {
  server_ = wirestead::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(server_->start().get());

  const std::string small(64, 's');
  const std::string large(4096, 'l');

  auto quiet = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto noisy = wirestead::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(quiet->start().get());
  ASSERT_TRUE(noisy->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 2; }, 10000));

  const auto ids = server_->connected_clients();
  ASSERT_EQ(ids.size(), 2u);

  ASSERT_TRUE(quiet->send(small));
  ASSERT_TRUE(noisy->send(large));

  // Whichever id each client got, one session must show the small payload and
  // the other the large one.
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() {
        const auto a = server_->client_stats(ids[0]);
        const auto b = server_->client_stats(ids[1]);
        return a && b && a->bytes_received >= small.size() && b->bytes_received >= small.size() &&
               (a->bytes_received >= large.size() || b->bytes_received >= large.size());
      },
      10000));

  const auto a = server_->client_stats(ids[0]);
  const auto b = server_->client_stats(ids[1]);
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());

  const auto& heavier = (a->bytes_received >= b->bytes_received) ? *a : *b;
  const auto& lighter = (a->bytes_received >= b->bytes_received) ? *b : *a;
  EXPECT_GE(heavier.bytes_received, large.size());
  EXPECT_LT(lighter.bytes_received, large.size());

  // The aggregate covers both, so it is at least their sum.
  const auto aggregate = server_->stats();
  EXPECT_GE(aggregate.bytes_received, a->bytes_received + b->bytes_received);

  // An id that was never handed out has no session behind it.
  EXPECT_FALSE(server_->client_stats(999999).has_value());

  // After a disconnect the session is gone, so its counters are only reachable
  // through the aggregate - see CumulativeStatsSurviveClientDisconnect.
  quiet->stop();
  noisy->stop();
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&]() { return !server_->client_stats(ids[0]) && !server_->client_stats(ids[1]); }, 10000));
  EXPECT_GE(server_->stats().bytes_received, large.size());
}

}  // namespace

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> target_result;
thread_local int target_observations = 0;
void observe_target_result(const wirestead::wrapper::SendResult& result) {
  target_result = result;
  ++target_observations;
}
class TcpServerTargetResultTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpServerTargetResultTest, CombinesValidationLifecycleAndSessionAdmission) {
  using namespace wirestead;
  using Rejection = wrapper::SendRejection;
  boost::asio::io_context io;
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.backpressure_threshold = 1024;
  cfg.backpressure_strategy = (GetParam() >= 2 && GetParam() < 11) || GetParam() == 14
                                  ? base::constants::BackpressureStrategy::BestEffort
                                  : base::constants::BackpressureStrategy::Reliable;
  auto native = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
  wrapper::TcpServer server(native);
  server.backpressure_strategy(GetParam() >= 11 && GetParam() < 14 ? base::constants::BackpressureStrategy::Reliable
                                                                   : cfg.backpressure_strategy);
  wrapper::detail::g_tcp_server_send_result_hook.store(observe_target_result);
  transport::detail::g_tcp_server_write_result_hook.store(observe_target_result);
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    wrapper::detail::g_tcp_server_send_result_hook.store(nullptr);
    transport::detail::g_tcp_server_write_result_hook.store(nullptr);
    test::stop_wrapper_with_context(server, io);
  }};
  auto until = [&](auto predicate) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_for(std::chrono::milliseconds(1));
    }
    return true;
  };
  ClientId id = 999;
  const bool native_form = GetParam() >= 6 && GetParam() < 10;
  const bool reliable_form = GetParam() >= 11;
  const bool line_form = (!native_form && GetParam() < 11 && GetParam() % 2) || GetParam() == 12;
  auto write = [&](std::string_view data) {
    target_result.reset();
    target_observations = 0;
    bool accepted;
    auto public_acceptance = [&](wrapper::SendResult result) {
      EXPECT_TRUE(target_result.has_value());
      if (target_result) {
        EXPECT_EQ(target_result->accepted(), result.accepted());
        if (!result.accepted() && !target_result->accepted()) {
          EXPECT_EQ(target_result->reason(), result.reason());
        }
      }
      target_result = result;
      return result.accepted();
    };
    if (reliable_form) {
      if (GetParam() == 11)
        accepted = public_acceptance(server.send_to(id, data));
      else if (GetParam() == 12)
        accepted = public_acceptance(server.send_to_line(id, data));
      else
        accepted = public_acceptance(server.send_to_blocking(id, data));
    } else if (native_form) {
      if (GetParam() == 6)
        accepted = native->send_to_client(id, data);
      else if (GetParam() == 7)
        accepted = native->try_send_to_client(id, data);
      else if (GetParam() == 8)
        accepted = native->send_to_client(
            id, memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
      else
        accepted = native->try_send_to_client(
            id, memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
    } else if (GetParam() >= 2 && GetParam() < 4) {
      accepted = public_acceptance(GetParam() % 2 ? server.send_to_line(id, data) : server.send_to(id, data));
    } else {
      accepted = public_acceptance(GetParam() % 2 ? server.try_send_to_line(id, data) : server.try_send_to(id, data));
    }
    EXPECT_EQ(target_observations, 1);
    EXPECT_TRUE(target_result.has_value());
    if (target_result) {
      EXPECT_EQ(target_result->accepted(), accepted);
    }
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(target_result.has_value());
    ASSERT_FALSE(target_result->accepted());
    EXPECT_EQ(target_result->reason(), expected);
  };
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  if (!native_form && !line_form) {
    EXPECT_FALSE(write(""));
    reason(Rejection::InvalidArgument);
  }
  if (GetParam() == 10) {
    native->start();
    ASSERT_TRUE(until([&] { return server.listening(); }));
    EXPECT_FALSE(write("native running before wrapper start"));
    reason(Rejection::NotStarted);
  }
  auto ready = server.start();
  ASSERT_TRUE(until([&] { return ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  EXPECT_FALSE(write("missing"));
  reason(Rejection::NotReady);
  boost::asio::ip::tcp::socket peer(io);
  peer.connect({boost::asio::ip::make_address("127.0.0.1"), cfg.port});
  ASSERT_TRUE(until([&] { return native->client_count() == 1; }));
  id = native->connected_clients().front();
  if (!native_form) {
    const auto failures = native->stats().failed_sends;
    EXPECT_FALSE(write(std::string(*native->write_queue_limit(id) + 1, 'x')));
    reason(Rejection::TooLarge);
    EXPECT_EQ(native->stats().failed_sends, failures);
  }
  EXPECT_TRUE(write("ok"));
  const bool ordinary = reliable_form || (native_form && GetParam() % 2 == 0);
  const size_t used = line_form ? 3 : 2;
  const auto fill = ordinary ? *native->write_queue_limit(id) - used : 1024 - used;
  if (ordinary)
    ASSERT_TRUE(native->send_to_client(id, std::string(fill, 'f')));
  else
    ASSERT_TRUE(native->try_send_to_client(id, std::string(fill, 'f')));
  const auto failures_before = native->stats().failed_sends;
  EXPECT_FALSE(write("full"));
  reason(GetParam() >= 2 && GetParam() < 4 ? Rejection::QueueFull : Rejection::WouldBlock);
  if (reliable_form) {
    EXPECT_EQ(native->stats().failed_sends, failures_before + 5);
    {
      wrapper::detail::CallbackGuard callback;
      EXPECT_FALSE(write("callback"));
      reason(Rejection::WouldBlock);
    }
    EXPECT_EQ(native->stats().failed_sends, failures_before + 6);
    const auto failures = native->stats().failed_sends;
    EXPECT_FALSE(write(std::string(*native->write_queue_limit(id) + 1, 'x')));
    reason(Rejection::TooLarge);
    EXPECT_EQ(native->stats().failed_sends, failures);
  }
  peer.close();
  ASSERT_TRUE(until([&] { return native->client_count() == 0; }));
  EXPECT_FALSE(write("disconnected"));
  reason(Rejection::NotReady);
  bool stopped = false;
  boost::asio::post(io, [&] {
    server.stop();
    EXPECT_FALSE(write("stopping"));
    reason(Rejection::Stopping);
    stopped = true;
  });
  ASSERT_TRUE(until([&] { return stopped; }));
  test::stop_wrapper_with_context(server, io);
  EXPECT_FALSE(write("stopped"));
  reason(Rejection::NotStarted);
}
INSTANTIATE_TEST_SUITE_P(WrapperAndNativeForms, TcpServerTargetResultTest, ::testing::Range(0, 15));
}  // namespace

namespace {
class TcpServerReliableWaitResultTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpServerReliableWaitResultTest, PreservesFirstCauseAndPinsAdmission) {
  using namespace wirestead;
  boost::asio::io_context io;
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.backpressure_threshold = 1024;

  auto native = transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
  boost::asio::ip::tcp::socket peer(io);
  test::server_wait::run_case<wrapper::TcpServer>(
      GetParam() / 4, GetParam() % 4, io, native, peer,
      [&](auto& socket) { socket.connect({boost::asio::ip::make_address("127.0.0.1"), cfg.port}); },
      {wrapper::detail::g_tcp_server_send_result_hook, wrapper::detail::g_tcp_server_capacity_wait_hook,
       wrapper::detail::g_tcp_server_capacity_wait_result_hook, transport::detail::g_tcp_server_pinned_write_hook});
}
INSTANTIATE_TEST_SUITE_P(FormsAndTerminalEvents, TcpServerReliableWaitResultTest, ::testing::Range(0, 44));
}  // namespace
