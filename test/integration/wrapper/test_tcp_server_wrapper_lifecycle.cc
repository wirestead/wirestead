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
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "unilink/framer/line_framer.hpp"
#include "unilink/unilink.hpp"
#include "wrapper_contract_test_utils.hpp"

namespace {

using namespace unilink;
using namespace unilink::test;

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
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  auto server1 = unilink::tcp_server(test_port_).on_error([](auto&&) {}).build();
  auto server2 = unilink::tcp_server(port2).on_error([](auto&&) {}).build();

  server1->on_connect([&](const wrapper::ConnectionContext&) { thread1 = std::this_thread::get_id(); });
  server2->on_connect([&](const wrapper::ConnectionContext&) { thread2 = std::this_thread::get_id(); });

  auto f1 = server1->start();
  auto f2 = server2->start();
  EXPECT_TRUE(f1.get());
  EXPECT_TRUE(f2.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server1->listening() && server2->listening(); }, 1000));

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = unilink::tcp_client("127.0.0.1", port2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  auto server1 = unilink::tcp_server(test_port_).shared_context(true).on_error([](auto&&) {}).build();
  auto server2 = unilink::tcp_server(port2).shared_context(true).on_error([](auto&&) {}).build();

  server1->on_connect([&](const wrapper::ConnectionContext&) { thread1 = std::this_thread::get_id(); });
  server2->on_connect([&](const wrapper::ConnectionContext&) { thread2 = std::this_thread::get_id(); });

  auto f1 = server1->start();
  auto f2 = server2->start();
  EXPECT_TRUE(f1.get());
  EXPECT_TRUE(f2.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server1->listening() && server2->listening(); }, 1000));

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = unilink::tcp_client("127.0.0.1", port2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto f1 = server_->start();
  ASSERT_TRUE(f1.get());

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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

  auto client2 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(server_->start().get());
  server_->stop();

  // Must not crash even if transport_cache_ is stale/reset at this point.
  server_->max_clients(1);

  ASSERT_TRUE(server_->start().get());

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->max_clients(1);
  ASSERT_TRUE(server_->start().get());

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client1->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  // Second client is over the limit - must be closed promptly (not left
  // connected-but-silent). Its own on_disconnect doesn't reliably fire here
  // (an unlimited-retry client transitions Connected->Connecting again
  // without a distinct Closed notification), so observe promptness via
  // repeated connect attempts instead: the server accepting-then-closing
  // each attempt drives the client's retry loop, which wouldn't advance at
  // all if the connection were instead left open and silent.
  auto client2 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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

  auto client3 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(client3->start().get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() >= 1; }, 5000));

  client2->stop();
  client3->stop();
}

TEST_F(TcpServerWrapperLifecycleTest, SendAndCountReflectLiveClientsAndReturnStatus) {
  std::vector<size_t> ids;
  std::mutex ids_mutex;

  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->on_connect([&](const wrapper::ConnectionContext& ctx) {
    std::lock_guard<std::mutex> lk(ids_mutex);
    ids.push_back(ctx.client_id());
  });
  auto server_start_fut = server_->start();
  ASSERT_TRUE(server_start_fut.get());

  auto client1 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto client2 = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

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
  if (target_id != 0) server_->send_to(target_id, "ping");
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
      unilink::tcp_server(test_port_).port_retry(true, 5, 100).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  auto f = server_->start();
  EXPECT_TRUE(f.get());
  EXPECT_TRUE(server_->listening());
}

TEST_F(TcpServerWrapperLifecycleTest, ConcurrentStartStop) {
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {  // Reduced count for stability
    threads.emplace_back([this]() {
      for (int j = 0; j < 5; ++j) {
        auto f = server_->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        server_->stop();
      }
    });
  }
  for (auto& t : threads) t.join();
  SUCCEED();
}

TEST_F(TcpServerWrapperLifecycleTest, HandlerReplacement) {
  std::atomic<int> count{0};
  server_ = unilink::tcp_server(test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  server_->on_connect([&](const wrapper::ConnectionContext&) { count = 1; });
  server_->on_connect([&](const wrapper::ConnectionContext&) { count = 2; });

  auto f = server_->start();
  auto client = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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

  auto client = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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

  auto client = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
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
  auto client = unilink::tcp_client("127.0.0.1", test_port_)
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

}  // namespace
