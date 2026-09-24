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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "test/mocks/mock_uds_acceptor.hpp"
#include "test/mocks/mock_uds_socket.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/uds_client/uds_client.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"
#include "wrapper_contract_test_utils.hpp"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using namespace std::chrono_literals;

namespace wirestead::wrapper {
namespace {

class ControlledUdsChannel : public interface::Channel {
 public:
  void start() override { connected_ = true; }

  void stop() override { connected_ = false; }

  bool is_connected() const override { return connected_; }

  bool is_backpressure_active() const override { return backpressure_active_; }

  boost::asio::any_io_executor get_executor() override { return ioc_.get_executor(); }

  bool async_write_copy(memory::ConstByteSpan data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++write_count_;
    last_write_.assign(reinterpret_cast<const char*>(data.data()), data.size());
    return write_result_;
  }

  bool async_write_move(std::vector<uint8_t>&& data) override {
    return async_write_copy(memory::ConstByteSpan(data.data(), data.size()));
  }

  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    if (!data) return false;
    return async_write_copy(memory::ConstByteSpan(data->data(), data->size()));
  }

  bool async_try_write_copy(memory::ConstByteSpan data) override { return async_write_copy(data); }

  bool async_try_write_move(std::vector<uint8_t>&& data) override { return async_write_move(std::move(data)); }

  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return async_write_shared(std::move(data));
  }

  void on_bytes(OnBytes cb) override { on_bytes_ = std::move(cb); }

  void on_state(OnState cb) override { on_state_ = std::move(cb); }

  void on_backpressure(OnBackpressure cb) override { on_backpressure_ = std::move(cb); }

  void emit_bytes(std::string_view text) {
    if (!on_bytes_) return;
    on_bytes_(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
  }

  void emit_state(base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected_ = true;
    } else if (state == base::LinkState::Closed || state == base::LinkState::Error || state == base::LinkState::Idle) {
      connected_ = false;
    }

    if (on_state_) on_state_(state);
  }

  void emit_backpressure(size_t queued) {
    if (on_backpressure_) on_backpressure_(queued);
  }

  void set_backpressure_active(bool active) { backpressure_active_ = active; }

  void set_write_result(bool result) { write_result_ = result; }

  int write_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_count_;
  }

  std::string last_write() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_write_;
  }

 private:
  boost::asio::io_context ioc_;
  bool connected_{false};
  bool backpressure_active_{false};
  bool write_result_{true};
  mutable std::mutex mutex_;
  int write_count_{0};
  std::string last_write_;
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_backpressure_;
};

TEST(UdsClientWrapperLifecycleTest, AutoManageStartsInjectedTransport) {
  boost::asio::io_context ioc;
  config::UdsClientConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uwc").string();

  auto* mock_socket = new test::mocks::MockUdsSocket();
  auto transport_client =
      transport::UdsClient::create(cfg, std::unique_ptr<interface::UdsSocketInterface>(mock_socket), ioc);

  EXPECT_CALL(*mock_socket, async_connect(_, _)).WillOnce(Invoke([&ioc](const auto&, auto handler) {
    boost::asio::post(ioc, [handler]() { handler({}); });
  }));
  EXPECT_CALL(*mock_socket, async_read_some(_, _)).WillRepeatedly(Invoke([](const auto&, auto) {}));

  UdsClient client(std::static_pointer_cast<interface::Channel>(transport_client));
  client.auto_start(true);

  ioc.restart();
  ioc.run_for(100ms);

  EXPECT_TRUE(client.connected());

  wirestead::test::stop_wrapper_with_context(client, ioc);
  ioc.restart();
  ioc.run_for(50ms);
}

TEST(UdsClientWrapperLifecycleTest, StartFutureReflectsTransportFailure) {
  boost::asio::io_context ioc;
  config::UdsClientConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uwc-fail").string();

  auto* mock_socket = new test::mocks::MockUdsSocket();
  auto transport_client =
      transport::UdsClient::create(cfg, std::unique_ptr<interface::UdsSocketInterface>(mock_socket), ioc);

  EXPECT_CALL(*mock_socket, async_connect(_, _)).WillOnce(Invoke([&ioc](const auto&, auto handler) {
    boost::asio::post(ioc, [handler]() { handler(make_error_code(boost::asio::error::connection_refused)); });
  }));

  UdsClient client(std::static_pointer_cast<interface::Channel>(transport_client));
  auto started = client.start();

  ioc.restart();
  ioc.run_for(100ms);

  ASSERT_EQ(started.wait_for(0ms), std::future_status::ready);
  EXPECT_FALSE(started.get());

  wirestead::test::stop_wrapper_with_context(client, ioc);
  ioc.restart();
  ioc.run_for(50ms);
}

TEST(UdsClientWrapperLifecycleTest, ManagedExternalContextStopsOnShutdown) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto socket_path = test::TestUtils::makeUniqueUdsSocketPath("uwc-managed").string();
  test::TestUtils::removeFileIfExists(socket_path);

  UdsServer server(socket_path);
  auto server_started = server.start();
  ASSERT_EQ(server_started.wait_for(1s), std::future_status::ready);
  ASSERT_TRUE(server_started.get());

  UdsClient client(socket_path, ioc);
  client.manage_external_context(true);

  ioc->stop();
  auto started = client.start();
  ASSERT_EQ(started.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(started.get());

  EXPECT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return client.connected(); }, 2000));

  client.stop();
  EXPECT_TRUE(ioc->stopped());

  server.stop();
  test::TestUtils::removeFileIfExists(socket_path);
}

TEST(UdsServerWrapperLifecycleTest, AutoManageStartsInjectedTransport) {
  boost::asio::io_context ioc;
  config::UdsServerConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uws").string();
  test::TestUtils::removeFileIfExists(cfg.socket_path);

  auto* mock_acceptor = new test::mocks::MockUdsAcceptor();
  auto transport_server =
      transport::UdsServer::create(cfg, std::unique_ptr<interface::UdsAcceptorInterface>(mock_acceptor), ioc);

  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, listen(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, async_accept(_)).WillOnce(Invoke([](auto) {}));

  UdsServer server(std::static_pointer_cast<interface::Channel>(transport_server));
  server.auto_start(true);

  ioc.restart();
  ioc.poll();

  EXPECT_TRUE(server.listening());

  wirestead::test::stop_wrapper_with_context(server, ioc);
  ioc.restart();
  ioc.run_for(50ms);
}

TEST(UdsServerWrapperLifecycleTest, StartFutureReflectsBindFailure) {
  boost::asio::io_context ioc;
  config::UdsServerConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uws-fail").string();
  test::TestUtils::removeFileIfExists(cfg.socket_path);

  auto* mock_acceptor = new test::mocks::MockUdsAcceptor();
  auto transport_server =
      transport::UdsServer::create(cfg, std::unique_ptr<interface::UdsAcceptorInterface>(mock_acceptor), ioc);

  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _)).WillOnce(Invoke([](const auto&, boost::system::error_code& ec) {
    ec = make_error_code(boost::asio::error::address_in_use);
  }));

  UdsServer server(std::static_pointer_cast<interface::Channel>(transport_server));
  auto started = server.start();

  ioc.restart();
  ioc.poll();

  ASSERT_EQ(started.wait_for(0ms), std::future_status::ready);
  EXPECT_FALSE(started.get());
  EXPECT_FALSE(server.listening());

  wirestead::test::stop_wrapper_with_context(server, ioc);
  ioc.restart();
  ioc.run_for(50ms);
}

TEST(UdsServerWrapperLifecycleTest, ManagedExternalContextStopsOnShutdown) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto socket_path = test::TestUtils::makeUniqueUdsSocketPath("uws-managed").string();
  test::TestUtils::removeFileIfExists(socket_path);

  UdsServer server(socket_path, ioc);
  server.manage_external_context(true);

  ioc->stop();
  auto started = server.start();
  ASSERT_EQ(started.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(started.get());

  EXPECT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return server.listening(); }, 2000));

  server.stop();
  EXPECT_TRUE(ioc->stopped());
  test::TestUtils::removeFileIfExists(socket_path);
}

TEST(UdsServerWrapperLifecycleTest, FramedMessageDoesNotDeadlock) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-framer");
  auto server = harness.start_server();
  std::atomic<int> messages{0};

  server->framer([]() { return std::make_unique<framer::LineFramer>(); });
  server->on_message([&](const MessageContext&) { messages++; });

  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));
  ASSERT_TRUE(client->send_line("hello"));

  EXPECT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return messages.load() == 1; }, 5000));
}

// Mirrors CumulativeStatsSurviveClientDisconnect in the TCP lifecycle suite.
// UdsServer aggregates its sessions the same way and lost their counters the
// same way when a client went. See docs/error_model.md.
TEST(UdsServerWrapperLifecycleTest, CumulativeStatsSurviveClientDisconnect) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-stats");
  auto server = harness.start_server();
  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));

  const std::string payload = "stats-survive-probe\n";
  ASSERT_TRUE(client->send(payload));
  ASSERT_TRUE(server->broadcast(payload));
  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition(
      [&]() {
        const auto s = server->stats();
        return s.bytes_received >= payload.size() && s.bytes_accepted >= payload.size();
      },
      10000));

  const auto connected = server->stats();
  ASSERT_GT(connected.bytes_received, 0u);
  ASSERT_GT(connected.bytes_accepted, 0u);

  client->stop();
  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return server->client_count() == 0; }, 10000));

  // client_count() drops a session as soon as it stops being alive, which is
  // before the server erases it, so assert the negative: give the teardown room
  // to run and require that the counters never collapse.
  const bool collapsed =
      wirestead::test::TestUtils::waitForCondition([&]() { return server->stats().bytes_received == 0; }, 2000);
  EXPECT_FALSE(collapsed) << "cumulative counters vanished along with the disconnected session";

  const auto after = server->stats();
  EXPECT_GE(after.bytes_received, connected.bytes_received);
  EXPECT_GE(after.bytes_accepted, connected.bytes_accepted);
  EXPECT_EQ(after.queued_bytes, 0u);
}

TEST(UdsServerWrapperLifecycleTest, ClientStatsAreReportedPerSession) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-client-stats");
  auto server = harness.start_server();
  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));

  const auto ids = server->connected_clients();
  ASSERT_EQ(ids.size(), 1u);

  const std::string payload(256, 'p');
  ASSERT_TRUE(client->send(payload));
  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition(
      [&]() {
        const auto s = server->client_stats(ids[0]);
        return s && s->bytes_received >= payload.size();
      },
      10000));

  const auto session = server->client_stats(ids[0]);
  ASSERT_TRUE(session.has_value());
  EXPECT_GE(server->stats().bytes_received, session->bytes_received);

  EXPECT_FALSE(server->client_stats(999999).has_value());

  client->stop();
  EXPECT_TRUE(
      wirestead::test::TestUtils::waitForCondition([&]() { return !server->client_stats(ids[0]).has_value(); }, 10000));
}

TEST(UdsServerWrapperContractTest, InjectedChannelStateAndFallbackOperations) {
  auto fake_channel = std::make_shared<test::wrapper_support::FakeChannel>();
  UdsServer server(std::static_pointer_cast<interface::Channel>(fake_channel));

  std::atomic<int> errors{0};
  server.on_error([&](const ErrorContext&) { errors++; });

  auto started = server.start();
  fake_channel->emit_state(base::LinkState::Listening);

  ASSERT_EQ(started.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(server.listening());

  auto second_start = server.start();
  ASSERT_EQ(second_start.wait_for(100ms), std::future_status::ready);
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

TEST(UdsServerWrapperLifecycleTest, RawDataBatchFlushesByLatency) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-raw-batch");
  auto server = harness.start_server();
  std::atomic<int> batch_count{0};
  std::vector<std::string> payloads;
  std::mutex payloads_mutex;

  server->batch_size(100).batch_latency(20ms);
  server->on_data_batch([&](const std::vector<MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(payloads_mutex);
    batch_count++;
    for (const auto& ctx : batch) payloads.push_back(ctx.data_as_string());
  });

  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));
  ASSERT_TRUE(client->send("raw-batch"));

  ASSERT_TRUE(test::TestUtils::waitForCondition([&]() { return batch_count.load() == 1; }, 5000));
  std::lock_guard<std::mutex> lock(payloads_mutex);
  ASSERT_EQ(payloads.size(), 1U);
  EXPECT_EQ(payloads[0], "raw-batch");
}

TEST(UdsServerWrapperLifecycleTest, FramedMessageBatchFlushesAtBatchSize) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-message-batch");
  auto server = harness.start_server();
  std::atomic<int> batch_count{0};
  std::vector<std::string> messages;
  std::mutex messages_mutex;

  server->batch_size(2).batch_latency(1s);
  server->framer([]() { return std::make_unique<framer::LineFramer>(); });
  server->on_message_batch([&](const std::vector<MessageContext>& batch) {
    std::lock_guard<std::mutex> lock(messages_mutex);
    batch_count++;
    for (const auto& ctx : batch) messages.push_back(ctx.data_as_string());
  });

  auto client = harness.connect_client();
  ASSERT_TRUE(harness.wait_for_client_count(1));
  ASSERT_TRUE(client->send("first\nsecond\n"));

  ASSERT_TRUE(test::TestUtils::waitForCondition([&]() { return batch_count.load() == 1; }, 5000));
  std::lock_guard<std::mutex> lock(messages_mutex);
  ASSERT_EQ(messages.size(), 2U);
  EXPECT_EQ(messages[0], "first");
  EXPECT_EQ(messages[1], "second");
}

TEST(UdsServerWrapperLifecycleTest, LineSendingVariantsReachConnectedClients) {
  test::wrapper_support::UdsServerLoopbackHarness harness("uws-line-sending");
  auto server = harness.start_server();
  server->backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);

  std::atomic<int> received{0};
  std::string received_data;
  std::mutex received_mutex;

  auto client = harness.connect_client();
  client->on_data([&](const MessageContext& ctx) {
    std::lock_guard<std::mutex> lock(received_mutex);
    received++;
    received_data += ctx.data_as_string();
  });
  ASSERT_TRUE(harness.wait_for_client_count(1));

  auto clients = server->connected_clients();
  ASSERT_EQ(clients.size(), 1U);
  const auto client_id = clients.front();

  EXPECT_TRUE(server->broadcast_line("broadcast"));
  EXPECT_TRUE(server->try_broadcast_line("try-broadcast"));
  EXPECT_TRUE(server->send_to_line(client_id, "send-to"));
  EXPECT_TRUE(server->try_send_to_line(client_id, "try-send-to"));

  // Wait for all 4 lines to arrive, not just the first - each send/broadcast
  // call can complete as a separate on_data invocation, so checking after
  // only one has arrived is a race (matches the already-correct wait
  // condition in TcpServerWrapperLifecycleTest's identically-named test).
  ASSERT_TRUE(test::TestUtils::waitForCondition(
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

TEST(UdsClientWrapperContractTest, HandlerReplacementUsesLatestCallback) {
  auto fake_channel = std::make_shared<test::wrapper_support::FakeChannel>();
  UdsClient client(fake_channel);

  std::atomic<int> connected{0};
  std::atomic<int> data{0};
  std::atomic<int> errors{0};

  client.on_connect([&](const ConnectionContext&) { connected = 1; });
  client.on_connect([&](const ConnectionContext&) { connected = 2; });

  client.on_data([&](const MessageContext&) { data = 1; });
  client.on_data([&](const MessageContext&) { data = 2; });

  client.on_error([&](const ErrorContext&) { errors = 1; });
  client.on_error([&](const ErrorContext&) { errors = 2; });

  fake_channel->emit_state(base::LinkState::Connected);
  fake_channel->emit_bytes("payload");
  fake_channel->emit_state(base::LinkState::Error);

  EXPECT_EQ(connected.load(), 2);
  EXPECT_EQ(data.load(), 2);
  EXPECT_EQ(errors.load(), 2);
}

TEST(UdsClientWrapperContractTest, StopSuppressesLateCallbacks) {
  auto fake_channel = std::make_shared<test::wrapper_support::FakeChannel>();
  UdsClient client(fake_channel);

  std::atomic<int> callbacks{0};
  client.on_connect([&](const ConnectionContext&) { callbacks++; });
  client.on_data([&](const MessageContext&) { callbacks++; });
  client.on_error([&](const ErrorContext&) { callbacks++; });
  client.on_disconnect([&](const wrapper::ConnectionContext&) { callbacks++; });

  auto f = client.start();
  client.stop();

  fake_channel->emit_state(base::LinkState::Connected);
  fake_channel->emit_bytes("payload");
  fake_channel->emit_state(base::LinkState::Error);
  fake_channel->emit_state(base::LinkState::Closed);

  EXPECT_EQ(callbacks.load(), 0);
}

TEST(UdsClientWrapperContractTest, InjectedChannelCoversHandlersAndSendVariants) {
  auto channel = std::make_shared<ControlledUdsChannel>();
  UdsClient client(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> connected{0};
  std::atomic<int> disconnected{0};
  std::atomic<int> errors{0};
  std::atomic<int> data{0};
  std::atomic<size_t> queued_bytes{0};
  std::string received;

  client.on_connect([&](const ConnectionContext&) { connected++; });
  client.on_disconnect([&](const ConnectionContext&) { disconnected++; });
  client.on_error([&](const ErrorContext&) { errors++; });
  client.on_data([&](const MessageContext& ctx) {
    data++;
    received = ctx.data_as_string();
  });
  client.on_backpressure([&](size_t queued) { queued_bytes = queued; });

  auto started = client.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_EQ(started.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(client.connected());
  EXPECT_EQ(connected.load(), 1);

  EXPECT_TRUE(client.send("abc"));
  EXPECT_TRUE(client.send_line("line"));
  EXPECT_TRUE(client.try_send("try"));
  EXPECT_TRUE(client.try_send_line("tryline"));
  EXPECT_EQ(channel->write_count(), 4);
  EXPECT_EQ(channel->last_write(), "tryline\n");

  channel->emit_bytes("payload");
  EXPECT_EQ(data.load(), 1);
  EXPECT_EQ(received, "payload");

  channel->emit_backpressure(4096);
  EXPECT_EQ(queued_bytes.load(), 4096U);

  channel->emit_state(base::LinkState::Closed);
  EXPECT_EQ(disconnected.load(), 1);

  channel->emit_state(base::LinkState::Error);
  EXPECT_EQ(errors.load(), 1);

  client.stop();
}

TEST(UdsClientWrapperContractTest, InjectedChannelBatchesRawDataAndFramedMessages) {
  auto channel = std::make_shared<ControlledUdsChannel>();
  UdsClient client(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> data_batches{0};
  std::atomic<int> message_batches{0};
  std::vector<std::string> raw_payloads;
  std::vector<std::string> framed_payloads;

  client.batch_size(2).batch_latency(1s);
  client.on_data_batch([&](const std::vector<MessageContext>& batch) {
    data_batches++;
    for (const auto& ctx : batch) raw_payloads.push_back(ctx.data_as_string());
  });

  auto started = client.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());

  channel->emit_bytes("raw1");
  EXPECT_EQ(data_batches.load(), 0);
  channel->emit_bytes("raw2");
  EXPECT_EQ(data_batches.load(), 1);
  ASSERT_EQ(raw_payloads.size(), 2U);
  EXPECT_EQ(raw_payloads[0], "raw1");
  EXPECT_EQ(raw_payloads[1], "raw2");

  client.framer(std::make_unique<framer::LineFramer>());
  client.on_message_batch([&](const std::vector<MessageContext>& batch) {
    message_batches++;
    for (const auto& ctx : batch) framed_payloads.push_back(ctx.data_as_string());
  });

  channel->emit_bytes("msg1\nmsg2\n");
  EXPECT_EQ(message_batches.load(), 1);
  ASSERT_EQ(framed_payloads.size(), 2U);
  EXPECT_EQ(framed_payloads[0], "msg1");
  EXPECT_EQ(framed_payloads[1], "msg2");

  client.stop();
}

TEST(UdsClientWrapperContractTest, StartWhileConnectedAndBestEffortWriteFailure) {
  auto channel = std::make_shared<ControlledUdsChannel>();
  UdsClient client(std::static_pointer_cast<interface::Channel>(channel));

  auto started = client.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());

  auto second_start = client.start();
  ASSERT_EQ(second_start.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(second_start.get());

  client.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);
  channel->set_write_result(false);
  EXPECT_FALSE(client.send("drop"));
  EXPECT_FALSE(client.send_line("drop-line"));
  EXPECT_EQ(channel->write_count(), 2);

  client.stop();
}

TEST(UdsClientWrapperContractTest, ConfigurationSettersBeforeStartRemainFluent) {
  UdsClient client(test::TestUtils::makeUniqueUdsSocketPath("uwc-config").string());

  EXPECT_EQ(&client, &client.retry_interval(7ms));
  EXPECT_EQ(&client, &client.max_retries(3));
  EXPECT_EQ(&client, &client.connection_timeout(25ms));
  EXPECT_EQ(&client, &client.backpressure_threshold(512));
  EXPECT_EQ(&client, &client.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort));
  EXPECT_EQ(&client, &client.batch_size(3));
  EXPECT_EQ(&client, &client.batch_latency(15ms));
  EXPECT_EQ(&client, &client.manage_external_context(false));
}

TEST(UdsServerWrapperContractTest, ConnectHandlerReplacementUsesLatestCallback) {
  boost::asio::io_context ioc;
  config::UdsServerConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uds-server-contract").string();
  test::TestUtils::removeFileIfExists(cfg.socket_path);

  auto* mock_acceptor = new test::mocks::MockUdsAcceptor();
  auto transport_server =
      transport::UdsServer::create(cfg, std::unique_ptr<interface::UdsAcceptorInterface>(mock_acceptor), ioc);

  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, listen(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, close(_)).Times(testing::AnyNumber()).WillRepeatedly(Return());
  EXPECT_CALL(*mock_acceptor, async_accept(_))
      .WillOnce(Invoke([&ioc](auto handler) {
        auto socket = boost::asio::local::stream_protocol::socket(ioc);
        boost::asio::post(ioc, [handler = std::move(handler), socket = std::move(socket)]() mutable {
          handler({}, std::move(socket));
        });
      }))
      .WillRepeatedly(Invoke([](auto) {}));

  std::atomic<int> count{0};
  UdsServer server(std::static_pointer_cast<interface::Channel>(transport_server));
  server.on_connect([&](const ConnectionContext&) { count = 1; });
  server.on_connect([&](const ConnectionContext&) { count = 2; });

  auto started = server.start();
  ioc.restart();
  ioc.run_for(100ms);

  ASSERT_EQ(started.wait_for(0ms), std::future_status::ready);
  ASSERT_TRUE(started.get());
  ASSERT_TRUE(wirestead::test::TestUtils::waitForCondition([&]() { return count.load() > 0; }, 5000));
  EXPECT_EQ(count.load(), 2);

  wirestead::test::stop_wrapper_with_context(server, ioc);
  ioc.restart();
  ioc.run_for(50ms);
}

}  // namespace
}  // namespace wirestead::wrapper

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> nonblocking_result;
thread_local int nonblocking_observations = 0;
void observe_nonblocking_result(const wirestead::wrapper::SendResult& result) {
  nonblocking_result = result;
  ++nonblocking_observations;
}

struct NonblockingResultPeer {
  boost::asio::io_context io;
  std::string path = wirestead::test::TestUtils::makeUniqueUdsSocketPath("uds-result").string();
  // Windows AF_UNIX bind rejects SO_REUSEADDR; match the native server's open/bind/listen path.
  boost::asio::local::stream_protocol::acceptor acceptor{io, boost::asio::local::stream_protocol::endpoint(path),
                                                         false};
  std::shared_ptr<wirestead::transport::UdsClient> native;
  std::unique_ptr<wirestead::wrapper::UdsClient> client;
  explicit NonblockingResultPeer(bool best_effort) {
    wirestead::config::UdsClientConfig cfg;
    cfg.socket_path = path;
    cfg.backpressure_threshold = 1024;
    cfg.backpressure_strategy = best_effort ? wirestead::base::constants::BackpressureStrategy::BestEffort
                                            : wirestead::base::constants::BackpressureStrategy::Reliable;
    native = wirestead::transport::UdsClient::create(cfg, io);
    client = std::make_unique<wirestead::wrapper::UdsClient>(native);
    client->backpressure_strategy(cfg.backpressure_strategy);
    wirestead::wrapper::detail::g_uds_send_result_hook.store(observe_nonblocking_result);
  }
  ~NonblockingResultPeer() {
    wirestead::wrapper::detail::g_uds_send_result_hook.store(nullptr);
    wirestead::test::stop_wrapper_with_context(*client, io);
    acceptor.close();
    wirestead::test::TestUtils::removeFileIfExists(path);
  }
  template <typename Predicate>
  bool until(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_for(std::chrono::milliseconds(10));
    }
    return true;
  }
};

class UdsNonblockingResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsNonblockingResultTest, OrdersValidationLifecycleAndCapacityReasons) {
  using Rejection = wirestead::wrapper::SendRejection;
  const bool best_effort = GetParam() >= 4 && GetParam() < 8;
  const int form = GetParam() % 4;
  // Explicit try methods must stay WouldBlock even on a BestEffort channel.
  NonblockingResultPeer peer(GetParam() >= 4);
  auto& client = *peer.client;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    bool accepted = false;
    if (form == 0) {
      accepted = best_effort ? client.send(text) : client.try_send(text);
    } else if (form == 1) {
      accepted = best_effort ? client.send_line(text) : client.try_send_line(text);
    } else if (form == 2) {
      std::vector<uint8_t> payload(text.begin(), text.end());
      accepted = best_effort ? client.send_move(std::move(payload)) : client.try_send_move(std::move(payload));
      if (!accepted) {
        EXPECT_EQ(std::string(payload.begin(), payload.end()), text);
      }
    } else {
      auto payload = std::make_shared<const std::vector<uint8_t>>(text.begin(), text.end());
      accepted = best_effort ? client.send_shared(payload) : client.try_send_shared(payload);
    }
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (nonblocking_result) {
      EXPECT_EQ(nonblocking_result->accepted(), accepted);
    }
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };

  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  if (form != 1) {
    EXPECT_FALSE(write(""));
    reason(Rejection::InvalidArgument);
  }
  // Line delimiters count against the hard queue limit before state/capacity.
  const std::string oversized(*peer.native->write_queue_limit() + (form == 1 ? 0 : 1), 'x');
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  EXPECT_EQ(peer.native->stats().failed_sends, 0u);

  auto started = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  // Fill high-water without running the executor again.
  ASSERT_TRUE(client.try_send(std::string(1024, 'f')));
  EXPECT_FALSE(write("valid"));
  reason(best_effort ? Rejection::QueueFull : Rejection::WouldBlock);
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);

  bool stopped_on_executor = false;
  boost::asio::post(peer.io, [&] {
    client.stop();
    EXPECT_FALSE(write("valid"));
    reason(Rejection::Stopping);
    EXPECT_FALSE(write(oversized));
    reason(Rejection::TooLarge);
    stopped_on_executor = true;
  });
  ASSERT_TRUE(peer.until([&] { return stopped_on_executor; }));
  wirestead::test::stop_wrapper_with_context(client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  auto restarted = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(restarted.get());
  EXPECT_TRUE(write("valid"));
}

INSTANTIATE_TEST_SUITE_P(TryAndBestEffortForms, UdsNonblockingResultTest, ::testing::Range(0, 12));

TEST(UdsNonblockingResultContract, ConnectedNativeStillRequiresWrapperStart) {
  using Rejection = wirestead::wrapper::SendRejection;
  NonblockingResultPeer peer(false);
  peer.native->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  EXPECT_FALSE(peer.client->try_send("before wrapper start"));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), Rejection::NotStarted);
  EXPECT_EQ(peer.native->stats().messages_accepted, 0u);
  ASSERT_TRUE(peer.client->start().get());
  EXPECT_TRUE(peer.client->try_send("after wrapper start"));
}

TEST(UdsNonblockingResultContract, NullSharedPayloadIsInvalidBeforeStartAndAfterStop) {
  NonblockingResultPeer peer(true);
  EXPECT_FALSE(peer.client->send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
  peer.client->stop();
  EXPECT_FALSE(peer.client->try_send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
}

class UdsReliableResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsReliableResultTest, ValidatesBeforeStateAndPreservesPayload) {
  using Rejection = wirestead::wrapper::SendRejection;
  // The last two cases exercise explicit blocking under BestEffort.
  NonblockingResultPeer peer(GetParam() >= 6);
  const int form = GetParam() >= 6 ? GetParam() - 4 : GetParam();
  auto& client = *peer.client;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    bool accepted = false;
    if (form == 0)
      accepted = client.send(text);
    else if (form == 1)
      accepted = client.send_line(text);
    else if (form == 2)
      accepted = client.send_blocking(text);
    else if (form == 3)
      accepted = client.send_line_blocking(text);
    else if (form == 4) {
      std::vector<uint8_t> payload(text.begin(), text.end());
      accepted = client.send_move(std::move(payload));
      if (!accepted) {
        EXPECT_EQ(std::string(payload.begin(), payload.end()), text);
      }
    } else
      accepted = client.send_shared(std::make_shared<const std::vector<uint8_t>>(text.begin(), text.end()));
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (nonblocking_result) {
      EXPECT_EQ(nonblocking_result->accepted(), accepted);
    }
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };
  const bool line = form == 1 || form == 3;
  const std::string oversized(*peer.native->write_queue_limit() + (line ? 0 : 1), 'x');
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  if (!line) {
    EXPECT_FALSE(write(""));
    reason(Rejection::InvalidArgument);
  }
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  EXPECT_EQ(peer.native->stats().failed_sends, 0u);
  auto started = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  {
    wirestead::wrapper::detail::CallbackGuard guard;
    EXPECT_TRUE(write("capacity available in callback"));
  }
  wirestead::test::stop_wrapper_with_context(client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  EXPECT_FALSE(client.send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), Rejection::InvalidArgument);
}
INSTANTIATE_TEST_SUITE_P(ReliableAndExplicitBlocking, UdsReliableResultTest, ::testing::Range(0, 8));

TEST(UdsReliableResultContract, RetriesOnlyCapacityAndPreservesMoveStorage) {
  NonblockingResultPeer peer(false);
  auto started = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  // Keep the executor paused: inflight reservations fill the hard limit but
  // have not yet published high-water pressure, exercising bounded retries.
  ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit(), 'f')));
  ASSERT_FALSE(peer.native->is_backpressure_active());
  const auto failures = peer.native->stats().failed_sends;
  for (int form = 0; form < 3; ++form) {
    nonblocking_observations = 0;
    std::vector<uint8_t> payload{1, 2, 3};
    if (form == 0) {
      EXPECT_FALSE(peer.client->send_blocking("copy"));
    } else if (form == 1) {
      EXPECT_FALSE(peer.client->send_move(std::move(payload)));
      EXPECT_EQ(payload, (std::vector<uint8_t>{1, 2, 3}));
    } else {
      EXPECT_FALSE(peer.client->send_shared(std::make_shared<const std::vector<uint8_t>>(payload)));
    }
    ASSERT_TRUE(nonblocking_result.has_value());
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::WouldBlock);
    EXPECT_EQ(peer.native->stats().failed_sends, failures + 5 * (form + 1));
    EXPECT_EQ(peer.native->stats().messages_accepted, 1u);
  }
}
}  // namespace

namespace {
class UdsNativeResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsNativeResultTest, ReportsLifecycleValidationAndCapacityAtAdmission) {
  using Rejection = wirestead::wrapper::SendRejection;
  NonblockingResultPeer peer(GetParam() >= 6);
  wirestead::transport::detail::g_uds_write_result_hook.store(observe_nonblocking_result);
  struct ResetHook {
    ~ResetHook() { wirestead::transport::detail::g_uds_write_result_hook.store(nullptr); }
  } reset_hook;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    std::vector<uint8_t> data(text.begin(), text.end());
    bool accepted;
    switch (GetParam() % 6) {
      case 0:
        accepted = peer.native->async_write_copy({data.data(), data.size()});
        break;
      case 1:
        accepted = peer.native->async_write_move(std::move(data));
        break;
      case 2:
        accepted = peer.native->async_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
      case 3:
        accepted = peer.native->async_try_write_copy({data.data(), data.size()});
        break;
      case 4:
        accepted = peer.native->async_try_write_move(std::move(data));
        break;
      default:
        accepted = peer.native->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
    }
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (!accepted) {
      EXPECT_EQ(std::string(data.begin(), data.end()), text);
    }
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  auto started = peer.client->start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  EXPECT_FALSE(write(""));
  reason(Rejection::InvalidArgument);
  EXPECT_TRUE(write("accepted"));
  ASSERT_TRUE(nonblocking_result->accepted());
  if (GetParam() % 6 < 3) {
    ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit() - 8, 'f')));
  } else {
    ASSERT_TRUE(peer.native->async_try_write_move(std::vector<uint8_t>(1024 - 8, 'f')));
  }
  EXPECT_FALSE(write("valid"));
  reason(Rejection::WouldBlock);
  // Stop on the executor, while completion is still pending.
  bool requested = false;
  boost::asio::post(peer.io, [&] {
    peer.native->stop();
    EXPECT_FALSE(write("valid"));
    reason(Rejection::Stopping);
    requested = true;
  });
  ASSERT_TRUE(peer.until([&] { return requested; }));
  wirestead::test::stop_wrapper_with_context(*peer.client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
}
INSTANTIATE_TEST_SUITE_P(FormsAndStrategies, UdsNativeResultTest, ::testing::Range(0, 12));
}  // namespace

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> target_result;
thread_local int target_observations = 0;
void observe_target_result(const wirestead::wrapper::SendResult& result) {
  target_result = result;
  ++target_observations;
}
class UdsServerTargetResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsServerTargetResultTest, CombinesValidationLifecycleAndSessionAdmission) {
  using namespace wirestead;
  using Rejection = wrapper::SendRejection;
  boost::asio::io_context io;
  config::UdsServerConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("target-result").string();
  cfg.backpressure_threshold = 1024;
  cfg.backpressure_strategy = GetParam() >= 2 ? base::constants::BackpressureStrategy::BestEffort
                                              : base::constants::BackpressureStrategy::Reliable;
  auto native = transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
  wrapper::UdsServer server(native);
  server.backpressure_strategy(cfg.backpressure_strategy);
  wrapper::detail::g_uds_server_send_result_hook.store(observe_target_result);
  transport::detail::g_uds_server_write_result_hook.store(observe_target_result);
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    wrapper::detail::g_uds_server_send_result_hook.store(nullptr);
    transport::detail::g_uds_server_write_result_hook.store(nullptr);
    test::stop_wrapper_with_context(server, io);
    test::TestUtils::removeFileIfExists(cfg.socket_path);
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
  auto write = [&](std::string_view data) {
    target_result.reset();
    target_observations = 0;
    bool accepted;
    if (native_form) {
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
      accepted = GetParam() % 2 ? server.send_to_line(id, data) : server.send_to(id, data);
    } else {
      accepted = GetParam() % 2 ? server.try_send_to_line(id, data) : server.try_send_to(id, data);
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
  if (!native_form && GetParam() % 2 == 0) {
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
  boost::asio::local::stream_protocol::socket peer(io);
  peer.connect(boost::asio::local::stream_protocol::endpoint(cfg.socket_path));
  ASSERT_TRUE(until([&] { return native->client_count() == 1; }));
  id = native->connected_clients().front();
  if (!native_form) {
    const auto failures = native->stats().failed_sends;
    EXPECT_FALSE(write(std::string(*native->write_queue_limit(id) + 1, 'x')));
    reason(Rejection::TooLarge);
    EXPECT_EQ(native->stats().failed_sends, failures);
  }
  EXPECT_TRUE(write("ok"));
  const bool ordinary = native_form && GetParam() % 2 == 0;
  const size_t used = !native_form && GetParam() % 2 ? 3 : 2;
  const auto fill = ordinary ? *native->write_queue_limit(id) - used : 1024 - used;
  if (ordinary)
    ASSERT_TRUE(native->send_to_client(id, std::string(fill, 'f')));
  else
    ASSERT_TRUE(native->try_send_to_client(id, std::string(fill, 'f')));
  EXPECT_FALSE(write("full"));
  reason(GetParam() >= 2 && GetParam() < 4 ? Rejection::QueueFull : Rejection::WouldBlock);
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
INSTANTIATE_TEST_SUITE_P(WrapperAndNativeForms, UdsServerTargetResultTest, ::testing::Range(0, 11));
}  // namespace
