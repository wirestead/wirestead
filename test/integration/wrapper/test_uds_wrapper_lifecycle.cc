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
#include <string>
#include <vector>

#include "test/mocks/mock_uds_acceptor.hpp"
#include "test/mocks/mock_uds_socket.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
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

  client.stop();
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

  client.stop();
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

  server.stop();
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

  server.stop();
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

  server.stop();
  ioc.restart();
  ioc.run_for(50ms);
}

}  // namespace
}  // namespace wirestead::wrapper
