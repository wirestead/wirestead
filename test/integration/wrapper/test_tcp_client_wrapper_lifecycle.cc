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
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "unilink/framer/line_framer.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/unilink.hpp"
#include "wrapper_contract_test_utils.hpp"

namespace {

using namespace unilink;
using namespace unilink::test;
using namespace std::chrono_literals;

class ControlledChannel : public interface::Channel {
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

class TcpClientWrapperLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_port_ = TestUtils::getAvailableTestPort();
    server_ = std::make_shared<wrapper::TcpServer>(test_port_);
    server_->start();
  }

  void TearDown() override {
    if (client_) client_->stop();
    if (server_) server_->stop();
  }

  uint16_t test_port_;
  std::shared_ptr<wrapper::TcpServer> server_;
  std::shared_ptr<wrapper::TcpClient> client_;
};

TEST_F(TcpClientWrapperLifecycleTest, ClientStartStopMultipleTimes) {
  client_ = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  for (int i = 0; i < 3; ++i) {
    auto f = client_->start();
    EXPECT_TRUE(f.get());
    EXPECT_TRUE(client_->connected());
    client_->stop();
    EXPECT_FALSE(client_->connected());
  }
}

TEST_F(TcpClientWrapperLifecycleTest, ExternalContextNotStoppedWhenNotManaged) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work = boost::asio::make_work_guard(*ioc);

  client_ = std::make_shared<wrapper::TcpClient>("127.0.0.1", test_port_, ioc);
  client_->start();

  std::thread t([&]() { ioc->run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  client_->stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(ioc->stopped());

  work.reset();
  ioc->stop();
  if (t.joinable()) t.join();
}

TEST_F(TcpClientWrapperLifecycleTest, ExternalContextManagedRunsAndStops) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  client_ = std::make_shared<wrapper::TcpClient>("127.0.0.1", test_port_, ioc);
  client_->manage_external_context(true);
  client_->start();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000));
  client_->stop();
  // With graceful shutdown, the io_context loop will finish naturally.
  // We use waitForCondition to give it a moment to finish its run loop.
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return ioc->stopped() || ioc->poll() == 0; }, 1000));
}

// #454: the TCP client wrapper used to be the only one of six wrappers
// polling an externally-managed io_context with run_one_for(50ms) instead of
// blocking on run() woken by a stop_callback. Confirms stop() still returns
// promptly (well under the old 50ms poll interval) with the new pattern.
TEST_F(TcpClientWrapperLifecycleTest, ManagedExternalContextStopReturnsPromptly) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  client_ = std::make_shared<wrapper::TcpClient>("127.0.0.1", test_port_, ioc);
  client_->manage_external_context(true);
  client_->start();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000));

  auto begin = std::chrono::steady_clock::now();
  client_->stop();
  auto elapsed = std::chrono::steady_clock::now() - begin;

  EXPECT_LT(elapsed, std::chrono::milliseconds(50));
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return ioc->stopped() || ioc->poll() == 0; }, 1000));
}

TEST_F(TcpClientWrapperLifecycleTest, ManagedExternalContextRestartsStoppedIoContext) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  ioc->stop();

  client_ = std::make_shared<wrapper::TcpClient>("127.0.0.1", test_port_, ioc);
  client_->manage_external_context(true);

  auto started = client_->start();
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000));

  client_->stop();
  EXPECT_TRUE(ioc->stopped());
}

TEST_F(TcpClientWrapperLifecycleTest, AutoManageStartsClientAndInvokesCallback) {
  std::atomic<bool> connected{false};
  client_ = unilink::tcp_client("127.0.0.1", test_port_)
                .auto_start(true)
                .on_connect([&](const wrapper::ConnectionContext&) { connected = true; })
                .on_data([](auto&&) {})
                .on_error([](auto&&) {})
                .build();

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connected.load(); }, 10000));
}

TEST_F(TcpClientWrapperLifecycleTest, SendMultipleMessages) {
  std::atomic<int> received{0};
  // Ensure handler is registered BEFORE anything starts
  server_->on_data([&](const wrapper::MessageContext&) { received++; });

  client_ = unilink::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  client_->start();

  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000));

  // Give a small stabilization delay
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  for (int i = 0; i < 5; ++i) {
    client_->send("msg");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Safe interval
  }

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return received.load() >= 5; }, 5000));
}

TEST_F(TcpClientWrapperLifecycleTest, IdleTimeoutReconnectsByDefault) {
  std::atomic<int> connected{0};
  client_ = unilink::tcp_client("127.0.0.1", test_port_)
                .retry_interval(100ms)
                .idle_timeout(100ms)
                .on_connect([&](const wrapper::ConnectionContext&) { connected++; })
                .on_data([](auto&&) {})
                .on_error([](auto&&) {})
                .build();

  auto started = client_->start();
  ASSERT_TRUE(started.get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connected.load() >= 2; }, 3000));
  EXPECT_TRUE(client_->connected());
}

TEST_F(TcpClientWrapperLifecycleTest, IdleTimeoutCloseActionClosesWithoutReconnect) {
  std::atomic<int> connected{0};
  std::atomic<int> disconnected{0};
  client_ = unilink::tcp_client("127.0.0.1", test_port_)
                .idle_timeout(100ms)
                .idle_timeout_action(IdleTimeoutAction::Close)
                .on_connect([&](const wrapper::ConnectionContext&) { connected++; })
                .on_disconnect([&](const wrapper::ConnectionContext&) { disconnected++; })
                .on_data([](auto&&) {})
                .on_error([](auto&&) {})
                .build();

  auto started = client_->start();
  ASSERT_TRUE(started.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return connected.load() == 1; }, 1000));
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return disconnected.load() == 1; }, 3000));
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(connected.load(), 1);
  EXPECT_FALSE(client_->connected());
}

TEST_F(TcpClientWrapperLifecycleTest, IdleTimeoutResetsOnInboundData) {
  constexpr std::string_view payload = "tick";
  std::atomic<int> connected{0};
  std::atomic<int> disconnected{0};
  std::atomic<size_t> received_bytes{0};

  client_ = unilink::tcp_client("127.0.0.1", test_port_)
                .idle_timeout(1000ms)
                .idle_timeout_action(IdleTimeoutAction::Close)
                .on_connect([&](const wrapper::ConnectionContext&) { connected++; })
                .on_disconnect([&](const wrapper::ConnectionContext&) { disconnected++; })
                .on_data([&](const wrapper::MessageContext& ctx) { received_bytes += ctx.data().size(); })
                .on_error([](auto&&) {})
                .build();

  auto started = client_->start();
  ASSERT_TRUE(started.get());
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return connected.load() == 1; }, 1000));
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->client_count() == 1; }, 1000));

  for (int i = 1; i <= 6; ++i) {
    std::this_thread::sleep_for(200ms);
    ASSERT_TRUE(server_->broadcast(payload));
    ASSERT_TRUE(TestUtils::waitForCondition(
        [&]() { return received_bytes.load() >= payload.size() * static_cast<size_t>(i); }, 1000));
    EXPECT_EQ(disconnected.load(), 0);
    EXPECT_TRUE(client_->connected());
  }
}

TEST(TcpClientWrapperContractTest, HandlerReplacementUsesLatestCallback) {
  auto fake_channel = std::make_shared<wrapper_support::FakeChannel>();
  wrapper::TcpClient client(fake_channel);

  std::atomic<int> connected{0};
  std::atomic<int> data{0};
  std::atomic<int> errors{0};

  client.on_connect([&](const wrapper::ConnectionContext&) { connected = 1; });
  client.on_connect([&](const wrapper::ConnectionContext&) { connected = 2; });

  client.on_data([&](const wrapper::MessageContext&) { data = 1; });
  client.on_data([&](const wrapper::MessageContext&) { data = 2; });

  client.on_error([&](const wrapper::ErrorContext&) { errors = 1; });
  client.on_error([&](const wrapper::ErrorContext&) { errors = 2; });

  fake_channel->emit_state(base::LinkState::Connected);
  fake_channel->emit_bytes("payload");
  fake_channel->emit_state(base::LinkState::Error);

  EXPECT_EQ(connected.load(), 2);
  EXPECT_EQ(data.load(), 2);
  EXPECT_EQ(errors.load(), 2);
}

TEST(TcpClientWrapperContractTest, StopSuppressesLateCallbacks) {
  auto fake_channel = std::make_shared<wrapper_support::FakeChannel>();
  wrapper::TcpClient client(fake_channel);

  std::atomic<int> callbacks{0};
  client.on_connect([&](const wrapper::ConnectionContext&) { callbacks++; });
  client.on_data([&](const wrapper::MessageContext&) { callbacks++; });
  client.on_error([&](const wrapper::ErrorContext&) { callbacks++; });
  client.on_disconnect([&](const wrapper::ConnectionContext&) { callbacks++; });

  auto f = client.start();
  client.stop();

  fake_channel->emit_state(base::LinkState::Connected);
  fake_channel->emit_bytes("payload");
  fake_channel->emit_state(base::LinkState::Error);
  fake_channel->emit_state(base::LinkState::Closed);

  EXPECT_EQ(callbacks.load(), 0);
}

TEST(TcpClientWrapperContractTest, BestEffortDisconnectedSendReturnsFalse) {
  auto fake_channel = std::make_shared<wrapper_support::FakeChannel>();
  wrapper::TcpClient client(fake_channel);
  client.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);

  EXPECT_FALSE(client.try_send("payload"));
  EXPECT_FALSE(client.send("payload"));
  EXPECT_EQ(fake_channel->write_count(), 0);

  fake_channel->emit_state(base::LinkState::Connected);
  EXPECT_EQ(fake_channel->write_count(), 0);
}

TEST(TcpClientWrapperContractTest, InjectedChannelCoversHandlersAndSendVariants) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::TcpClient client(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> connected{0};
  std::atomic<int> disconnected{0};
  std::atomic<int> errors{0};
  std::atomic<int> data{0};
  std::atomic<size_t> queued_bytes{0};
  std::string received;

  client.on_connect([&](const wrapper::ConnectionContext&) { connected++; });
  client.on_disconnect([&](const wrapper::ConnectionContext&) { disconnected++; });
  client.on_error([&](const wrapper::ErrorContext&) { errors++; });
  client.on_data([&](const wrapper::MessageContext& ctx) {
    data++;
    received = ctx.data_as_string();
  });
  client.on_backpressure([&](size_t queued) { queued_bytes = queued; });

  auto started = client.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_EQ(started.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
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

  channel->emit_backpressure(2048);
  EXPECT_EQ(queued_bytes.load(), 2048U);

  channel->emit_state(base::LinkState::Closed);
  EXPECT_EQ(disconnected.load(), 1);

  channel->emit_state(base::LinkState::Error);
  EXPECT_EQ(errors.load(), 1);

  client.stop();
}

TEST(TcpClientWrapperContractTest, InjectedChannelBatchesRawDataAndFramedMessages) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::TcpClient client(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> data_batches{0};
  std::atomic<int> message_batches{0};
  std::vector<std::string> raw_payloads;
  std::vector<std::string> framed_payloads;

  client.batch_size(2).batch_latency(std::chrono::seconds(1));
  client.on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
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
  client.on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
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

TEST(TcpClientWrapperContractTest, StartWhileConnectedAndBestEffortWriteFailure) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::TcpClient client(std::static_pointer_cast<interface::Channel>(channel));

  auto started = client.start();
  channel->emit_state(base::LinkState::Connected);
  ASSERT_TRUE(started.get());

  auto second_start = client.start();
  ASSERT_EQ(second_start.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_TRUE(second_start.get());

  client.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);
  channel->set_write_result(false);
  EXPECT_FALSE(client.send("drop"));
  EXPECT_FALSE(client.send_line("drop-line"));
  EXPECT_EQ(channel->write_count(), 2);

  client.stop();
}

}  // namespace
