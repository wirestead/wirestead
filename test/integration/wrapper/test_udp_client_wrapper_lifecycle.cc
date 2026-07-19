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
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wrapper_contract_test_utils.hpp"

using namespace wirestead;

namespace wirestead {
namespace test {

namespace {

class ControlledUdpChannel : public interface::Channel {
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
    if (state == base::LinkState::Connected || state == base::LinkState::Listening) {
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

}  // namespace

TEST(UdpClientWrapperLifecycleTest, StartMultipleTimes) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  wrapper::UdpClient client(cfg);

  (void)client.start();
  (void)client.start();
}

TEST(UdpClientWrapperLifecycleTest, ExternalIoContextManagement) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  config::UdpConfig cfg;
  cfg.local_port = 0;

  wrapper::UdpClient client(cfg, ioc);
  client.manage_external_context(true);

  (void)client.start();
  EXPECT_FALSE(ioc->stopped());

  client.stop();
  EXPECT_TRUE(ioc->stopped());
}

TEST(UdpClientWrapperLifecycleTest, SendWithoutConnection) {
  config::UdpConfig cfg;
  wrapper::UdpClient client(cfg);
  EXPECT_FALSE(client.send("data"));
  EXPECT_FALSE(client.send_line("line"));
}

TEST(UdpClientWrapperLifecycleTest, FramerIntegration) {
  config::UdpConfig cfg;
  wrapper::UdpClient client(cfg);
  client.framer(std::make_unique<framer::LineFramer>());
  client.on_message([](const wrapper::MessageContext&) {});
}

TEST(UdpClientWrapperLifecycleTest, MessageBatchHandlerAttachesAfterFramer) {
  auto fake_channel = std::make_shared<wrapper_support::FakeChannel>();
  wrapper::UdpClient client(fake_channel);
  std::atomic<int> batches{0};

  client.framer(std::make_unique<framer::LineFramer>());
  client.on_message_batch(
      [&](const std::vector<wrapper::MessageContext>& messages) { batches += static_cast<int>(messages.size()); });

  std::string payload;
  for (int i = 0; i < 100; ++i) {
    payload += "payload\n";
  }
  fake_channel->emit_bytes(payload);

  EXPECT_EQ(batches.load(), 100);
}

TEST(UdpClientWrapperLifecycleTest, InjectedChannelCoversHandlersAndSendVariants) {
  auto channel = std::make_shared<ControlledUdpChannel>();
  wrapper::UdpClient client(std::static_pointer_cast<interface::Channel>(channel));

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

  channel->emit_backpressure(1024);
  EXPECT_EQ(queued_bytes.load(), 1024U);

  channel->emit_state(base::LinkState::Closed);
  EXPECT_EQ(disconnected.load(), 1);

  channel->emit_state(base::LinkState::Error);
  EXPECT_EQ(errors.load(), 1);

  client.stop();
}

TEST(UdpClientWrapperLifecycleTest, InjectedChannelBatchesRawDataAndFramedMessages) {
  auto channel = std::make_shared<ControlledUdpChannel>();
  wrapper::UdpClient client(std::static_pointer_cast<interface::Channel>(channel));

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

TEST(UdpClientWrapperLifecycleTest, StartWhileConnectedAndBestEffortWriteFailure) {
  auto channel = std::make_shared<ControlledUdpChannel>();
  wrapper::UdpClient client(std::static_pointer_cast<interface::Channel>(channel));

  auto started = client.start();
  channel->emit_state(base::LinkState::Listening);
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

TEST(UdpClientWrapperLifecycleTest, ConfigurationSettersBeforeStartRemainFluent) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  wrapper::UdpClient client(cfg);

  EXPECT_EQ(&client, &client.backpressure_threshold(256));
  EXPECT_EQ(&client, &client.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort));
  EXPECT_EQ(&client, &client.batch_size(3));
  EXPECT_EQ(&client, &client.batch_latency(std::chrono::milliseconds(15)));
  EXPECT_EQ(&client, &client.manage_external_context(false));
}

TEST(UdpClientWrapperLifecycleTest, StopWithoutStart) {
  config::UdpConfig cfg;
  wrapper::UdpClient client(cfg);
  client.stop();
}

// Regression test for jwsung91/wirestead#444: Impl::stop() used to null the
// channel's callbacks (including on_state, which is what fulfills the
// start() future) without releasing the channel object itself. Since
// start()'s `if (!channel)` guard is the only place that re-runs
// setup_internal_handlers(), a restarted channel's handlers stayed null
// forever, and the second start()'s future never resolved. Uses wait_for()
// with a bounded timeout rather than a bare get(), so this test fails
// loudly instead of hanging the suite if the fix regresses.
TEST(UdpClientWrapperLifecycleTest, RestartAfterStopResolvesFutureAndReinstallsHandlers) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  wrapper::UdpClient client(cfg);

  std::atomic<int> connect_count{0};
  client.on_connect([&](const wrapper::ConnectionContext&) { connect_count++; });

  auto first = client.start();
  ASSERT_EQ(first.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_TRUE(first.get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connect_count.load() == 1; }, 1000));

  client.stop();

  auto second = client.start();
  ASSERT_EQ(second.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_TRUE(second.get());
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connect_count.load() == 2; }, 1000));

  client.stop();
}

}  // namespace test
}  // namespace wirestead
