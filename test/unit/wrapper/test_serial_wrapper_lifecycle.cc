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
#include <boost/system/error_code.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "test/utils/test_utils.hpp"
#include "unilink/config/serial_config.hpp"
#include "unilink/framer/line_framer.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/interface/iserial_port.hpp"
#include "unilink/transport/serial/serial.hpp"
#include "unilink/unilink.hpp"

using namespace unilink;
using namespace unilink::test;
using namespace std::chrono_literals;

namespace {

class FakeSerialPort : public interface::SerialPortInterface {
 public:
  explicit FakeSerialPort(boost::asio::io_context& ioc, boost::system::error_code open_ec = {})
      : ioc_(ioc), open_ec_(open_ec) {}

  void open(const std::string&, boost::system::error_code& ec) override {
    ec = open_ec_;
    open_ = !ec;
  }

  bool is_open() const override { return open_; }

  void close(boost::system::error_code& ec) override {
    open_ = false;
    ec.clear();
    emit_operation_aborted();
  }

  void set_option(const boost::asio::serial_port_base::baud_rate&, boost::system::error_code& ec) override {
    ec.clear();
  }
  void set_option(const boost::asio::serial_port_base::character_size&, boost::system::error_code& ec) override {
    ec.clear();
  }
  void set_option(const boost::asio::serial_port_base::stop_bits&, boost::system::error_code& ec) override {
    ec.clear();
  }
  void set_option(const boost::asio::serial_port_base::parity&, boost::system::error_code& ec) override { ec.clear(); }
  void set_option(const boost::asio::serial_port_base::flow_control&, boost::system::error_code& ec) override {
    ec.clear();
  }

  void async_read_some(const boost::asio::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const boost::asio::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    auto size = buffer.size();
    boost::asio::post(ioc_, [handler = std::move(handler), size]() { handler({}, size); });
  }

 private:
  void emit_operation_aborted() {
    if (!read_handler_) return;
    auto handler = std::move(read_handler_);
    boost::asio::post(
        ioc_, [handler = std::move(handler)]() { handler(make_error_code(boost::asio::error::operation_aborted), 0); });
  }

  boost::asio::io_context& ioc_;
  boost::system::error_code open_ec_;
  bool open_{false};
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
};

class ControlledChannel : public interface::Channel {
 public:
  void start() override { emit_state(base::LinkState::Connected); }

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

}  // namespace

class SerialWrapperLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef _WIN32
    device_ = "NUL";
#else
    device_ = "/dev/null";
#endif
  }

  std::string device_;
};

TEST_F(SerialWrapperLifecycleTest, AutoManageStartsAndStopsChannel) {
  auto serial = unilink::serial(device_, 9600).auto_start(true).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  std::atomic<bool> connected{false};
  std::atomic<bool> disconnected{false};

  serial->on_connect([&](const wrapper::ConnectionContext&) { connected = true; });
  serial->on_disconnect([&](const wrapper::ConnectionContext&) { disconnected = true; });

  // In auto-manage mode, start() is called automatically
  TestUtils::waitFor(100);

  serial->stop();
  EXPECT_TRUE(disconnected.load() || !serial->connected());
}

// #440: Serial now defaults to a dedicated io_context + thread instead of
// the shared IoContextManager singleton. Two default-built instances must
// run their callbacks on distinct threads. Uses device_ (/dev/null or NUL),
// which opens successfully but fails baud-rate configuration since it isn't
// a real TTY - that failure fires on_error on the transport's own thread,
// giving a portable way to observe thread identity without a real device.
TEST_F(SerialWrapperLifecycleTest, DefaultUsesDistinctThreadsPerInstance) {
  std::atomic<std::thread::id> thread1{};
  std::atomic<std::thread::id> thread2{};

  auto serial1 = unilink::serial(device_, 9600).reopen_on_error(false).build();
  auto serial2 = unilink::serial(device_, 9600).reopen_on_error(false).build();
  serial1->on_error([&](const wrapper::ErrorContext&) { thread1 = std::this_thread::get_id(); });
  serial2->on_error([&](const wrapper::ErrorContext&) { thread2 = std::this_thread::get_id(); });

  serial1->start();
  serial2->start();

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] { return thread1.load() != std::thread::id{} && thread2.load() != std::thread::id{}; }, 2000));
  EXPECT_NE(thread1.load(), thread2.load());

  serial1->stop();
  serial2->stop();
}

// #440: .shared_context(true) opts back into the shared IoContextManager
// singleton - two such instances should end up driven by the same thread.
TEST_F(SerialWrapperLifecycleTest, SharedContextOptInUsesOneThread) {
  std::atomic<std::thread::id> thread1{};
  std::atomic<std::thread::id> thread2{};

  auto serial1 = unilink::serial(device_, 9600).shared_context(true).reopen_on_error(false).build();
  auto serial2 = unilink::serial(device_, 9600).shared_context(true).reopen_on_error(false).build();
  serial1->on_error([&](const wrapper::ErrorContext&) { thread1 = std::this_thread::get_id(); });
  serial2->on_error([&](const wrapper::ErrorContext&) { thread2 = std::this_thread::get_id(); });

  serial1->start();
  serial2->start();

  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] { return thread1.load() != std::thread::id{} && thread2.load() != std::thread::id{}; }, 2000));
  EXPECT_EQ(thread1.load(), thread2.load());

  serial1->stop();
  serial2->stop();
}

TEST_F(SerialWrapperLifecycleTest, ConfigurationSetters) {
  auto serial = std::make_shared<wrapper::Serial>(device_, 9600);

  serial->baud_rate(115200);
  serial->data_bits(7);
  serial->stop_bits(2);
  serial->parity("even");
  serial->flow_control("hardware");
  serial->retry_interval(500ms);

  // Should be able to start with these settings
  serial->start();
  serial->stop();
}

// Regression test for jwsung91/unilink#444: Impl::stop() used to null the
// channel's callbacks (including on_state, which is what fulfills the
// start() future) without releasing the channel object itself. Since
// start()'s `if (!channel)` guard is the only place that re-runs
// setup_internal_handlers(), a restarted channel's handlers stayed null
// forever, and the second start()'s future never resolved. Uses wait_for()
// with a bounded timeout rather than a bare get(), so this test fails
// loudly instead of hanging the suite if the fix regresses.
TEST_F(SerialWrapperLifecycleTest, RestartAfterStopResolvesFutureAndReinstallsHandlers) {
  auto serial = std::make_shared<wrapper::Serial>(device_, 9600);
  // Fail fast instead of retrying forever: /dev/null (or NUL) doesn't
  // support the serial ioctls, so open() always fails here - the point of
  // this test is that the future resolves at all (worse-finding #1 from
  // #444: it used to hang), not that the device actually connects.
  serial->reopen_on_error(false);
  serial->on_error([](const wrapper::ErrorContext&) {});

  auto first = serial->start();
  ASSERT_EQ(first.wait_for(1s), std::future_status::ready);

  serial->stop();

  auto second = serial->start();
  ASSERT_EQ(second.wait_for(1s), std::future_status::ready);

  serial->stop();
}

TEST_F(SerialWrapperLifecycleTest, AutoManageStartsInjectedTransport) {
  boost::asio::io_context ioc;

  config::SerialConfig cfg;
  cfg.device = "fake";
  cfg.baud_rate = 9600;
  cfg.reopen_on_error = false;

  auto transport_serial = transport::Serial::create(cfg, std::make_unique<FakeSerialPort>(ioc), ioc);
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(transport_serial));

  serial.auto_start(true);
  ioc.run_for(50ms);

  EXPECT_TRUE(serial.connected());

  serial.stop();
  ioc.restart();
  ioc.run_for(50ms);
}

TEST_F(SerialWrapperLifecycleTest, StartFutureReflectsTransportFailure) {
  boost::asio::io_context ioc;

  config::SerialConfig cfg;
  cfg.device = "fake";
  cfg.baud_rate = 9600;
  cfg.reopen_on_error = false;

  auto transport_serial = transport::Serial::create(
      cfg, std::make_unique<FakeSerialPort>(ioc, make_error_code(boost::asio::error::access_denied)), ioc);
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(transport_serial));

  auto started = serial.start();
  ioc.run_for(50ms);

  ASSERT_EQ(started.wait_for(100ms), std::future_status::ready);
  EXPECT_FALSE(started.get());
  EXPECT_FALSE(serial.connected());

  serial.stop();
  ioc.restart();
  ioc.run_for(50ms);
}

TEST_F(SerialWrapperLifecycleTest, InjectedChannelCoversHandlersAndSendVariants) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> connected{0};
  std::atomic<int> disconnected{0};
  std::atomic<int> errors{0};
  std::atomic<int> data{0};
  std::atomic<size_t> queued_bytes{0};
  std::string received;

  serial.on_connect([&](const wrapper::ConnectionContext&) { connected++; });
  serial.on_disconnect([&](const wrapper::ConnectionContext&) { disconnected++; });
  serial.on_error([&](const wrapper::ErrorContext&) { errors++; });
  serial.on_data([&](const wrapper::MessageContext& ctx) {
    data++;
    received = ctx.data_as_string();
  });
  serial.on_backpressure([&](size_t queued) { queued_bytes = queued; });

  auto started = serial.start();
  ASSERT_EQ(started.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(started.get());
  EXPECT_TRUE(serial.connected());
  EXPECT_EQ(connected.load(), 1);

  EXPECT_TRUE(serial.send("abc"));
  EXPECT_TRUE(serial.send_line("line"));
  EXPECT_TRUE(serial.try_send("try"));
  EXPECT_TRUE(serial.try_send_line("tryline"));
  EXPECT_EQ(channel->write_count(), 4);
  EXPECT_EQ(channel->last_write(), "tryline\n");

  channel->emit_bytes("payload");
  EXPECT_EQ(data.load(), 1);
  EXPECT_EQ(received, "payload");

  channel->emit_backpressure(1234);
  EXPECT_EQ(queued_bytes.load(), 1234U);

  channel->emit_state(base::LinkState::Closed);
  EXPECT_EQ(disconnected.load(), 1);

  channel->emit_state(base::LinkState::Error);
  EXPECT_EQ(errors.load(), 1);

  serial.stop();
}

TEST_F(SerialWrapperLifecycleTest, InjectedChannelBatchesRawDataAndFramedMessages) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(channel));

  std::atomic<int> data_batches{0};
  std::atomic<int> message_batches{0};
  std::vector<std::string> raw_payloads;
  std::vector<std::string> framed_payloads;

  serial.batch_size(2).batch_latency(1s);
  serial.on_data_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    data_batches++;
    for (const auto& ctx : batch) raw_payloads.push_back(ctx.data_as_string());
  });

  ASSERT_TRUE(serial.start().get());

  channel->emit_bytes("raw1");
  EXPECT_EQ(data_batches.load(), 0);
  channel->emit_bytes("raw2");
  EXPECT_EQ(data_batches.load(), 1);
  ASSERT_EQ(raw_payloads.size(), 2U);
  EXPECT_EQ(raw_payloads[0], "raw1");
  EXPECT_EQ(raw_payloads[1], "raw2");

  serial.framer(std::make_unique<framer::LineFramer>());
  serial.on_message_batch([&](const std::vector<wrapper::MessageContext>& batch) {
    message_batches++;
    for (const auto& ctx : batch) framed_payloads.push_back(ctx.data_as_string());
  });

  channel->emit_bytes("msg1\nmsg2\n");
  EXPECT_EQ(message_batches.load(), 1);
  ASSERT_EQ(framed_payloads.size(), 2U);
  EXPECT_EQ(framed_payloads[0], "msg1");
  EXPECT_EQ(framed_payloads[1], "msg2");

  serial.stop();
}

TEST_F(SerialWrapperLifecycleTest, StartWhileAlreadyConnectedReturnsReadyFuture) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(channel));

  ASSERT_TRUE(serial.start().get());

  auto second_start = serial.start();
  ASSERT_EQ(second_start.wait_for(100ms), std::future_status::ready);
  EXPECT_TRUE(second_start.get());

  serial.stop();
}

TEST_F(SerialWrapperLifecycleTest, BestEffortSendUsesTryPathAndPropagatesWriteFailure) {
  auto channel = std::make_shared<ControlledChannel>();
  wrapper::Serial serial(std::static_pointer_cast<interface::Channel>(channel));
  serial.backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);

  ASSERT_TRUE(serial.start().get());

  channel->set_write_result(false);
  EXPECT_FALSE(serial.send("drop"));
  EXPECT_FALSE(serial.send_line("drop-line"));
  EXPECT_EQ(channel->write_count(), 2);

  serial.stop();
}
