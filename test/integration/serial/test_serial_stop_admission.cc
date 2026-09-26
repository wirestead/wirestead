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
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "test_connection_channel.hpp"
#include "test_utils.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace {

class Signal {
 public:
  void notify() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_ = true;
    cv_.notify_all();
  }
  bool wait(std::chrono::milliseconds limit = 5s) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, limit, [this] { return set_; });
  }
  void hold() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return set_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool set_ = false;
};

struct OnExit {
  std::function<void()> action;
  ~OnExit() { action(); }
};

struct Context {
  std::shared_ptr<boost::asio::io_context> io = std::make_shared<boost::asio::io_context>();
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work{io->get_executor()};
  std::thread runner{[this] { io->run(); }};
  ~Context() {
    work.reset();
    io->stop();
    runner.join();
  }
};

// This channel preserves snapshots without borrowing an actual serial strand.
// A parked wrapper admission must not also prevent transport cleanup.
class SavedChannel : public wirestead::test::TestConnectionChannel {
 public:
  void start() override {
    connected_ = true;
    connection_opened();
  }
  void stop() override {
    connected_ = false;
    connection_lost();
  }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override { return io_.get_executor(); }
  SendResult async_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { bp = std::move(cb); }
  OnBytes bytes;
  OnState state;
  OnBackpressure bp;

 private:
  boost::asio::io_context io_;
  std::atomic<bool> connected_{false};
};

struct AdmissionPark {
  Signal entered, release;
  std::atomic<bool> armed{true};
};
std::atomic<AdmissionPark*> admission_park{nullptr};
void park_admission() {
  auto* park = admission_park.load();
  if (park && park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

class SerialStopAdmissionTest : public ::testing::Test {};

class SerialSavedAdmissionTest : public ::testing::TestWithParam<int> {};

TEST_P(SerialSavedAdmissionTest, ParkedOldHandlerIsRefusedAfterStopAndRestart) {
  auto channel = std::make_shared<SavedChannel>();
  auto client = std::make_shared<wrapper::Serial>(channel);
  std::atomic<int> delivered{0};
  client->on_data([&](const auto&) { ++delivered; });
  client->on_error([&](const auto&) { ++delivered; });
  client->on_backpressure([&](size_t) { ++delivered; });
  auto start = [&] {
    auto ready = client->start();
    channel->state(base::LinkState::Connected);
    EXPECT_EQ(ready.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(ready.get());
  };
  auto stop = [&] { client->stop(); };
  auto snapshot = [&]() -> std::function<void()> {
    if (GetParam() == 1) {
      auto state = channel->state;
      return [state] { state(base::LinkState::Error); };
    }
    if (GetParam() == 2) {
      auto bp = channel->bp;
      return [bp] { bp(100); };
    }
    auto bytes = channel->bytes;
    return [bytes] {
      const uint8_t data[] = {42};
      bytes(memory::ConstByteSpan(data, 1));
    };
  };
  AdmissionPark park;
  std::future<void> pending;
  OnExit cleanup{[&] {
    park.release.notify();
    if (pending.valid()) pending.wait();
    wrapper::detail::g_pre_admission_hook.store(nullptr);
    admission_park.store(nullptr);
    stop();
  }};
  start();
  auto old = snapshot();
  admission_park.store(&park);
  wrapper::detail::g_pre_admission_hook.store(&park_admission);
  pending = std::async(std::launch::async, old);
  ASSERT_TRUE(park.entered.wait());
  stop();
  // Stop completed while the old handler retained a liveness reference.
  start();
  auto fresh = snapshot();
  park.release.notify();
  pending.get();
  EXPECT_EQ(delivered.load(), 0);
  old();
  EXPECT_EQ(delivered.load(), 0);
  fresh();
  EXPECT_EQ(delivered.load(), 1);
}

struct CleanupPark {
  Signal entered, release;
  std::atomic<int> waiters{0};
  std::atomic<bool> armed{true};
};
std::atomic<CleanupPark*> cleanup_park{nullptr};
void park_cleanup(const void*, bool completing) {
  auto* park = cleanup_park.load();
  if (!park) return;
  if (!completing) {
    ++park->waiters;
  } else if (park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

class FakeSerialPort : public interface::SerialPortInterface {
 public:
  explicit FakeSerialPort(boost::asio::io_context& ioc) : ioc_(ioc) {}

  void open(const std::string&, boost::system::error_code& ec) override {
    ec = open_ec_;
    open_ = !ec;
  }
  bool is_open() const override { return open_; }
  void close(boost::system::error_code& ec) override {
    open_ = false;
    ec.clear();
    complete_pending_write(make_error_code(boost::asio::error::operation_aborted));
    emit_operation_aborted();
  }

  void set_option(const boost::asio::serial_port_base::baud_rate&, boost::system::error_code& ec) override {
    ec = baud_rate_ec_;
  }
  void set_option(const boost::asio::serial_port_base::character_size&, boost::system::error_code& ec) override {
    ec = character_size_ec_;
  }
  void set_option(const boost::asio::serial_port_base::stop_bits&, boost::system::error_code& ec) override {
    ec = stop_bits_ec_;
  }
  void set_option(const boost::asio::serial_port_base::parity&, boost::system::error_code& ec) override {
    ec = parity_ec_;
  }
  void set_option(const boost::asio::serial_port_base::flow_control&, boost::system::error_code& ec) override {
    ec = flow_control_ec_;
  }

  bool set_low_latency() override {
    ++low_latency_requests_;
    return low_latency_supported_;
  }

  bool set_rs485(bool rts_on_send, bool rx_during_tx, unsigned delay_before_ms, unsigned delay_after_ms) override {
    ++rs485_requests_;
    rs485_rts_on_send_ = rts_on_send;
    rs485_rx_during_tx_ = rx_during_tx;
    rs485_delay_before_ = delay_before_ms;
    rs485_delay_after_ = delay_after_ms;
    return rs485_supported_;
  }

  bool set_modem_lines(std::optional<bool> dtr, std::optional<bool> rts) override {
    ++modem_requests_;
    modem_dtr_ = dtr;
    modem_rts_ = rts;
    return true;
  }

  void async_read_some(const boost::asio::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const boost::asio::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    ++write_count_;
    auto size = buffer.size();
    if (!complete_writes_) {
      pending_write_handler_ = std::move(handler);
      pending_write_size_ = size;
      return;
    }
    boost::asio::post(ioc_, [handler = std::move(handler), ec = write_ec_, size]() { handler(ec, size); });
  }

  void set_open_error(boost::system::error_code ec) { open_ec_ = ec; }
  void set_baud_rate_error(boost::system::error_code ec) { baud_rate_ec_ = ec; }
  void set_character_size_error(boost::system::error_code ec) { character_size_ec_ = ec; }
  void set_stop_bits_error(boost::system::error_code ec) { stop_bits_ec_ = ec; }
  void set_parity_error(boost::system::error_code ec) { parity_ec_ = ec; }
  void set_flow_control_error(boost::system::error_code ec) { flow_control_ec_ = ec; }
  void set_write_error(boost::system::error_code ec) { write_ec_ = ec; }
  void set_complete_writes(bool complete) { complete_writes_ = complete; }
  int write_count() const { return write_count_; }
  void set_low_latency_supported(bool supported) { low_latency_supported_ = supported; }
  int low_latency_requests() const { return low_latency_requests_; }
  void set_rs485_supported(bool supported) { rs485_supported_ = supported; }
  int rs485_requests() const { return rs485_requests_; }
  bool rs485_rts_on_send() const { return rs485_rts_on_send_; }
  bool rs485_rx_during_tx() const { return rs485_rx_during_tx_; }
  unsigned rs485_delay_before() const { return rs485_delay_before_; }
  unsigned rs485_delay_after() const { return rs485_delay_after_; }
  int modem_requests() const { return modem_requests_; }
  std::optional<bool> modem_dtr() const { return modem_dtr_; }
  std::optional<bool> modem_rts() const { return modem_rts_; }

  void complete_pending_write(const boost::system::error_code& ec = {}) {
    if (!pending_write_handler_) return;
    auto handler = std::move(pending_write_handler_);
    auto size = pending_write_size_.value_or(0);
    pending_write_size_.reset();
    boost::asio::post(ioc_, [handler = std::move(handler), ec, size]() { handler(ec, size); });
  }

  void emit_read(std::size_t n = 1, const boost::system::error_code& ec = {}) {
    if (!read_handler_) return;
    auto handler = std::move(read_handler_);
    boost::asio::post(ioc_, [handler = std::move(handler), ec, n]() { handler(ec, n); });
  }

  void emit_operation_aborted() {
    if (!read_handler_) return;
    auto handler = std::move(read_handler_);
    boost::asio::post(ioc_, [handler]() { handler(make_error_code(boost::asio::error::operation_aborted), 0); });
  }

 private:
  boost::asio::io_context& ioc_;
  boost::system::error_code open_ec_;
  boost::system::error_code baud_rate_ec_;
  boost::system::error_code character_size_ec_;
  boost::system::error_code stop_bits_ec_;
  boost::system::error_code parity_ec_;
  boost::system::error_code flow_control_ec_;
  boost::system::error_code write_ec_;
  bool open_{false};
  bool complete_writes_{true};
  int write_count_{0};
  bool low_latency_supported_{true};
  int low_latency_requests_{0};
  bool rs485_supported_{true};
  int rs485_requests_{0};
  bool rs485_rts_on_send_{false};
  bool rs485_rx_during_tx_{false};
  unsigned rs485_delay_before_{0};
  unsigned rs485_delay_after_{0};
  int modem_requests_{0};
  std::optional<bool> modem_dtr_;
  std::optional<bool> modem_rts_;
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
  std::function<void(const boost::system::error_code&, std::size_t)> pending_write_handler_;
  std::optional<std::size_t> pending_write_size_;
};

std::shared_ptr<transport::Serial> make_transport(boost::asio::io_context& io) {
  config::SerialConfig cfg;
  cfg.device = "/dev/ttyTEST";
  cfg.reopen_on_error = false;
  return transport::Serial::create(cfg, std::make_unique<FakeSerialPort>(io), io);
}

TEST_F(SerialStopAdmissionTest, BothOutsideStopsWaitUntilCleanupActuallyCompletes) {
  Context context;
  auto channel = make_transport(*context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  Signal started;
  boost::asio::post(channel->get_executor(), [&] { started.notify(); });
  ASSERT_TRUE(started.wait());
  cleanup_park.store(&park);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return park.waiters.load() >= 2; }, 1000));
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
  channel->stop();
  // A completed stop also permits the SAME transport instance to restart.
  channel->start();
  Signal restarted;
  boost::asio::post(channel->get_executor(), [&] { restarted.notify(); });
  ASSERT_TRUE(restarted.wait());
  channel->stop();
}

TEST_F(SerialStopAdmissionTest, StopDoesNotRunUnrelatedReadyHandlers) {
  Context context;
  auto channel = make_transport(*context.io);
  Signal occupied, release, marker;
  CleanupPark observed;
  observed.armed = false;
  std::atomic<std::thread::id> ran_on{};
  std::future<void> stopper;
  OnExit cleanup{[&] {
    release.notify();
    if (stopper.valid()) stopper.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  boost::asio::post(channel->get_executor(), [&] {
    occupied.notify();
    release.hold();
  });
  ASSERT_TRUE(occupied.wait());
  boost::asio::post(*context.io, [&] {
    ran_on = std::this_thread::get_id();
    marker.notify();
  });
  cleanup_park.store(&observed);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  stopper = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return observed.waiters.load() == 1; }, 1000));
  EXPECT_FALSE(marker.wait(100ms));
  EXPECT_EQ(stopper.wait_for(100ms), std::future_status::timeout);
  release.notify();
  stopper.get();
  ASSERT_TRUE(marker.wait());
  EXPECT_EQ(ran_on.load(), context.runner.get_id());
}

TEST_F(SerialStopAdmissionTest, NeverStartedStopNeedsNoExecutorAndRetainsNoWork) {
  boost::asio::io_context io;
  auto channel = make_transport(io);
  std::weak_ptr<interface::Channel> weak = channel;
  channel->stop();
  channel->stop();
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}

// Unlike a real port, this port deliberately retains its gather-write handler
// after close. The caller releases it only after observing cleanup in progress.
class HeldWritePort : public FakeSerialPort {
 public:
  explicit HeldWritePort(boost::asio::io_context& io) : FakeSerialPort(io) {}
  void close(boost::system::error_code& ec) override {
    FakeSerialPort::close(ec);
    closed.notify();
  }
  void async_write(const std::vector<boost::asio::const_buffer>& buffers,
                   std::function<void(const boost::system::error_code&, std::size_t)> cb) override {
    views = buffers;
    completion = std::move(cb);
    writing.notify();
  }
  Signal writing, closed;
  std::vector<boost::asio::const_buffer> views;
  std::function<void(const boost::system::error_code&, std::size_t)> completion;
};
TEST(SerialWriteStopTest, GatherStorageRemainsValidUntilCancelledCompletionIsReleased) {
  Context context;
  config::SerialConfig cfg;
  cfg.device = "/dev/ttyTEST";
  auto port = std::make_unique<HeldWritePort>(*context.io);
  auto* raw = port.get();
  auto channel = transport::Serial::create(cfg, std::move(port), *context.io);
  Signal connected, released;
  std::future<void> first, second;
  bool submitted = false;
  OnExit cleanup{[&] {
    if (submitted) {
      Signal cleanup_done;
      boost::asio::post(channel->get_executor(), [&] {
        if (raw->completion) {
          auto cb = std::move(raw->completion);
          cb(boost::asio::error::operation_aborted, 0);
        }
        cleanup_done.notify();
      });
      cleanup_done.wait();
    }
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    channel->stop();
  }};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected.notify();
  });
  channel->start();
  ASSERT_TRUE(connected.wait());
  const std::string payload(1024, 'x');
  ASSERT_TRUE(channel->async_write_copy(
      memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(payload.data()), payload.size())));
  submitted = true;
  ASSERT_TRUE(raw->writing.wait());
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(raw->closed.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  boost::asio::post(channel->get_executor(), [&] {
    ASSERT_EQ(raw->views.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char*>(raw->views[0].data()), raw->views[0].size()), payload);
    auto cb = std::move(raw->completion);
    cb(boost::asio::error::operation_aborted, 0);
    released.notify();
  });
  ASSERT_TRUE(released.wait());
  first.get();
  second.get();
}
TEST(SerialWriteStopTest, DiscardedCompletionAlsoReleasesShutdownWaiters) {
  Context context;
  config::SerialConfig cfg;
  cfg.device = "/dev/ttyTEST";
  auto port = std::make_unique<HeldWritePort>(*context.io);
  auto* raw = port.get();
  auto channel = transport::Serial::create(cfg, std::move(port), *context.io);
  Signal connected, released;
  std::future<void> outside;
  OnExit cleanup{[&] {
    Signal cleanup_done;
    boost::asio::post(channel->get_executor(), [&] {
      raw->completion = nullptr;
      cleanup_done.notify();
    });
    cleanup_done.wait();
    if (outside.valid()) outside.wait();
    channel->stop();
  }};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected.notify();
  });
  channel->start();
  ASSERT_TRUE(connected.wait());
  ASSERT_TRUE(channel->async_write_move(std::vector<uint8_t>(1024, 42)));
  ASSERT_TRUE(raw->writing.wait());
  outside = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(raw->closed.wait());
  EXPECT_EQ(outside.wait_for(100ms), std::future_status::timeout);
  boost::asio::post(channel->get_executor(), [&] {
    raw->completion = nullptr;
    released.notify();
  });
  ASSERT_TRUE(released.wait());
  outside.get();
}
TEST(SerialRetryStopTest, FailedOpenRetryIsCancelledBeforeRestart) {
  Context context;
  config::SerialConfig cfg;
  cfg.device = "/dev/ttyTEST";
  cfg.retry_interval_ms = 100;
  auto port = std::make_unique<FakeSerialPort>(*context.io);
  auto* raw = port.get();
  raw->set_open_error(make_error_code(boost::asio::error::access_denied));
  auto channel = transport::Serial::create(cfg, std::move(port), *context.io);
  Signal failed;
  OnExit cleanup{[&] { channel->stop(); }};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) failed.notify();
  });
  channel->start();
  ASSERT_TRUE(failed.wait());
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return channel->last_error_info().has_value(); }, 1000));
  Signal retry_armed;
  boost::asio::post(channel->get_executor(), [&] { retry_armed.notify(); });
  ASSERT_TRUE(retry_armed.wait());
  channel->stop();
  raw->set_open_error({});
  Signal connected;
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected.notify();
  });
  channel->start();
  ASSERT_TRUE(connected.wait());
  channel->stop();
}
TEST(SerialWriteStopTest, NeverStartedWritesRetainNoExecutorWork) {
  boost::asio::io_context io;
  auto channel = make_transport(io);
  const uint8_t byte = 1;
  EXPECT_FALSE(channel->async_write_copy(memory::ConstByteSpan(&byte, 1)));
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>{byte}));
  channel->stop();
  std::weak_ptr<transport::Serial> weak = channel;
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}

TEST(SerialCancelledIoCompletionTest, OutsideStopsWaitForTheLastCancelledHandler) {
  Context context;
  auto channel = make_transport(*context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_serial_io_completion_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  Signal listening;
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) listening.notify();
  });
  channel->start();
  ASSERT_TRUE(listening.wait());
  cleanup_park.store(&park);
  transport::detail::g_serial_io_completion_hook.store([] { park_cleanup(nullptr, true); });
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
}

INSTANTIATE_TEST_SUITE_P(DataStateBackpressure, SerialSavedAdmissionTest, ::testing::Values(0, 1, 2));

}  // namespace
