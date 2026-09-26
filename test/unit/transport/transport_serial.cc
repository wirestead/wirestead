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
#include <optional>

#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/config/serial_config.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/serial/serial.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace std::chrono_literals;

namespace {

// Minimal fake serial port to avoid real device access in tests
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

}  // namespace

// Ensure destructor path is safe when never started
TEST(TransportSerialTest, DestructorWithoutStartIsSafe) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  EXPECT_NO_THROW({ auto serial = Serial::create(cfg, std::move(port), ioc); });
}

TEST(TransportSerialTest, CreateProvidesSharedFromThis) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);
  EXPECT_NO_THROW({
    auto self = serial->shared_from_this();
    EXPECT_EQ(self.get(), serial.get());
  });
  wirestead::test::stop_with_context(serial, ioc);
}

// The whole point of cfg_.low_latency is that it reaches the driver, and that a
// driver refusing it is not treated as a failure - an FTDI takes it, a native
// UART does not, and both must end up Connected.
TEST(TransportSerialTest, LowLatencyIsRequestedByDefault) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();

  auto serial = Serial::create(cfg, std::move(port), ioc);
  std::atomic<bool> connected{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected = true;
  });

  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->low_latency_requests(), 1);
  EXPECT_TRUE(connected.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

TEST(TransportSerialTest, LowLatencyDisabledLeavesTheDriverAlone) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.low_latency = false;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();

  auto serial = Serial::create(cfg, std::move(port), ioc);
  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->low_latency_requests(), 0);
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

TEST(TransportSerialTest, UnsupportedLowLatencyStillConnects) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  port_raw->set_low_latency_supported(false);

  auto serial = Serial::create(cfg, std::move(port), ioc);
  std::atomic<bool> connected{false};
  std::atomic<bool> errored{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected = true;
    if (state == base::LinkState::Error) errored = true;
  });

  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->low_latency_requests(), 1);
  EXPECT_TRUE(connected.load());
  EXPECT_FALSE(errored.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// A device that goes quiet without erroring leaves a healthy open port and a
// read that never completes, so nothing else in the transport notices. The
// watchdog has to, and it has to survive its own teardown: closing the port
// aborts the pending read, and that aborted read must not cancel the reopen it
// just triggered.
TEST(TransportSerialTest, RxIdleTimeoutReopensASilentPort) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.rx_idle_timeout_ms = 30;
  cfg.retry_interval_ms = 20;
  cfg.reopen_on_error = true;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  // Reaching Connected more than once is the reopen: the port is only opened
  // by the initial start and by a scheduled retry.
  std::atomic<int> connects{0};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connects.fetch_add(1);
  });

  serial->start();
  ioc.run_for(150ms);

  EXPECT_GE(connects.load(), 2) << "the silent port was closed but never reopened";
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

TEST(TransportSerialTest, RxIdleTimeoutIsOffByDefault) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.retry_interval_ms = 20;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<int> connects{0};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connects.fetch_add(1);
  });

  serial->start();
  ioc.run_for(150ms);

  EXPECT_EQ(connects.load(), 1) << "a silent port was torn down with the watchdog disabled";
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// Arriving data has to push the deadline back, otherwise the watchdog tears
// down a perfectly healthy link on a fixed timer.
TEST(TransportSerialTest, ReceivedDataRearmsTheRxIdleTimeout) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.rx_idle_timeout_ms = 30;
  cfg.retry_interval_ms = 20;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<int> connects{0};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connects.fetch_add(1);
  });

  serial->start();
  ioc.run_for(5ms);  // open, configure, arm the watchdog

  for (int i = 0; i < 10; ++i) {
    port_raw->emit_read(1);
    ioc.run_for(10ms);
  }

  EXPECT_EQ(connects.load(), 1) << "a stream arriving every 10ms tripped a 30ms watchdog";
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// RS-485 is off unless asked for, and when asked for the settings must reach
// the driver verbatim - a wrong RTS polarity produces a link that looks dead
// in one direction only, which is near-impossible to diagnose from the outside.
TEST(TransportSerialTest, Rs485SettingsReachTheDriver) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.rs485.enabled = true;
  cfg.rs485.rts_on_send = false;
  cfg.rs485.rx_during_tx = true;
  cfg.rs485.delay_rts_before_send_ms = 2;
  cfg.rs485.delay_rts_after_send_ms = 3;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->rs485_requests(), 1);
  EXPECT_FALSE(port_raw->rs485_rts_on_send());
  EXPECT_TRUE(port_raw->rs485_rx_during_tx());
  EXPECT_EQ(port_raw->rs485_delay_before(), 2u);
  EXPECT_EQ(port_raw->rs485_delay_after(), 3u);
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

TEST(TransportSerialTest, Rs485IsNotRequestedUnlessEnabled) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();

  auto serial = Serial::create(cfg, std::move(port), ioc);
  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->rs485_requests(), 0);
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// An adapter that switches direction in hardware refuses the ioctl. The port
// must still come up - refusing is the normal answer on such hardware.
TEST(TransportSerialTest, UnsupportedRs485StillConnects) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.rs485.enabled = true;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  port_raw->set_rs485_supported(false);

  auto serial = Serial::create(cfg, std::move(port), ioc);
  std::atomic<bool> connected{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected = true;
  });

  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->rs485_requests(), 1);
  EXPECT_TRUE(connected.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// Leaving a line unset must not touch it: an Arduino reboots when DTR is
// asserted at open, so "no opinion" and "drive it low" are different requests.
TEST(TransportSerialTest, ModemLinesAreOnlyTouchedWhenSet) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();

  auto serial = Serial::create(cfg, std::move(port), ioc);
  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->modem_requests(), 0) << "an unset line was still written";
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

TEST(TransportSerialTest, ModemLinesAreAppliedWhenSet) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.dtr = false;  // the Arduino case: explicitly do not assert

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  serial->start();
  ioc.run_for(20ms);

  EXPECT_EQ(port_raw->modem_requests(), 1);
  ASSERT_TRUE(port_raw->modem_dtr().has_value());
  EXPECT_FALSE(*port_raw->modem_dtr());
  EXPECT_FALSE(port_raw->modem_rts().has_value()) << "RTS was written despite being unset";
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(50ms);
}

// operation_aborted after stop must not trigger reconnect/reopen
TEST(TransportSerialTest, StopPreventsReopenAfterOperationAborted) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.retry_interval_ms = 20;
  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();

  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> stop_called{false};
  std::atomic<int> reconnect_after_stop{0};
  serial->on_state([&](base::LinkState state) {
    if (stop_called.load() && state == base::LinkState::Connecting) {
      reconnect_after_stop.fetch_add(1);
    }
  });

  serial->start();
  ioc.run_for(5ms);  // allow open/configure and first read to post

  stop_called.store(true);
  wirestead::test::stop_with_context(serial, ioc);

  // Simulate read completion with operation_aborted after stop
  port_raw->emit_operation_aborted();

  ioc.run_for(50ms);
  EXPECT_EQ(reconnect_after_stop.load(), 0);
}

TEST(TransportSerialTest, QueueLimitRejectsMessage) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> error_seen{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_seen = true;
  });

  serial->start();
  ioc.poll();
  ASSERT_TRUE(serial->is_connected());

  // 5MB exceeds bp_limit (max(bp_high*4, 4MB) = 4MB)
  std::vector<uint8_t> huge(5 * 1024 * 1024, 0xEF);
  EXPECT_FALSE(serial->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size())));

  ioc.run_for(50ms);

  EXPECT_FALSE(error_seen.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, OpenAndConfigureFailuresMoveToError) {
  using Setter = void (FakeSerialPort::*)(boost::system::error_code);
  std::vector<Setter> setters = {
      &FakeSerialPort::set_open_error,           &FakeSerialPort::set_baud_rate_error,
      &FakeSerialPort::set_character_size_error, &FakeSerialPort::set_stop_bits_error,
      &FakeSerialPort::set_parity_error,         &FakeSerialPort::set_flow_control_error,
  };

  for (auto setter : setters) {
    boost::asio::io_context ioc;
    config::SerialConfig cfg;
    cfg.reopen_on_error = false;
    cfg.parity = config::SerialConfig::Parity::Odd;
    cfg.flow = config::SerialConfig::Flow::Hardware;

    auto port = std::make_unique<FakeSerialPort>(ioc);
    ((*port).*setter)(make_error_code(boost::asio::error::access_denied));
    auto serial = Serial::create(cfg, std::move(port), ioc);

    std::atomic<bool> error_seen{false};
    serial->on_state([&](base::LinkState state) {
      if (state == base::LinkState::Error) error_seen = true;
    });

    serial->start();
    ioc.run_for(30ms);

    EXPECT_TRUE(error_seen.load());
    // #445: last_error_info() should now report detail for this transport too.
    ASSERT_TRUE(serial->last_error_info().has_value());
    EXPECT_EQ(serial->last_error_info()->component, "serial");
    wirestead::test::stop_with_context(serial, ioc);
    ioc.restart();
    ioc.run_for(5ms);
  }
}

TEST(TransportSerialTest, MoveWriteRespectsQueueLimit) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> error_seen{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_seen = true;
  });

  serial->start();
  ioc.poll();
  ASSERT_TRUE(serial->is_connected());

  std::vector<uint8_t> huge(20 * 1024 * 1024, 0xCD);
  // Rejects synchronously when exceeding bp_limit
  EXPECT_FALSE(serial->async_write_move(std::move(huge)));

  ioc.run_for(50ms);

  EXPECT_FALSE(error_seen.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, SharedWriteRespectsQueueLimit) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> error_seen{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_seen = true;
  });

  serial->start();
  ioc.poll();
  ASSERT_TRUE(serial->is_connected());

  auto huge = std::make_shared<const std::vector<uint8_t>>(20 * 1024 * 1024, 0xAB);
  // Rejects synchronously when exceeding bp_limit
  EXPECT_FALSE(serial->async_write_shared(huge));

  ioc.run_for(50ms);

  EXPECT_FALSE(error_seen.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, CallbackExceptionStopsWhenConfigured) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.stop_on_callback_exception = true;
  cfg.retry_interval_ms = 10;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> error_seen{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_seen = true;
  });

  serial->on_bytes([](memory::ConstByteSpan) { throw std::runtime_error("boom"); });

  serial->start();
  ioc.run_for(5ms);  // allow start to set up read handler

  // Simulate a successful read that triggers the throwing callback.
  port_raw->emit_read(4);

  ioc.run_for(20ms);

  EXPECT_TRUE(error_seen.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, CallbackExceptionRetriesWhenAllowed) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.stop_on_callback_exception = false;
  cfg.retry_interval_ms = 10;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<int> error_events{0};
  std::atomic<int> connecting_events{0};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_events.fetch_add(1);
    if (state == base::LinkState::Connecting) connecting_events.fetch_add(1);
  });

  serial->on_bytes([](memory::ConstByteSpan) { throw std::runtime_error("boom"); });

  serial->start();
  ioc.run_for(5ms);

  port_raw->emit_read(4);

  // Allow retry timer to fire at least once.
  ioc.run_for(40ms);

  EXPECT_EQ(error_events.load(), 0);
  EXPECT_GE(connecting_events.load(), 2);  // initial start + retry attempt
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, CallbackUnknownExceptionRetriesWhenAllowed) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.stop_on_callback_exception = false;
  cfg.retry_interval_ms = 10;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<int> connecting_events{0};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_events.fetch_add(1);
  });

  serial->on_bytes([](memory::ConstByteSpan) { throw 7; });

  serial->start();
  ioc.run_for(5ms);
  port_raw->emit_read(4);
  ioc.run_for(40ms);

  EXPECT_GE(connecting_events.load(), 2);
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, WriteErrorMovesToErrorWhenRetryDisabled) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.reopen_on_error = false;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  port->set_write_error(make_error_code(boost::asio::error::broken_pipe));
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::atomic<bool> error_seen{false};
  serial->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) error_seen = true;
  });

  serial->start();
  ioc.run_for(5ms);

  std::vector<uint8_t> payload(32, 0x42);
  EXPECT_TRUE(serial->async_write_move(std::move(payload)));
  ioc.run_for(30ms);

  EXPECT_TRUE(error_seen.load());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, BestEffortPreservesAcceptedWritesUnderPressure) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto* port_raw = port.get();
  port->set_complete_writes(false);
  auto serial = Serial::create(cfg, std::move(port), ioc);
  serial->set_backpressure_strategy(base::constants::BackpressureStrategy::BestEffort);

  std::vector<size_t> bp_events;
  serial->on_backpressure([&](size_t queued) { bp_events.push_back(queued); });

  serial->start();
  ioc.run_for(5ms);

  std::vector<uint8_t> payload(2048, 0x31);
  EXPECT_TRUE(serial->async_write_move(std::vector<uint8_t>(payload)));
  EXPECT_TRUE(serial->async_write_move(std::vector<uint8_t>(payload)));
  EXPECT_TRUE(serial->async_write_shared(std::make_shared<const std::vector<uint8_t>>(payload)));
  ioc.run_for(20ms);

  EXPECT_GE(port_raw->write_count(), 1);
  EXPECT_TRUE(serial->is_backpressure_active());
  auto stats = serial->stats();
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
  serial->reset_stats();
  stats = serial->stats();
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
  EXPECT_GT(stats.queued_bytes, 0u);

  port_raw->complete_pending_write();
  ioc.run_for(30ms);

  EXPECT_FALSE(bp_events.empty());
  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

TEST(TransportSerialTest, BackpressureReliefAfterDrain) {
  boost::asio::io_context ioc;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;

  auto port = std::make_unique<FakeSerialPort>(ioc);
  auto serial = Serial::create(cfg, std::move(port), ioc);

  std::vector<size_t> events;
  serial->on_backpressure([&](size_t queued) { events.push_back(queued); });

  serial->start();
  ioc.poll();
  ASSERT_TRUE(serial->is_connected());

  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0x11);  // exceed high watermark, below limit
  ASSERT_TRUE(serial->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));

  ioc.run_for(50ms);

  ASSERT_GE(events.size(), 2u);
  EXPECT_GE(events.front(), cfg.backpressure_threshold);
  EXPECT_LE(events.back(), cfg.backpressure_threshold / 2);

  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}

namespace {
// Delays old write completions across a reconnect and borrows the real gather
// views, so ASan also checks the lifetime promised to port implementations.
class DelayedSerialPort final : public interface::SerialPortInterface {
 public:
  using Handler = std::function<void(const boost::system::error_code&, size_t)>;
  struct Write {
    std::vector<boost::asio::const_buffer> views;
    Handler handler;
  };
  boost::asio::io_context& io;
  Handler read;
  boost::asio::mutable_buffer read_buffer;
  std::vector<std::pair<boost::asio::mutable_buffer, Handler>> retired_reads;
  bool retain_reads = false;
  unsigned opens = 0;
  std::vector<Write> writes;
  bool retain_writes = true;
  explicit DelayedSerialPort(boost::asio::io_context& context) : io(context) {}
  bool opened = false;
  void open(const std::string&, boost::system::error_code& ec) override {
    ++opens;
    opened = true;
    ec.clear();
  }
  bool is_open() const override { return opened; }
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
  void async_read_some(const boost::asio::mutable_buffer& buffer, Handler handler) override {
    read_buffer = buffer;
    read = std::move(handler);
  }
  void async_write(const boost::asio::const_buffer& buffer, Handler handler) override {
    writes.push_back({{buffer}, std::move(handler)});
  }
  void async_write(const std::vector<boost::asio::const_buffer>& buffers, Handler handler) override {
    writes.push_back({buffers, std::move(handler)});
  }
  void close(boost::system::error_code&) override {
    opened = false;
    if (auto handler = std::move(read)) {
      if (retain_reads)
        retired_reads.emplace_back(read_buffer, std::move(handler));
      else
        handler(boost::asio::error::operation_aborted, 0);
    }
    if (!retain_writes) {
      for (auto& write : writes)
        if (auto handler = std::move(write.handler)) handler(boost::asio::error::operation_aborted, 0);
    }
  }
  std::string contents(size_t index) const {
    std::string result;
    for (auto view : writes.at(index).views) result.append(static_cast<const char*>(view.data()), view.size());
    return result;
  }
  void complete(size_t index) {
    const auto size = boost::asio::buffer_size(writes.at(index).views);
    auto handler = std::move(writes.at(index).handler);
    handler({}, size);
  }
};

class SerialConnectionFenceTest : public ::testing::TestWithParam<int> {};
TEST_P(SerialConnectionFenceTest, DropsQueuedAndPostedWritesAndKeepsOldBuffersAlive) {
  boost::asio::io_context io;
  config::SerialConfig cfg;

  cfg.retry_interval_ms = 1;
  cfg.enable_memory_pool = (GetParam() / 6) % 2 == 0;
  cfg.backpressure_strategy = GetParam() >= 12 ? base::constants::BackpressureStrategy::BestEffort
                                               : base::constants::BackpressureStrategy::Reliable;
  auto socket = std::make_unique<DelayedSerialPort>(io);
  auto* delayed = socket.get();
  auto client = transport::Serial::create(cfg, std::move(socket), io);
  struct Cleanup {
    std::function<void()> action;
    ~Cleanup() { action(); }
  } cleanup{[&] {
    delayed->retain_writes = false;
    wirestead::test::stop_with_context(client, io);
  }};
  auto pump = [&] {
    if (io.stopped()) io.restart();
    io.run_for(std::chrono::milliseconds(20));
  };
  auto write = [&](std::string text) {
    std::vector<uint8_t> payload(text.begin(), text.end());
    switch (GetParam() % 6) {
      case 0:
        return client->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));
      case 1:
        return client->async_write_move(std::move(payload));
      case 2:
        return client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(payload));
      case 3:
        return client->async_try_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));
      case 4:
        return client->async_try_write_move(std::move(payload));
      default:
        return client->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(payload));
    }
  };
  client->start();
  pump();
  ASSERT_TRUE(client->is_connected());
  ASSERT_TRUE(write("active-old"));
  pump();
  ASSERT_EQ(delayed->writes.size(), 1u);
  ASSERT_TRUE(write("queued-old"));
  pump();
  ASSERT_EQ(delayed->writes.size(), 1u);
  // Put loss ahead of an already accepted submission's strand handler.
  boost::asio::post(client->get_executor(), [&] {
    auto handler = std::move(delayed->read);
    handler(boost::asio::error::connection_reset, 0);
  });
  ASSERT_TRUE(write("posted-old"));
  pump();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!client->is_connected() && std::chrono::steady_clock::now() < deadline) pump();
  ASSERT_TRUE(client->is_connected());
  EXPECT_EQ(delayed->contents(0), "active-old");
  EXPECT_EQ(client->stats().dropped_messages, 3u);
  ASSERT_TRUE(write("new"));
  pump();
  ASSERT_EQ(delayed->writes.size(), 2u);
  EXPECT_EQ(delayed->contents(1), "new");
  const auto queued = client->stats().queued_bytes;
  delayed->complete(0);
  pump();
  EXPECT_EQ(client->stats().queued_bytes, queued);
  EXPECT_EQ(client->stats().bytes_sent, 0u);
  delayed->complete(1);
  pump();
  EXPECT_EQ(client->stats().bytes_sent, 3u);
  EXPECT_EQ(client->stats().queued_bytes, 0u);
}
INSTANTIATE_TEST_SUITE_P(FormsPoolAndStrategy, SerialConnectionFenceTest, ::testing::Range(0, 24));
}  // namespace

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> nonblocking_result;
thread_local int nonblocking_observations = 0;
void observe_nonblocking_result(const wirestead::wrapper::SendResult& result) {
  nonblocking_result = result;
  ++nonblocking_observations;
}

struct NonblockingResultPeer {
  boost::asio::io_context io;
  std::shared_ptr<wirestead::transport::Serial> native;
  std::unique_ptr<wirestead::wrapper::Serial> client;
  explicit NonblockingResultPeer(bool best_effort) {
    wirestead::config::SerialConfig cfg;
    cfg.backpressure_threshold = 1024;
    cfg.backpressure_strategy = best_effort ? wirestead::base::constants::BackpressureStrategy::BestEffort
                                            : wirestead::base::constants::BackpressureStrategy::Reliable;
    native = wirestead::transport::Serial::create(cfg, std::make_unique<FakeSerialPort>(io), io);
    client = std::make_unique<wirestead::wrapper::Serial>(native);
    client->backpressure_strategy(cfg.backpressure_strategy);
    wirestead::wrapper::detail::g_serial_send_result_hook.store(observe_nonblocking_result);
  }
  ~NonblockingResultPeer() {
    wirestead::wrapper::detail::g_serial_send_result_hook.store(nullptr);
    wirestead::test::stop_wrapper_with_context(*client, io);
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

class SerialNonblockingResultTest : public ::testing::TestWithParam<int> {};
TEST_P(SerialNonblockingResultTest, OrdersValidationLifecycleAndCapacityReasons) {
  using Rejection = wirestead::wrapper::SendRejection;
  const bool best_effort = GetParam() >= 4 && GetParam() < 8;
  const int form = GetParam() % 4;
  // Explicit try methods must stay WouldBlock even on a BestEffort channel.
  NonblockingResultPeer peer(GetParam() >= 4);
  auto& client = *peer.client;
  std::optional<wirestead::wrapper::SendResult> returned_result;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    auto accepted = wirestead::wrapper::SendResult::reject(wirestead::wrapper::SendRejection::NotReady);
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
      EXPECT_EQ(nonblocking_result->accepted(), accepted.accepted());
    }
    returned_result = accepted;
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(returned_result.has_value());
    ASSERT_FALSE(returned_result->accepted());
    EXPECT_EQ(returned_result->reason(), expected);
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

INSTANTIATE_TEST_SUITE_P(TryAndBestEffortForms, SerialNonblockingResultTest, ::testing::Range(0, 12));

TEST(SerialNonblockingResultContract, ConnectedNativeStillRequiresWrapperStart) {
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

TEST(SerialNonblockingResultContract, NullSharedPayloadIsInvalidBeforeStartAndAfterStop) {
  NonblockingResultPeer peer(true);
  EXPECT_FALSE(peer.client->send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
  peer.client->stop();
  EXPECT_FALSE(peer.client->try_send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
}

class SerialReliableResultTest : public ::testing::TestWithParam<int> {};
TEST_P(SerialReliableResultTest, ValidatesBeforeStateAndPreservesPayload) {
  using Rejection = wirestead::wrapper::SendRejection;
  // The last two cases exercise explicit blocking under BestEffort.
  NonblockingResultPeer peer(GetParam() >= 6);
  const int form = GetParam() >= 6 ? GetParam() - 4 : GetParam();
  auto& client = *peer.client;
  std::optional<wirestead::wrapper::SendResult> returned_result;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    auto accepted = wirestead::wrapper::SendResult::reject(wirestead::wrapper::SendRejection::NotReady);
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
      EXPECT_EQ(nonblocking_result->accepted(), accepted.accepted());
    }
    returned_result = accepted;
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(returned_result.has_value());
    ASSERT_FALSE(returned_result->accepted());
    EXPECT_EQ(returned_result->reason(), expected);
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
INSTANTIATE_TEST_SUITE_P(ReliableAndExplicitBlocking, SerialReliableResultTest, ::testing::Range(0, 8));

TEST(SerialReliableResultContract, NativeCapacityRetriesBeyondFiveAndStopReleasesSender) {
  NonblockingResultPeer peer(false);
  auto started = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit(), 'f')));
  const auto failures = peer.native->stats().failed_sends;
  std::vector<uint8_t> payload{1, 2, 3};
  auto sender = std::async(std::launch::async, [&] { return peer.client->send_move(std::move(payload)); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (peer.native->stats().failed_sends < failures + 12 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  const bool retried = peer.native->stats().failed_sends >= failures + 12;
  auto stopper = std::async(std::launch::async, [&] { peer.client->stop(); });
  // Stop cancels the retained wait before waiting for native executor cleanup.
  const bool released = sender.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(peer.until([&] { return stopper.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready; }));
  stopper.get();
  ASSERT_TRUE(released);
  const auto result = sender.get();
  EXPECT_TRUE(retried);
  EXPECT_FALSE(result.accepted());
  EXPECT_TRUE(result.reason() == wirestead::wrapper::SendRejection::CancelledWhileWaiting ||
              result.reason() == wirestead::wrapper::SendRejection::Stopping);
  EXPECT_EQ(payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(SerialReliableResultContract, CallbackCapacityRefusalPreservesMoveStorage) {
  NonblockingResultPeer peer(false);
  auto started = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  // Keep the executor paused: inflight reservations fill the hard limit but
  // have not yet published high-water pressure. Callback callers must not retry.
  ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit(), 'f')));
  ASSERT_FALSE(peer.native->is_backpressure_active());
  wirestead::wrapper::detail::CallbackGuard callback_scope;
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
    EXPECT_EQ(peer.native->stats().failed_sends, failures + (form + 1));
    EXPECT_EQ(peer.native->stats().messages_accepted, 1u);
  }
}
}  // namespace

namespace {
class SerialNativeResultTest : public ::testing::TestWithParam<int> {};
TEST_P(SerialNativeResultTest, ReportsLifecycleValidationAndCapacityAtAdmission) {
  using Rejection = wirestead::wrapper::SendRejection;
  NonblockingResultPeer peer(GetParam() >= 6);
  wirestead::transport::detail::g_serial_write_result_hook.store(observe_nonblocking_result);
  struct ResetHook {
    ~ResetHook() { wirestead::transport::detail::g_serial_write_result_hook.store(nullptr); }
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
INSTANTIATE_TEST_SUITE_P(FormsAndStrategies, SerialNativeResultTest, ::testing::Range(0, 12));
}  // namespace

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

std::atomic<int> observed_send_result{-1};
void observe_send_result(const wrapper::SendResult& result) {
  observed_send_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}
std::atomic<int> observed_wait_result{-1};
std::atomic<AdmissionPark*> wait_result_park{nullptr};
void observe_wait_result(const wrapper::SendResult& result) {
  if (auto park = wait_result_park.load()) {
    park->entered.notify();
    park->release.hold();
  }
  observed_wait_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}

class SerialCapacityWaitConnectionTest : public ::testing::TestWithParam<int> {};
TEST_P(SerialCapacityWaitConnectionTest, PreservesWaitReleaseOutcome) {
  Context context;
  namespace net = boost::asio;
  config::SerialConfig cfg;
  cfg.backpressure_threshold = 1024;
  cfg.retry_interval_ms = 20;
  auto port = std::make_unique<FakeSerialPort>(*context.io);
  auto* fake = port.get();
  fake->set_complete_writes(false);
  auto transport = transport::Serial::create(cfg, std::move(port), *context.io);
  wrapper::Serial client(transport);
  std::atomic<int> connections{0};
  AdmissionPark park, result_park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    result_park.release.notify();
    client.stop();
    if (writer.valid()) writer.wait();
    wrapper::detail::g_serial_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_serial_capacity_wait_result_hook.store(nullptr);
    wrapper::detail::g_serial_send_result_hook.store(nullptr);
    wait_result_park.store(nullptr);
    transport::detail::g_serial_pinned_write_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  client.on_connect([&](const auto&) { ++connections; });
  auto ready = client.start();
  ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
  ASSERT_TRUE(ready.get());
  if (GetParam() < 12 || GetParam() >= 18) {
    ASSERT_TRUE(transport->async_write_move(std::vector<uint8_t>(512 * 1024, 'x')));
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_backpressure_active(); }, 3000));
  }
  observed_wait_result = -1;
  wrapper::detail::g_serial_capacity_wait_result_hook.store(&observe_wait_result);
  if (GetParam() >= 30 && GetParam() < 36) wait_result_park.store(&result_park);
  admission_park.store(&park);
  if (GetParam() < 12 || GetParam() >= 18) {
    wrapper::detail::g_serial_capacity_wait_hook.store(&park_admission);
  } else {
    transport::detail::g_serial_pinned_write_hook.store(&park_admission);
  }
  auto send = [&] {
    switch (GetParam() % 6) {
      case 0:
        return client.send("old");
      case 1:
        return client.send_line("old");
      case 2:
        return client.send_blocking("old");
      case 3:
        return client.send_line_blocking("old");
      case 4:
        return client.send_move(std::vector<uint8_t>{1, 2, 3});
      default:
        return client.send_shared(std::make_shared<const std::vector<uint8_t>>(3, 42));
    }
  };
  observed_send_result = -1;
  wrapper::detail::g_serial_send_result_hook.store(&observe_send_result);
  if (GetParam() >= 42) {
    wrapper::detail::CallbackGuard guard;
    EXPECT_FALSE(send());
    EXPECT_EQ(observed_send_result, static_cast<int>(wrapper::SendRejection::WouldBlock));
    EXPECT_EQ(observed_wait_result, -1);
    return;
  }
  writer = std::async(std::launch::async, send);
  ASSERT_TRUE(park.entered.wait());
  if (GetParam() < 18) {
    boost::asio::post(transport->get_executor(), [&] { fake->emit_read(0, boost::asio::error::connection_reset); });
    if (GetParam() < 12) {
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return connections >= 2; }, 3000));
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_connected(); }, 3000));
    } else {
      // Serial publishes Connecting before scheduling retry. The parked sender holds
      // the wrapper read lock, so let it reject before completing that callback.
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_connected(); }, 3000));
    }
    if (GetParam() >= 6 && GetParam() < 12) {
      ASSERT_TRUE(transport->async_write_move(std::vector<uint8_t>(512 * 1024, 'y')));
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_backpressure_active(); }, 3000));
    }
  } else if (GetParam() < 24) {
    // Prevent a replacement connection while proving loss-before-stop ordering.
    boost::asio::post(transport->get_executor(), [&] {
      fake->set_open_error(boost::asio::error::access_denied);
      fake->emit_read(0, boost::asio::error::connection_reset);
    });
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_connected(); }, 3000));
    client.stop();
  } else if (GetParam() < 30) {
    if (GetParam() % 2 == 0)
      transport->stop();
    else
      client.stop();
    boost::asio::post(transport->get_executor(), [&] { fake->emit_read(0, boost::asio::error::connection_reset); });
  } else {
    boost::asio::post(transport->get_executor(), [&] { fake->complete_pending_write(); });
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_backpressure_active(); }, 3000));
  }
  const auto accepted_before = transport->stats().messages_accepted;
  park.release.notify();
  if (GetParam() >= 30 && GetParam() < 36) {
    ASSERT_TRUE(result_park.entered.wait());
    client.stop();
    result_park.release.notify();
  }
  const auto status = writer.wait_for(300ms);
  EXPECT_EQ(status, std::future_status::ready);
  if (status != std::future_status::ready) client.stop();
  const bool capacity_accepted = GetParam() >= 36;
  const auto send_result = writer.get();
  EXPECT_EQ(send_result.accepted(), capacity_accepted);
  EXPECT_EQ(transport->stats().messages_accepted, accepted_before + (capacity_accepted ? 1 : 0));
  const auto expected_send = capacity_accepted  ? 100
                             : GetParam() >= 30 ? static_cast<int>(wrapper::SendRejection::NotStarted)
                             : GetParam() >= 24 ? static_cast<int>(wrapper::SendRejection::CancelledWhileWaiting)
                                                : static_cast<int>(wrapper::SendRejection::NotReady);
  EXPECT_EQ(observed_send_result, expected_send);
  EXPECT_EQ(send_result.accepted() ? 100 : static_cast<int>(send_result.reason()), expected_send);
  if (GetParam() < 12 || (GetParam() >= 18 && GetParam() < 24)) {
    EXPECT_EQ(observed_wait_result, static_cast<int>(wrapper::SendRejection::NotReady));
  } else if (GetParam() >= 24 && GetParam() < 30) {
    EXPECT_EQ(observed_wait_result, static_cast<int>(wrapper::SendRejection::CancelledWhileWaiting));
  } else if (GetParam() >= 30) {
    EXPECT_EQ(observed_wait_result, 100);
  }
  if (GetParam() < 6 || (GetParam() >= 12 && GetParam() < 18)) {
    if (GetParam() >= 12) {
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return connections >= 2; }, 3000));
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_connected(); }, 3000));
    }
    EXPECT_TRUE(client.send("new"));
    EXPECT_EQ(transport->stats().messages_accepted, accepted_before + 1);
  }
}
INSTANTIATE_TEST_SUITE_P(ReconnectAndReleaseReasons, SerialCapacityWaitConnectionTest, ::testing::Range(0, 48));
}  // namespace

namespace {
TEST(SerialReadFenceTest, IdleReopenKeepsOldReadBufferAndIgnoresItsCompletion) {
  boost::asio::io_context io;
  config::SerialConfig cfg;
  cfg.rx_idle_timeout_ms = 30;
  cfg.retry_interval_ms = 1;
  auto port = std::make_unique<DelayedSerialPort>(io);
  auto* delayed = port.get();
  delayed->retain_reads = true;
  auto serial = transport::Serial::create(cfg, std::move(port), io);
  OnExit cleanup{[&] {
    delayed->retain_reads = false;
    delayed->retain_writes = false;
    delayed->retired_reads.clear();
    test::stop_with_context(serial, io);
  }};
  int received = 0;
  serial->on_bytes([&](memory::ConstByteSpan) { ++received; });
  serial->start();
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (delayed->opens < 2 && std::chrono::steady_clock::now() < deadline) {
    if (io.stopped()) io.restart();
    io.run_one_for(5ms);
  }
  ASSERT_EQ(delayed->opens, 2u);
  ASSERT_EQ(delayed->retired_reads.size(), 1u);
  auto old = std::move(delayed->retired_reads.front());
  delayed->retired_reads.clear();
  ASSERT_NE(old.first.data(), delayed->read_buffer.data());
  static_cast<uint8_t*>(old.first.data())[0] = 42;
  old.second({}, 1);
  old.second = {};
  io.poll();
  EXPECT_EQ(received, 0);
  EXPECT_EQ(serial->stats().bytes_received, 0u);
  EXPECT_TRUE(serial->is_connected());
}
}  // namespace
