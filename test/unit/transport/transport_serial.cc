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
#include "wirestead/config/serial_config.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/serial/serial.hpp"

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

TEST(TransportSerialTest, BestEffortDropsOldestWhileWriteIsInFlight) {
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
  EXPECT_EQ(stats.dropped_messages, 1u);
  EXPECT_EQ(stats.dropped_bytes, payload.size());
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

  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0x11);  // exceed high watermark, below limit
  serial->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));

  ioc.run_for(50ms);

  ASSERT_GE(events.size(), 2u);
  EXPECT_GE(events.front(), cfg.backpressure_threshold);
  EXPECT_LE(events.back(), cfg.backpressure_threshold / 2);

  wirestead::test::stop_with_context(serial, ioc);
  ioc.run_for(10ms);
}
