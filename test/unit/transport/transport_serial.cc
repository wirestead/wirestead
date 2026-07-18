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
  serial->stop();
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
  serial->stop();

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
  serial->stop();
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
    serial->stop();
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
  serial->stop();
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
  serial->stop();
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
  serial->stop();
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
  serial->stop();
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
  serial->stop();
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
  serial->stop();
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
  serial->stop();
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

  serial->stop();
  ioc.run_for(10ms);
}
