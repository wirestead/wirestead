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

#include <chrono>
#include <string>

#include "wirestead/config/serial_config.hpp"
#include "wirestead/wrapper/serial/serial.hpp"

using namespace wirestead;

// Thin subclass that re-exposes the protected build_config() method.
class TestableSerial : public wrapper::Serial {
 public:
  using wrapper::Serial::build_config;
  using wrapper::Serial::Serial;
};

class SerialConfigMappingTest : public ::testing::Test {};

TEST_F(SerialConfigMappingTest, MapsParityFlowBitsAndBaud) {
  std::string device = "/dev/ttyUSB0";
  uint32_t baud = 115200;

  auto wrapper = std::make_shared<TestableSerial>(device, baud);
  wrapper->data_bits(8);
  wrapper->stop_bits(1);
  wrapper->parity("even");
  wrapper->flow_control("hardware");
  wrapper->retry_interval(std::chrono::milliseconds(500));

  auto cfg = wrapper->build_config();

  EXPECT_EQ(cfg.device, device);
  EXPECT_EQ(cfg.baud_rate, baud);
  EXPECT_EQ(cfg.char_size, 8u);
  EXPECT_EQ(cfg.stop_bits, 1u);
  EXPECT_EQ(cfg.parity, config::SerialConfig::Parity::Even);
  EXPECT_EQ(cfg.flow, config::SerialConfig::Flow::Hardware);
  EXPECT_EQ(cfg.retry_interval_ms, 500u);
}

TEST_F(SerialConfigMappingTest, InvalidValuesPreservePriorConfiguration) {
  auto wrapper = std::make_shared<TestableSerial>("/dev/ttyUSB0", 9600);
  wrapper->parity("even").flow_control("hardware").data_bits(7).stop_bits(2);
  EXPECT_THROW(wrapper->parity("invalid"), std::invalid_argument);
  EXPECT_THROW(wrapper->flow_control("invalid"), std::invalid_argument);
  EXPECT_THROW(wrapper->data_bits(4), std::invalid_argument);
  EXPECT_THROW(wrapper->stop_bits(3), std::invalid_argument);
  const auto cfg = wrapper->build_config();
  EXPECT_EQ(cfg.parity, config::SerialConfig::Parity::Even);
  EXPECT_EQ(cfg.flow, config::SerialConfig::Flow::Hardware);
  EXPECT_EQ(cfg.char_size, 7u);
  EXPECT_EQ(cfg.stop_bits, 2u);
}

// The RS-485 and modem-line options from #603. Nothing called these setters,
// so the wrapper half of that feature was unverified end to end: the builder
// forwards to them and build_config() is what the transport finally opens the
// port with, which is the same silently-dropped-option shape as #432.
TEST_F(SerialConfigMappingTest, MapsRs485AndModemLineOptions) {
  auto wrapper = std::make_shared<TestableSerial>("/dev/ttyUSB0", 115200);

  wrapper->low_latency(false);
  wrapper->rs485(/*rts_on_send=*/false, /*rx_during_tx=*/true, /*delay_before_ms=*/2, /*delay_after_ms=*/3);
  wrapper->dtr(false);
  wrapper->rts(true);
  wrapper->rx_idle_timeout(std::chrono::milliseconds(750));

  auto cfg = wrapper->build_config();

  EXPECT_FALSE(cfg.low_latency);
  // rs485() enables the mode by being called at all - there is no separate
  // enable flag on the wrapper.
  EXPECT_TRUE(cfg.rs485.enabled);
  EXPECT_FALSE(cfg.rs485.rts_on_send);
  EXPECT_TRUE(cfg.rs485.rx_during_tx);
  EXPECT_EQ(cfg.rs485.delay_rts_before_send_ms, 2u);
  EXPECT_EQ(cfg.rs485.delay_rts_after_send_ms, 3u);
  ASSERT_TRUE(cfg.dtr.has_value());
  EXPECT_FALSE(*cfg.dtr);
  ASSERT_TRUE(cfg.rts.has_value());
  EXPECT_TRUE(*cfg.rts);
  EXPECT_EQ(cfg.rx_idle_timeout_ms, 750u);
}

// The defaults are load-bearing and differ from "unset": low_latency is on
// because the FTDI 16 ms latency timer is larger than every other cost in the
// stack, while dtr/rts stay nullopt because driving DTR at open reboots an
// Arduino - "leave the driver alone" is a different request from "drive low".
TEST_F(SerialConfigMappingTest, UntouchedRs485AndModemLinesKeepConfigDefaults) {
  auto wrapper = std::make_shared<TestableSerial>("/dev/ttyUSB0", 115200);

  auto cfg = wrapper->build_config();

  EXPECT_TRUE(cfg.low_latency);
  EXPECT_FALSE(cfg.rs485.enabled);
  EXPECT_FALSE(cfg.dtr.has_value());
  EXPECT_FALSE(cfg.rts.has_value());
  EXPECT_EQ(cfg.rx_idle_timeout_ms, 0u);
}
