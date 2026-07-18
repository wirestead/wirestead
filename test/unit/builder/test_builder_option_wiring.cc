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
#include <memory>
#include <string>

#include "wirestead/config/serial_config.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/serial/serial.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

// wirestead::serial()'s builder used to silently drop explicit char_size()/
// stop_bits()/reopen_on_error() calls: it captured them into its own fields
// but build() never forwarded them to the constructed wrapper::Serial at
// all (unlike parity()/flow_control(), which were correctly wired). This
// fixture is a declared friend of wrapper::Serial (see the forward
// declaration and friend statement in wirestead/wrapper/serial/serial.hpp) so
// it can read back the config actually produced via the public
// wirestead::serial() factory, the same path real callers use.
class SerialBuilderConfigTest : public ::testing::Test {
 protected:
  static config::SerialConfig BuildConfig(std::unique_ptr<wrapper::Serial>& out) { return out->build_config(); }
};

TEST_F(SerialBuilderConfigTest, ExplicitOptionsArePropagatedToTheWrapper) {
  auto serial = wirestead::serial("/dev/ttyUSB0", 115200)
                    .char_size(7)
                    .stop_bits(2)
                    .reopen_on_error(false)
                    .retry_interval(500ms)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  ASSERT_NE(serial, nullptr);

  auto cfg = BuildConfig(serial);
  EXPECT_EQ(cfg.char_size, 7u);
  EXPECT_EQ(cfg.stop_bits, 2u);
  EXPECT_FALSE(cfg.reopen_on_error);
  EXPECT_EQ(cfg.retry_interval_ms, 500u);
}

TEST_F(SerialBuilderConfigTest, UnsetOptionsFallBackToConfigDefaultsNotBuilderDefaults) {
  // No .char_size()/.stop_bits()/.reopen_on_error()/.retry_interval() calls:
  // the resulting config must match config::SerialConfig{}'s own defaults,
  // not some separately-hardcoded value inside the builder.
  auto serial = wirestead::serial("/dev/ttyUSB0", 115200).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_NE(serial, nullptr);

  config::SerialConfig expected;
  auto cfg = BuildConfig(serial);
  EXPECT_EQ(cfg.char_size, expected.char_size);
  EXPECT_EQ(cfg.stop_bits, expected.stop_bits);
  EXPECT_EQ(cfg.reopen_on_error, expected.reopen_on_error);
  EXPECT_EQ(cfg.retry_interval_ms, expected.retry_interval_ms);
}
