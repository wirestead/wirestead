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
#include <vector>

#include "wirestead/builder/udp_builder.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/framer/line_framer.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace wirestead {
namespace test {

TEST(UdpBuilderOptionsTest, UdpClientBuilderSetters) {
  auto udp = builder::UdpClientBuilder(0)
                 .auto_start(false)  // Disable auto-start for stability in builder tests
                 .local_port(0)
                 .remote("127.0.0.1", 5678)
                 .independent_context(true)
                 .broadcast(true)
                 .reuse_address(true)
                 .send_buffer_size(4 * 1024)
                 .receive_buffer_size(8 * 1024)
                 .on_data([](const wrapper::MessageContext&) {})
                 .on_connect([](const wrapper::ConnectionContext&) {})
                 .on_disconnect([](const wrapper::ConnectionContext&) {})
                 .on_error([](const wrapper::ErrorContext&) {})
                 .framer([]() { return std::make_unique<framer::LineFramer>(); })
                 .on_message([](const wrapper::MessageContext&) {})
                 .on_data([](auto&&) {})
                 .on_error([](auto&&) {})
                 .build();

  ASSERT_NE(udp, nullptr);
}

// #445: remote-address format is a pure-input error, detectable synchronously
// with no I/O and always the caller's fault - it should throw as soon as
// remote_endpoint()/remote() is called, not be deferred until build() time
// constructs the underlying transport.
TEST(UdpBuilderOptionsTest, InvalidRemoteAddressThrowsImmediately) {
  builder::UdpClientBuilder builder(0);
  EXPECT_THROW(builder.remote_endpoint("not-an-address", 1234), diagnostics::BuilderException);
}

TEST(UdpBuilderOptionsTest, UdpServerBuilderSetters) {
  auto server = builder::UdpServerBuilder(0)
                    .auto_start(false)
                    .local_port(0)
                    .independent_context(true)
                    .broadcast(true)
                    .reuse_address(true)
                    .send_buffer_size(4 * 1024)
                    .receive_buffer_size(8 * 1024)
                    .on_data([](const wrapper::MessageContext&) {})
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_disconnect([](const wrapper::ConnectionContext&) {})
                    .on_error([](const wrapper::ErrorContext&) {})
                    .framer([]() { return std::make_unique<framer::LineFramer>(); })
                    .on_message([](const wrapper::MessageContext&) {})
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(server, nullptr);
}

TEST(UdpBuilderOptionsTest, UdpClientBuilderSettersAfterDataHandler) {
  auto builder = builder::UdpClientBuilder<>(0).on_data_batch([](const std::vector<wrapper::MessageContext>&) {});

  auto udp = std::move(builder)
                 .auto_start(false)
                 .local_port(0)
                 .bind_address("127.0.0.1")
                 .remote_endpoint("127.0.0.1", 5678)
                 .broadcast(false)
                 .reuse_address(false)
                 .independent_context(false)
                 .backpressure_strategy(base::constants::BackpressureStrategy::BestEffort)
                 .backpressure_threshold(2048)
                 .on_backpressure([](size_t) {})
                 .use_packet_framer(std::vector<uint8_t>{0x02}, std::vector<uint8_t>{0x03}, 128)
                 .on_connect([](const wrapper::ConnectionContext&) {})
                 .on_disconnect([](const wrapper::ConnectionContext&) {})
                 .on_message([](const wrapper::MessageContext&) {})
                 .on_message_batch([](const std::vector<wrapper::MessageContext>&) {})
                 .on_error([](const wrapper::ErrorContext&) {})
                 .build();

  ASSERT_NE(udp, nullptr);
}

TEST(UdpBuilderOptionsTest, UdpClientBuilderSettersAfterErrorHandler) {
  auto builder = builder::UdpClientBuilder<>(0).on_error([](const wrapper::ErrorContext&) {});

  auto udp = std::move(builder)
                 .auto_start(false)
                 .local_port(0)
                 .bind_address("0.0.0.0")
                 .remote("127.0.0.1", 5678)
                 .broadcast(true)
                 .reuse_address(true)
                 .independent_context(true)
                 .backpressure_strategy(base::constants::BackpressureStrategy::Reliable)
                 .backpressure_threshold(4096)
                 .on_backpressure([](size_t) {})
                 .use_line_framer("\n", false, 128)
                 .on_connect([](const wrapper::ConnectionContext&) {})
                 .on_disconnect([](const wrapper::ConnectionContext&) {})
                 .on_data([](const wrapper::MessageContext&) {})
                 .build();

  ASSERT_NE(udp, nullptr);
}

TEST(UdpBuilderOptionsTest, UdpServerBuilderSettersAfterDataHandler) {
  auto builder = builder::UdpServerBuilder<>(0).on_data_batch([](const std::vector<wrapper::MessageContext>&) {});

  auto server = std::move(builder)
                    .auto_start(false)
                    .local_port(0)
                    .bind_address("127.0.0.1")
                    .max_clients(2)
                    .idle_timeout(25ms)
                    .broadcast(false)
                    .reuse_address(false)
                    .independent_context(false)
                    .backpressure_strategy(base::constants::BackpressureStrategy::BestEffort)
                    .backpressure_threshold(2048)
                    .on_backpressure([](size_t) {})
                    .use_packet_framer(std::vector<uint8_t>{0x02}, std::vector<uint8_t>{0x03}, 128)
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_disconnect([](const wrapper::ConnectionContext&) {})
                    .on_message([](const wrapper::MessageContext&) {})
                    .on_message_batch([](const std::vector<wrapper::MessageContext>&) {})
                    .on_error([](const wrapper::ErrorContext&) {})
                    .build();

  ASSERT_NE(server, nullptr);
}

TEST(UdpBuilderOptionsTest, UdpServerBuilderSettersAfterErrorHandler) {
  auto builder = builder::UdpServerBuilder<>(0).on_error([](const wrapper::ErrorContext&) {});

  auto server = std::move(builder)
                    .auto_start(false)
                    .local_port(0)
                    .bind_address("0.0.0.0")
                    .max_clients(3)
                    .idle_timeout(50ms)
                    .broadcast(true)
                    .reuse_address(true)
                    .independent_context(true)
                    .backpressure_strategy(base::constants::BackpressureStrategy::Reliable)
                    .backpressure_threshold(4096)
                    .on_backpressure([](size_t) {})
                    .use_line_framer("\n", false, 128)
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_disconnect([](const wrapper::ConnectionContext&) {})
                    .on_data([](const wrapper::MessageContext&) {})
                    .build();

  ASSERT_NE(server, nullptr);
}

TEST(UdpBuilderOptionsTest, MissingCallbacksAreAllowed) {
  auto client = builder::UdpClientBuilder<>(0).auto_start(false).build();
  auto server = builder::UdpServerBuilder<>(0).auto_start(false).build();

  EXPECT_NE(client, nullptr);
  EXPECT_NE(server, nullptr);
  EXPECT_FALSE(client->connected());
  EXPECT_FALSE(server->listening());
}

}  // namespace test
}  // namespace wirestead
