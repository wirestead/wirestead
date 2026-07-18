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

#include "wirestead/builder/uds_builder.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/framer/line_framer.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace wirestead {
namespace test {

TEST(UdsBuilderOptionsTest, UdsClientBuilderSetters) {
  auto client = builder::UdsClientBuilder("/tmp/test_uds_client_builder.sock")
                    .auto_start(false)
                    .independent_context(true)
                    .retry_interval(100ms)
                    .max_retries(5)
                    .connection_timeout(1000ms)
                    .on_data([](const wrapper::MessageContext&) {})
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_disconnect([](const wrapper::ConnectionContext&) {})
                    .on_error([](const wrapper::ErrorContext&) {})
                    .framer([]() { return std::make_unique<framer::LineFramer>(); })
                    .on_message([](const wrapper::MessageContext&) {})
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_NE(client, nullptr);
}

TEST(UdsBuilderOptionsTest, UdsServerBuilderSetters) {
  auto server = builder::UdsServerBuilder("/tmp/test_uds_server_builder.sock")
                    .auto_start(false)
                    .independent_context(true)
                    .idle_timeout(5000ms)
                    .max_clients(50)
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

TEST(UdsBuilderOptionsTest, UdsClientBuilderSettersAfterDataHandler) {
  auto builder = builder::UdsClientBuilder("/tmp/test_uds_client_builder_data.sock")
                     .on_data_batch([](const std::vector<wrapper::MessageContext>&) {});

  auto client = std::move(builder)
                    .auto_start(false)
                    .independent_context(false)
                    .retry_interval(200ms)
                    .max_retries(2)
                    .connection_timeout(1500ms)
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

  ASSERT_NE(client, nullptr);
}

TEST(UdsBuilderOptionsTest, UdsClientBuilderSettersAfterErrorHandler) {
  auto builder =
      builder::UdsClientBuilder("/tmp/test_uds_client_builder_error.sock").on_error([](const wrapper::ErrorContext&) {
      });

  auto client = std::move(builder)
                    .auto_start(false)
                    .independent_context(true)
                    .retry_interval(250ms)
                    .max_retries(3)
                    .connection_timeout(2000ms)
                    .backpressure_strategy(base::constants::BackpressureStrategy::Reliable)
                    .backpressure_threshold(4096)
                    .on_backpressure([](size_t) {})
                    .use_line_framer("\n", false, 128)
                    .on_connect([](const wrapper::ConnectionContext&) {})
                    .on_disconnect([](const wrapper::ConnectionContext&) {})
                    .on_data([](const wrapper::MessageContext&) {})
                    .build();

  ASSERT_NE(client, nullptr);
}

TEST(UdsBuilderOptionsTest, UdsServerBuilderSettersAfterDataHandler) {
  auto builder = builder::UdsServerBuilder("/tmp/test_uds_server_builder_data.sock")
                     .on_data_batch([](const std::vector<wrapper::MessageContext>&) {});

  auto server = std::move(builder)
                    .auto_start(false)
                    .independent_context(false)
                    .idle_timeout(250ms)
                    .max_clients(2)
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

TEST(UdsBuilderOptionsTest, UdsServerBuilderSettersAfterErrorHandler) {
  auto builder =
      builder::UdsServerBuilder("/tmp/test_uds_server_builder_error.sock").on_error([](const wrapper::ErrorContext&) {
      });

  auto server = std::move(builder)
                    .auto_start(false)
                    .independent_context(true)
                    .idle_timeout(500ms)
                    .max_clients(3)
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

TEST(UdsBuilderOptionsTest, UdsServerLegacyClientLimitHelpers) {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  auto single = builder::UdsServerBuilder("/tmp/test_uds_server_single.sock")
                    .single_client()
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  auto multi = builder::UdsServerBuilder("/tmp/test_uds_server_multi.sock")
                   .multi_client(4)
                   .on_data([](auto&&) {})
                   .on_error([](auto&&) {})
                   .build();
  EXPECT_THROW(builder::UdsServerBuilder("/tmp/test_uds_server_bad_multi.sock").multi_client(0),
               diagnostics::BuilderException);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

  EXPECT_NE(single, nullptr);
  EXPECT_NE(multi, nullptr);
}

TEST(UdsBuilderOptionsTest, InvalidPathThrows) {
  EXPECT_THROW(builder::UdsClientBuilder(""), diagnostics::BuilderException);
  EXPECT_THROW(builder::UdsServerBuilder(""), diagnostics::BuilderException);
}

TEST(UdsBuilderOptionsTest, MissingCallbacksAreAllowed) {
  auto client = builder::UdsClientBuilder("/tmp/test_uds_client_missing.sock").auto_start(false).build();
  auto server = builder::UdsServerBuilder("/tmp/test_uds_server_missing.sock").auto_start(false).build();

  EXPECT_NE(client, nullptr);
  EXPECT_NE(server, nullptr);
  EXPECT_FALSE(client->connected());
  EXPECT_FALSE(server->listening());
}

}  // namespace test
}  // namespace wirestead
