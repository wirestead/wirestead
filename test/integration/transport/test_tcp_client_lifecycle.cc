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

#include <boost/asio.hpp>

#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead::transport;

namespace wirestead {
namespace test {

TEST(TcpClientLifecycleTest, StopBeforeStart) {
  config::TcpClientConfig cfg;
  auto client = TcpClient::create(cfg);
  client->stop();  // Should return early due to stop_requested_
  EXPECT_FALSE(client->is_connected());
}

TEST(TcpClientLifecycleTest, RedundantStop) {
  config::TcpClientConfig cfg;
  auto client = TcpClient::create(cfg);
  client->start();
  client->stop();
  client->stop();  // Should return early from exchange(true)
}

TEST(TcpClientLifecycleTest, WriteInClosedState) {
  config::TcpClientConfig cfg;
  auto client = TcpClient::create(cfg);
  // Initial state is Closed or Idle.
  std::string data = "test";
  client->async_write_copy(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  // Should return early
}

TEST(TcpClientLifecycleTest, WriteZeroLength) {
  config::TcpClientConfig cfg;
  auto client = TcpClient::create(cfg);
  client->start();
  client->async_write_copy(memory::ConstByteSpan(nullptr, 0));
  // Should return early
  client->stop();
}

}  // namespace test
}  // namespace wirestead
