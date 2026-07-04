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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "test/mocks/mock_uds_socket.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/memory/safe_span.hpp"
#include "unilink/transport/base/reconnect_policy.hpp"
#include "unilink/transport/uds/uds_client.hpp"

using namespace unilink;
using namespace unilink::transport;
using namespace unilink::test::mocks;
using namespace unilink::test;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArg;

class TransportUdsClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    unilink::diagnostics::Logger::instance().set_console_output(true);
    unilink::diagnostics::Logger::instance().set_level(unilink::diagnostics::LogLevel::DEBUG);

    auto temp_path = TestUtils::makeUniqueUdsSocketPath("ulc");
    cfg.socket_path = temp_path.string();
    TestUtils::removeFileIfExists(temp_path);

    mock_socket = new MockUdsSocket();
    EXPECT_CALL(*mock_socket, close(_)).Times(AnyNumber());
    auto socket_ptr = std::unique_ptr<interface::UdsSocketInterface>(mock_socket);
    // Use an external io_context from IoContextManager to match common project patterns
    client = UdsClient::create(cfg, std::move(socket_ptr), ioc);
  }

  void TearDown() override {
    if (client) {
      client->stop();
      client.reset();
    }
    TestUtils::removeFileIfExists(cfg.socket_path);
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(50));
  }

  boost::asio::io_context ioc;
  config::UdsClientConfig cfg;
  MockUdsSocket* mock_socket;
  std::shared_ptr<UdsClient> client;
};

TEST_F(TransportUdsClientTest, Construction) {
  EXPECT_NE(client, nullptr);
  EXPECT_FALSE(client->is_connected());
}

TEST_F(TransportUdsClientTest, SharedFromThisCheck) {
  // Directly test if shared_from_this() works
  EXPECT_NO_THROW({
    auto self = client->shared_from_this();
    EXPECT_EQ(self.get(), client.get());
  });
}

TEST_F(TransportUdsClientTest, SuccessfulConnection) {
  // Ensure we don't call shared_from_this until it's safe
  auto self = client->shared_from_this();

  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(boost::system::error_code()); });
      }));

  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillRepeatedly(
          Invoke([](const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {
            // Do nothing
          }));

  std::atomic<bool> connected{false};
  client->on_state([&connected](base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected = true;
    }
  });

  client->start();

  // Run io_context to process handlers
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(100));

  EXPECT_TRUE(client->is_connected());
  EXPECT_TRUE(connected);
}

TEST_F(TransportUdsClientTest, ConnectionFailure) {
  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(make_error_code(boost::asio::error::connection_refused)); });
      }));

  std::atomic<bool> has_error{false};
  client->on_state([&has_error](base::LinkState state) {
    if (state == base::LinkState::Error) {
      has_error = true;
    }
  });

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(100));

  EXPECT_FALSE(client->is_connected());
  EXPECT_TRUE(has_error);
}

// Regression test for jwsung91/unilink#445: record_error()'s retry_count
// parameter used to be silently discarded here (an unnamed uint32_t
// parameter in the pre-ErrorInfoHolder implementation), so
// last_error_info()->retry_count always reported 0 regardless of how many
// reconnect attempts had actually happened - unlike TcpClient's equivalent,
// which always set it correctly. Asserts it's now propagated end-to-end
// through the shared ErrorInfoHolder.
TEST_F(TransportUdsClientTest, RepeatedConnectionFailuresRecordIncreasingRetryCount) {
  cfg.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;
  cfg.max_retries = -1;
  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillRepeatedly(Invoke([this](const net::local::stream_protocol::endpoint&,
                                    std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(make_error_code(boost::asio::error::connection_refused)); });
      }));

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(1500));

  ASSERT_TRUE(client->last_error_info().has_value());
  EXPECT_GT(client->last_error_info()->retry_count, 0u);
}

TEST_F(TransportUdsClientTest, WriteData) {
  // Setup successful connection first
  std::cout << "WriteData: setting up async_connect mock" << std::endl;
  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        std::cout << "WriteData: async_connect called, posting handler" << std::endl;
        net::post(ioc, [handler]() {
          std::cout << "WriteData: executing connect handler" << std::endl;
          handler(boost::system::error_code());
        });
      }));

  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillRepeatedly(
          Invoke([](const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {
            // Do nothing, just stay active
          }));

  std::cout << "WriteData: starting client" << std::endl;
  client->start();

  // Give it time to connect
  std::cout << "WriteData: waiting for connection" << std::endl;
  for (int i = 0; i < 10; ++i) {
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(20));
    if (client->is_connected()) break;
  }

  ASSERT_TRUE(client->is_connected());
  std::cout << "WriteData: client connected" << std::endl;

  std::vector<uint8_t> test_data = {1, 2, 3, 4, 5};
  std::atomic<bool> write_called{false};

  EXPECT_CALL(*mock_socket, async_write(_, _))
      .WillOnce(Invoke([&](const net::const_buffer& buffer,
                           std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        std::cout << "WriteData: async_write called" << std::endl;
        EXPECT_EQ(buffer.size(), test_data.size());
        write_called = true;
        net::post(ioc, [handler, size = buffer.size()]() { handler(boost::system::error_code(), size); });
      }));

  std::cout << "WriteData: calling async_write_copy" << std::endl;
  client->async_write_copy(memory::ConstByteSpan(test_data.data(), test_data.size()));

  // Process write
  std::cout << "WriteData: polling for write" << std::endl;
  for (int i = 0; i < 10; ++i) {
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(20));
    if (write_called) break;
  }

  EXPECT_TRUE(write_called);
  std::cout << "WriteData: test finished" << std::endl;
}

TEST_F(TransportUdsClientTest, StartWhileConnectingIsIgnored) {
  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .Times(1)
      .WillOnce(Invoke(
          [](const net::local::stream_protocol::endpoint&, std::function<void(const boost::system::error_code&)>) {
            // Keep the client in Connecting.
          }));

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(20));

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(20));
}

TEST_F(TransportUdsClientTest, InvalidConfigMovesToErrorAndRecordsLastError) {
  cfg.socket_path.clear();
  auto local_mock = new MockUdsSocket();
  EXPECT_CALL(*local_mock, close(_)).Times(AnyNumber());
  auto local_client = UdsClient::create(cfg, std::unique_ptr<interface::UdsSocketInterface>(local_mock), ioc);

  std::atomic<bool> error_seen{false};
  local_client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  local_client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(error_seen.load());
  ASSERT_TRUE(local_client->last_error_info().has_value());
  EXPECT_EQ(local_client->last_error_info()->category, diagnostics::ErrorCategory::CONFIGURATION);

  local_client->stop();
}

TEST_F(TransportUdsClientTest, ConnectionTimeoutRecordsLastError) {
  // #432: connection_timeout_ms is now clamped to MIN_CONNECTION_TIMEOUT_MS -
  // use that value explicitly (rather than an arbitrarily smaller one that
  // would silently get clamped up) and give run_for() enough margin past it.
  cfg.connection_timeout_ms = base::constants::MIN_CONNECTION_TIMEOUT_MS;
  cfg.max_retries = 0;

  auto local_mock = new MockUdsSocket();
  EXPECT_CALL(*local_mock, async_connect(_, _))
      .WillOnce(Invoke(
          [](const net::local::stream_protocol::endpoint&, std::function<void(const boost::system::error_code&)>) {
            // Leave the operation pending so the timeout path fires.
          }));
  EXPECT_CALL(*local_mock, close(_)).Times(AnyNumber());
  auto local_client = UdsClient::create(cfg, std::unique_ptr<interface::UdsSocketInterface>(local_mock), ioc);

  std::atomic<bool> error_seen{false};
  local_client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  local_client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(base::constants::MIN_CONNECTION_TIMEOUT_MS) + std::chrono::milliseconds(200));

  EXPECT_TRUE(error_seen.load());
  ASSERT_TRUE(local_client->last_error_info().has_value());
  EXPECT_EQ(local_client->last_error_info()->operation, "connect");

  local_client->stop();
}

TEST_F(TransportUdsClientTest, ReadCallbackReceivesDataThenCloseSchedulesRetry) {
  cfg.max_retries = 0;
  const std::string payload = "uds-payload";

  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(boost::system::error_code()); });
      }));

  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillOnce(Invoke([this, &payload](const net::mutable_buffer& buffer,
                                        std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        const auto bytes = std::min(buffer.size(), payload.size());
        std::memcpy(buffer.data(), payload.data(), bytes);
        net::post(ioc, [handler, bytes]() { handler(boost::system::error_code(), bytes); });
      }))
      .WillOnce(Invoke([this](const net::mutable_buffer&,
                              std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        net::post(ioc, [handler]() { handler(make_error_code(boost::asio::error::eof), 0); });
      }));
  EXPECT_CALL(*mock_socket, close(_)).Times(AnyNumber());

  std::string received;
  std::atomic<bool> error_seen{false};
  client->on_bytes(
      [&](memory::ConstByteSpan data) { received.assign(reinterpret_cast<const char*>(data.data()), data.size()); });
  client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(100));

  EXPECT_EQ(received, payload);
  EXPECT_TRUE(error_seen.load());
}

TEST_F(TransportUdsClientTest, MoveAndSharedWritesUseSocket) {
  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(boost::system::error_code()); });
      }));

  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillRepeatedly(Invoke(
          [](const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {}));

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(50));
  ASSERT_TRUE(client->is_connected());

  std::atomic<int> writes{0};
  EXPECT_CALL(*mock_socket, async_write(_, _))
      .Times(2)
      .WillRepeatedly(Invoke([&](const net::const_buffer& buffer,
                                 std::function<void(const boost::system::error_code&, std::size_t)> handler) {
        writes.fetch_add(1);
        net::post(ioc, [handler, size = buffer.size()]() { handler(boost::system::error_code(), size); });
      }));

  EXPECT_TRUE(client->async_write_move(std::vector<uint8_t>{1, 2, 3}));
  EXPECT_TRUE(client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{4, 5, 6})));

  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(100));

  EXPECT_EQ(writes.load(), 2);
}

TEST_F(TransportUdsClientTest, WriteRejectsDisconnectedAndOversizedPayloads) {
  std::vector<uint8_t> small = {1, 2, 3};
  EXPECT_FALSE(client->async_write_copy(memory::ConstByteSpan(small.data(), small.size())));
  EXPECT_FALSE(client->async_write_move(std::vector<uint8_t>{1}));
  EXPECT_FALSE(client->async_write_shared(nullptr));
  EXPECT_FALSE(client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(small)));

  EXPECT_CALL(*mock_socket, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(boost::system::error_code()); });
      }));
  EXPECT_CALL(*mock_socket, async_read_some(_, _))
      .WillRepeatedly(Invoke(
          [](const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {}));

  client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(50));
  ASSERT_TRUE(client->is_connected());

  std::vector<uint8_t> too_large(base::constants::MAX_BUFFER_SIZE + 1, 0x01);
  EXPECT_FALSE(client->async_write_move(std::move(too_large)));
}

TEST_F(TransportUdsClientTest, BackpressureCallbackExceptionsAreSwallowed) {
  cfg.backpressure_threshold = 1024;
  auto local_mock = new MockUdsSocket();
  EXPECT_CALL(*local_mock, async_connect(_, _))
      .WillOnce(Invoke([this](const net::local::stream_protocol::endpoint&,
                              std::function<void(const boost::system::error_code&)> handler) {
        net::post(ioc, [handler]() { handler(boost::system::error_code()); });
      }));
  EXPECT_CALL(*local_mock, async_read_some(_, _))
      .WillRepeatedly(Invoke(
          [](const net::mutable_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {}));
  EXPECT_CALL(*local_mock, async_write(_, _))
      .WillOnce(
          Invoke([this](const net::const_buffer&, std::function<void(const boost::system::error_code&, std::size_t)>) {
            // Leave write pending so the queue remains above the high watermark.
          }));
  EXPECT_CALL(*local_mock, close(_)).Times(AnyNumber());

  auto local_client = UdsClient::create(cfg, std::unique_ptr<interface::UdsSocketInterface>(local_mock), ioc);
  local_client->on_backpressure([](size_t) { throw std::runtime_error("backpressure"); });

  local_client->start();
  ioc.restart();
  ioc.run_for(std::chrono::milliseconds(50));
  ASSERT_TRUE(local_client->is_connected());

  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0xAB);
  EXPECT_TRUE(local_client->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));

  EXPECT_NO_THROW({
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(50));
  });
  EXPECT_TRUE(local_client->is_backpressure_active());

  local_client->stop();
}

// #446: UdsClient's move ctor/assignment are defaulted (and public, unlike
// its private regular constructors), so a moved-from instance has a null
// impl_. Destroying it must not dereference that null pointer (matches
// TcpServer/Serial/UdpChannel/UdsServer, whose destructors already
// null-guard). Reachable via the public API by move-constructing from a
// dereferenced create()'d shared_ptr, which leaves the original
// (still-shared_ptr-owned) object moved-from; its destructor runs whenever
// the shared_ptr's refcount reaches zero.
TEST(TransportUdsClientMoveTest, DestroyingAMovedFromInstanceDoesNotCrash) {
  config::UdsClientConfig cfg;
  cfg.socket_path = TestUtils::makeUniqueUdsSocketPath("ulc_move").string();
  TestUtils::removeFileIfExists(cfg.socket_path);

  auto client_ptr = UdsClient::create(cfg);
  UdsClient moved_to(std::move(*client_ptr));
  client_ptr.reset();  // destroys the now moved-from original - the assertion

  TestUtils::removeFileIfExists(cfg.socket_path);
}
