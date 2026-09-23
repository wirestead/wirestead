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

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "test/utils/test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"

using namespace wirestead;
using namespace wirestead::transport;

namespace {

// Drives the io_context in slices until `predicate` holds, or the timeout
// expires. The retry tests below used to run for a fixed duration and then
// assert on how many attempts had happened, which races anything that slows
// the machine down: a slow resolver, a loaded runner, coverage
// instrumentation. Waiting on the condition instead makes them finish as soon
// as the attempts arrive and tolerate the cases where they arrive late.
template <typename Predicate>
bool run_until(boost::asio::io_context& ioc, Predicate predicate,
               std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    if (ioc.stopped()) {
      ioc.restart();
    }
    ioc.run_for(std::chrono::milliseconds(10));
  }
  return true;
}

}  // namespace
using namespace wirestead::test;
using namespace std::chrono_literals;
namespace net = boost::asio;
using tcp = net::ip::tcp;

class TransportTcpClientTest : public ::testing::Test {
 protected:
  void TearDown() override {
    if (client_) {
      client_->stop();
      client_.reset();
    }
    TestUtils::waitFor(50);
  }

  std::shared_ptr<TcpClient> client_;
};

TEST_F(TransportTcpClientTest, BackpressureTriggersWhenConnected) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.port = acceptor.local_endpoint().port();
  cfg.backpressure_threshold = 1024;
  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));
  std::atomic<size_t> bytes_seen{0};
  client_->on_backpressure([&](size_t bytes) {
    if (bytes > bytes_seen.load()) bytes_seen = bytes;
  });
  std::vector<uint8_t> payload(cfg.backpressure_threshold * 4, 0xAA);
  EXPECT_TRUE(client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_TRUE(run_until(ioc, [&] { return bytes_seen.load() >= cfg.backpressure_threshold; }));
  client_->on_backpressure(nullptr);
  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, CreateProvidesSharedFromThis) {
  config::TcpClientConfig cfg;
  cfg.host = "localhost";
  cfg.port = 0;

  auto client = TcpClient::create(cfg);
  EXPECT_NO_THROW({
    auto self = client->shared_from_this();
    EXPECT_EQ(self.get(), client.get());
  });
  client->stop();
}

TEST_F(TransportTcpClientTest, TcpServerCreateProvidesSharedFromThis) {
  config::TcpServerConfig cfg;
  cfg.port = 0;
  auto server = TcpServer::create(cfg);
  EXPECT_NO_THROW({
    auto self = server->shared_from_this();
    EXPECT_EQ(self.get(), server.get());
  });
  server->stop();
}

TEST_F(TransportTcpClientTest, StopPreventsReconnectAfterManualStop) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "256.256.256.256";  // force resolve failure quickly
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.retry_interval_ms = 30;

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<bool> stop_called{false};
  std::atomic<int> reconnect_after_stop{0};
  client_->on_state([&](base::LinkState state) {
    if (stop_called.load() && state == base::LinkState::Connecting) {
      reconnect_after_stop.fetch_add(1);
    }
  });

  client_->start();
  ioc.run_for(std::chrono::milliseconds(20));

  stop_called.store(true);
  stop_with_context(client_, ioc);

  // Run longer than retry interval; should not see Connecting after stop
  ioc.run_for(std::chrono::milliseconds(100));
  EXPECT_EQ(reconnect_after_stop.load(), 0);

  // Ensure client is destroyed before io_context goes out of scope
  client_.reset();
}

TEST_F(TransportTcpClientTest, ExternalIoContextFlowsThroughLifecycle) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "localhost";
  cfg.port = 0;  // invalid port to avoid real connect
  cfg.retry_interval_ms = 20;

  client_ = TcpClient::create(cfg, ioc);

  ASSERT_NO_THROW({
    client_->start();
    ioc.run_for(std::chrono::milliseconds(10));
    stop_with_context(client_, ioc);
    ioc.run_for(std::chrono::milliseconds(10));
  });

  // Destroy client before io_context is torn down to avoid dangling pointer
  client_.reset();
}

TEST_F(TransportTcpClientTest, StartStopIdempotent) {
  // Use external io_context to avoid internal thread/join issues
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "localhost";
  cfg.port = 0;  // invalid/closed port

  client_ = TcpClient::create(cfg, ioc);

  // Multiple start/stop cycles should be safe even without running io_context
  EXPECT_NO_THROW({
    client_->start();
    client_->start();
    stop_with_context(client_, ioc);
    stop_with_context(client_, ioc);
    client_->start();
    stop_with_context(client_, ioc);
  });

  // Destroy client before io_context is torn down to avoid dangling pointer
  client_.reset();
}

TEST_F(TransportTcpClientTest, QueueLimitDropsMessage) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();  // no real server needed
  cfg.backpressure_threshold = 1024;             // bp_limit = max(4KB, 512KB) = 512KB

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t) { backpressure_seen = true; });

  // bp_limit_ = max(backpressure_threshold * 4, DEFAULT_BACKPRESSURE_THRESHOLD) = 1MB here
  // (backpressure_threshold=1024 * 4 = 4KB is below the 1MB default floor).
  // A 2MB write exceeds it outright and is now rejected synchronously via
  // try_reserve_limit_bytes() (jwsung91/wirestead#517) instead of being
  // accepted and only discovered too-large once routed onto the strand -
  // matching every other transport's existing fallback-path precheck (see
  // e.g. TransportTcpServerSessionTest.QueueLimitDropsMessage). Backpressure
  // does not fire since the write is never queued.
  std::vector<uint8_t> huge(2 * 1024 * 1024, 0xEF);
  EXPECT_FALSE(client_->async_write_copy(memory::ConstByteSpan(huge.data(), huge.size())));

  ioc.run_for(std::chrono::milliseconds(50));

  EXPECT_FALSE(backpressure_seen.load());
  auto stats = client_->stats();
  EXPECT_GE(stats.failed_sends, 1u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, OnBytesExceptionTriggersReconnect) {
  net::io_context ioc;

  // Spin up a local acceptor to allow a real connection and deliver one read.
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  auto port = acceptor.local_endpoint().port();

  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.retry_interval_ms = 20;

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_events{0};
  std::atomic<int> error_events{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_events.fetch_add(1);
    if (state == base::LinkState::Error) error_events.fetch_add(1);
  });

  client_->on_bytes([](memory::ConstByteSpan) { throw std::runtime_error("boom"); });

  // Accept a client and send a small payload to trigger on_bytes
  acceptor.async_accept([&](const boost::system::error_code& ec, tcp::socket sock) {
    if (!ec) {
      auto data = std::make_shared<std::string>("ping");
      net::async_write(sock, net::buffer(*data), [data](auto, auto) {});
    }
  });

  client_->start();

  // Run enough to connect, receive, throw, and schedule a retry
  run_until(ioc, [&] { return connecting_events.load() >= 2; });

  EXPECT_EQ(error_events.load(), 0);
  // At least two Connecting states: initial + post-exception reconnect attempt
  EXPECT_GE(connecting_events.load(), 2);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, MoveWriteRespectsQueueLimit) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();
  cfg.backpressure_threshold = 1024;  // bp_high = 1KB

  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t bytes) {
    if (bytes > 0) backpressure_seen = true;
  });

  // 512KB > bp_high (1KB) and < bp_limit (1MB): write is enqueued and backpressure fires
  std::vector<uint8_t> huge(cfg.backpressure_threshold * 512, 0xCD);
  client_->async_write_move(std::move(huge));

  ioc.run_for(std::chrono::milliseconds(20));

  EXPECT_TRUE(backpressure_seen.load());

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, SharedWriteRespectsQueueLimit) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();
  cfg.backpressure_threshold = 1024;  // bp_high = 1KB

  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t bytes) {
    if (bytes > 0) backpressure_seen = true;
  });

  // 512KB > bp_high (1KB) and < bp_limit (1MB): write is enqueued and backpressure fires
  auto huge = std::make_shared<const std::vector<uint8_t>>(cfg.backpressure_threshold * 512, 0xAB);
  client_->async_write_shared(huge);

  ioc.run_for(std::chrono::milliseconds(20));

  EXPECT_TRUE(backpressure_seen.load());

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, StopDoesNotEmitBackpressureRelief) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.port = acceptor.local_endpoint().port();
  cfg.backpressure_threshold = 1024;
  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));
  std::vector<size_t> bp_events;
  client_->on_backpressure([&](size_t queued) {
    bp_events.push_back(queued);
    client_->stop();
  });
  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0xAB);
  EXPECT_TRUE(client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_TRUE(run_until(ioc, [&] { return !bp_events.empty(); }));
  stop_with_context(client_, ioc);
  EXPECT_EQ(bp_events.size(), 1u);
  if (!bp_events.empty()) EXPECT_GE(bp_events.front(), cfg.backpressure_threshold);
  client_->on_backpressure(nullptr);
  client_.reset();
}

TEST_F(TransportTcpClientTest, ConnectionRefusedTriggersRetry) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();  // Port not listening
  cfg.retry_interval_ms = 50;
  // Ensure we timeout quickly if OS doesn't send RST immediately (common on Windows)
  cfg.connection_timeout_ms = 100;

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
    }
  });

  client_->start();

  // Run enough time for initial attempt + at least one retry
  // Initial (0ms) + Timeout(100ms) + Interval(50ms) + Retry(0ms) = ~150ms minimum
  run_until(ioc, [&] { return connecting_count.load() >= 2; });

  EXPECT_GE(connecting_count.load(), 2);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, ResolveFailureTriggersRetry) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "invalid.host.name.that.does.not.exist";
  cfg.port = 80;
  cfg.retry_interval_ms = 50;

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
    }
  });

  client_->start();

  // Run enough time for initial attempt + retry. Resolve might take time, give it margin.
  run_until(ioc, [&] { return connecting_count.load() >= 2; });

  EXPECT_GE(connecting_count.load(), 2);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, MaxRetriesStopsReconnection) {
  boost::asio::io_context ioc;

  config::TcpClientConfig cfg;

  cfg.host = "127.0.0.1";

  cfg.port = TestUtils::getAvailableTestPort();

  cfg.retry_interval_ms = 50;

  cfg.connection_timeout_ms = 200;  // Increased slightly

  cfg.max_retries = 0;  // Initial + 0 retries = 1 attempt total

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_count{0};

  std::atomic<bool> error_state{false};

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);

    } else if (state == base::LinkState::Error) {
      error_state = true;
    }
  });

  client_->start();

  // Run enough time for the single attempt to fail and error to propagate.

  // 1 attempt * (Timeout 200ms + Interval 50ms) = 250ms.

  // Give it plenty of time (1000ms).

  ioc.run_for(std::chrono::milliseconds(1000));

  if (!error_state.load()) {
    std::cout << "Failed to reach Error state. Connecting count: " << connecting_count.load() << std::endl;
  }

  // Expect 1 or 2:

  // 1: Initial Start -> Connecting -> Failure -> Error

  // 2: Initial Start -> Connecting -> Failure -> handle_close(Connecting) -> Error

  // Both indicate it stopped retrying.

  int count = connecting_count.load();

  EXPECT_TRUE(count == 1 || count == 2) << "Unexpected connecting count: " << count;

  EXPECT_TRUE(error_state.load());

  stop_with_context(client_, ioc);

  client_.reset();
}

TEST_F(TransportTcpClientTest, ConnectionTimeoutTriggersRetry) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "10.255.255.1";  // Unreachable IP to force timeout (or route failure)
  cfg.port = 80;
  cfg.connection_timeout_ms = 50;  // Short timeout
  cfg.retry_interval_ms = 50;
  cfg.max_retries = 2;

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
    }
  });

  client_->start();

  // Run enough for initial + 2 retries (timeout 50ms + retry interval 50ms = 100ms per attempt)
  run_until(ioc, [&] { return connecting_count.load() >= 3; });

  EXPECT_GE(connecting_count.load(), 3);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, UnlimitedRetriesKeepsConnecting) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.retry_interval_ms = 50;      // Increased to 50ms for better timer resolution on Windows
  cfg.connection_timeout_ms = 50;  // Ensure fast failure
  cfg.max_retries = -1;            // Unlimited

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
    }
  });

  client_->start();

  // Run for enough time to get 5 attempts.
  // Each attempt: Timeout(50) + Interval(50) = 100ms.
  // 5 attempts needs ~500ms. Give it 1000ms.
  run_until(ioc, [&] { return connecting_count.load() >= 5; });

  EXPECT_GE(connecting_count.load(), 5);

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, OwnedIoContextRestartAfterStopStart) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 0;
  cfg.max_retries = 0;  // avoid retry storm

  client_ = TcpClient::create(cfg);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
    }
  });

  client_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return connecting_count.load() >= 1; }, 200));

  client_->stop();
  TestUtils::waitFor(20);

  client_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return connecting_count.load() >= 2; }, 200));

  client_->stop();
}

TEST_F(TransportTcpClientTest, WriteRejectsInvalidPayloads) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();

  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));

  std::vector<uint8_t> empty;
  EXPECT_FALSE(client_->async_write_copy(memory::ConstByteSpan(empty.data(), empty.size())));
  EXPECT_FALSE(client_->async_write_move({}));
  EXPECT_FALSE(client_->async_write_shared(nullptr));
  EXPECT_FALSE(client_->async_write_shared(std::make_shared<const std::vector<uint8_t>>()));

  std::vector<uint8_t> too_large(base::constants::MAX_BUFFER_SIZE + 1, 0x01);
  EXPECT_FALSE(client_->async_write_copy(memory::ConstByteSpan(too_large.data(), too_large.size())));
  EXPECT_FALSE(client_->async_write_move(std::move(too_large)));

  auto too_large_shared = std::make_shared<const std::vector<uint8_t>>(base::constants::MAX_BUFFER_SIZE + 1, 0x02);
  EXPECT_FALSE(client_->async_write_shared(too_large_shared));

  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, WritesAfterStopReturnFalse) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();

  client_ = TcpClient::create(cfg, ioc);
  stop_with_context(client_, ioc);

  std::vector<uint8_t> payload = {0x01, 0x02};
  EXPECT_FALSE(client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_FALSE(client_->async_write_move(std::vector<uint8_t>{0x03}));
  EXPECT_FALSE(client_->async_write_shared(std::make_shared<const std::vector<uint8_t>>(payload)));

  client_.reset();
}

TEST_F(TransportTcpClientTest, SettersAndClearedReconnectPolicyAffectRetry) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();

  client_ = TcpClient::create(cfg, ioc);
  client_->set_reconnect_policy(FixedInterval(5ms));
  client_->set_reconnect_policy(nullptr);
  client_->set_retry_interval(20);
  client_->set_connection_timeout(20);
  client_->set_max_retries(0);

  std::atomic<bool> error_state{false};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_state = true;
    }
  });

  client_->start();
  ioc.run_for(300ms);

  EXPECT_TRUE(error_state.load());

  client_->on_state(nullptr);
  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, CallbackExceptionsAreSwallowed) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();
  cfg.backpressure_threshold = 1024;

  client_ = TcpClient::create(cfg, ioc);
  client_->start();
  ASSERT_TRUE(run_until(ioc, [&] { return client_->is_connected(); }));
  client_->on_state([](base::LinkState) { throw std::runtime_error("state"); });
  client_->on_backpressure([](size_t) { throw std::runtime_error("backpressure"); });

  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0xAA);
  EXPECT_TRUE(client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));

  EXPECT_NO_THROW({
    boost::system::error_code ec;
    acceptor.close(ec);
    ioc.run_for(100ms);
  });

  client_->on_state(nullptr);
  client_->on_backpressure(nullptr);
  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, SharedWriteSendsPayloadWhenConnected) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));

  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();
  cfg.connection_timeout_ms = 200;

  client_ = TcpClient::create(cfg, ioc);

  auto server_socket = std::make_shared<tcp::socket>(ioc);
  auto buffer = std::make_shared<std::array<uint8_t, 64>>();
  std::string received;
  std::atomic<bool> connected{false};
  std::atomic<bool> received_done{false};

  acceptor.async_accept(*server_socket, [&, server_socket, buffer](const boost::system::error_code& ec) {
    if (ec) {
      return;
    }
    server_socket->async_read_some(net::buffer(*buffer),
                                   [&, buffer](const boost::system::error_code& read_ec, std::size_t n) {
                                     if (!read_ec) {
                                       received.assign(reinterpret_cast<const char*>(buffer->data()), n);
                                       received_done = true;
                                     }
                                   });
  });

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected = true;
    }
  });

  client_->start();
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        return connected.load();
      },
      1000));

  auto payload = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{'s', 'h', 'a', 'r', 'e', 'd'});
  EXPECT_TRUE(client_->async_write_shared(payload));

  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        ioc.poll();
        ioc.restart();
        return received_done.load();
      },
      1000));
  EXPECT_EQ(received, "shared");

  client_->on_state(nullptr);
  stop_with_context(client_, ioc);
  client_.reset();
}

TEST_F(TransportTcpClientTest, UnknownOnBytesExceptionTriggersReconnect) {
  net::io_context ioc;
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));

  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = acceptor.local_endpoint().port();
  cfg.retry_interval_ms = 20;
  cfg.connection_timeout_ms = 100;

  client_ = TcpClient::create(cfg, ioc);

  auto server_socket = std::make_shared<tcp::socket>(ioc);
  acceptor.async_accept(*server_socket, [server_socket](const boost::system::error_code& ec) {
    if (ec) {
      return;
    }
    auto data = std::make_shared<std::string>("ping");
    net::async_write(*server_socket, net::buffer(*data),
                     [server_socket, data](const boost::system::error_code&, auto) {});
  });

  std::atomic<int> connecting_events{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_events.fetch_add(1);
    }
  });
  client_->on_bytes([](memory::ConstByteSpan) { throw 7; });

  client_->start();
  run_until(ioc, [&] { return connecting_events.load() >= 2; });

  EXPECT_GE(connecting_events.load(), 2);

  client_->on_state(nullptr);
  client_->on_bytes(nullptr);
  stop_with_context(client_, ioc);
  client_.reset();
}

namespace {
struct ReconnectWritePeer {
  net::io_context io;
  tcp::acceptor acceptor{io, tcp::endpoint(tcp::v4(), 0)};
  tcp::socket first{io}, second{io};
  net::streambuf received;
  std::shared_ptr<TcpClient> client;
  int connections = 0;
  bool read_done = false;
  bool fresh_accepted = false;
  boost::system::error_code read_error;

  explicit ReconnectWritePeer(base::constants::BackpressureStrategy strategy, size_t threshold = 1024) {
    acceptor.set_option(net::socket_base::receive_buffer_size(1024));
    config::TcpClientConfig cfg;
    cfg.port = acceptor.local_endpoint().port();
    cfg.send_buffer_size = 1024;
    cfg.backpressure_threshold = threshold;
    cfg.backpressure_strategy = strategy;
    cfg.retry_interval_ms = 20;
    client = TcpClient::create(cfg, io);
    client->on_state([this](base::LinkState state) {
      if (state == base::LinkState::Connected && ++connections == 2) {
        fresh_accepted = client->async_write_move(std::vector<uint8_t>{'n', 'e', 'w', '\n'});
      }
    });
    client->on_bytes([](memory::ConstByteSpan) { throw std::runtime_error("end first connection"); });
    acceptor.async_accept(first, [this](auto ec) {
      if (ec) return;
      acceptor.async_accept(second, [this](auto next_ec) {
        if (next_ec) return;
        net::async_read_until(second, received, '\n', [this](auto ec, size_t) {
          read_error = ec;
          read_done = true;
        });
      });
    });
    client->start();
  }
  ~ReconnectWritePeer() {
    client->on_bytes(nullptr);
    client->on_state(nullptr);
    stop_with_context(client, io);
  }
  void trigger_loss() { net::write(first, net::buffer("!", 1)); }
  std::string line() {
    std::istream stream(&received);
    std::string result;
    std::getline(stream, result);
    return result;
  }
};

class TcpReconnectPostedWriteTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpReconnectPostedWriteTest, OldSubmissionNeverReachesNewConnection) {
  using Strategy = base::constants::BackpressureStrategy;
  ReconnectWritePeer peer(GetParam() < 6 ? Strategy::Reliable : Strategy::BestEffort);
  ASSERT_TRUE(run_until(peer.io, [&] { return peer.connections == 1 && peer.first.is_open(); }));
  bool old_accepted = false;
  peer.client->on_bytes([&](memory::ConstByteSpan) {
    std::vector<uint8_t> old{'o', 'l', 'd', '\n'};
    auto shared = std::make_shared<const std::vector<uint8_t>>(old);
    switch (GetParam() % 6) {
      case 0:
        old_accepted = peer.client->async_write_copy(memory::ConstByteSpan(old.data(), old.size()));
        break;
      case 1:
        old_accepted = peer.client->async_write_move(std::move(old));
        break;
      case 2:
        old_accepted = peer.client->async_write_shared(shared);
        break;
      case 3:
        old_accepted = peer.client->async_try_write_copy(memory::ConstByteSpan(old.data(), old.size()));
        break;
      case 4:
        old_accepted = peer.client->async_try_write_move(std::move(old));
        break;
      default:
        old_accepted = peer.client->async_try_write_shared(shared);
        break;
    }
    // The posted write cannot run until this callback closes the connection.
    throw std::runtime_error("end first connection after submission");
  });
  peer.trigger_loss();
  ASSERT_TRUE(run_until(peer.io, [&] { return peer.read_done; }));
  EXPECT_FALSE(peer.read_error);
  EXPECT_TRUE(old_accepted);
  EXPECT_TRUE(peer.fresh_accepted);
  EXPECT_EQ(peer.line(), "new");
  EXPECT_EQ(peer.client->stats().dropped_messages, 1u);
  EXPECT_EQ(peer.client->stats().dropped_bytes, 4u);
}
INSTANTIATE_TEST_SUITE_P(AllStrategiesAndWriteForms, TcpReconnectPostedWriteTest, ::testing::Range(0, 12));

class TcpReconnectWriteTest : public ::testing::TestWithParam<bool> {};
TEST_P(TcpReconnectWriteTest, DiscardsActiveAndWaitingDataBeforeReconnect) {
  ReconnectWritePeer peer(base::constants::BackpressureStrategy::Reliable, GetParam() ? 1024 : 1024 * 1024);
  ASSERT_TRUE(run_until(peer.io, [&] { return peer.connections == 1 && peer.first.is_open(); }));
  constexpr size_t active_size = 512 * 1024;
  EXPECT_TRUE(peer.client->async_write_move(std::vector<uint8_t>(active_size, 'x')));
  EXPECT_TRUE(peer.client->async_write_move(std::vector<uint8_t>(32, 'y')));
  ASSERT_TRUE(run_until(peer.io, [&] {
    const auto stats = peer.client->stats();
    return GetParam() ? stats.pending_bytes == 32 : stats.queued_bytes == active_size + 32;
  }));
  peer.trigger_loss();
  ASSERT_TRUE(run_until(peer.io, [&] { return peer.read_done; }));
  EXPECT_FALSE(peer.read_error);
  EXPECT_TRUE(peer.fresh_accepted);
  EXPECT_EQ(peer.line(), "new");
  const auto stats = peer.client->stats();
  EXPECT_EQ(stats.dropped_messages, 2u);
  EXPECT_EQ(stats.dropped_bytes, active_size + 32);
  EXPECT_EQ(stats.pending_bytes, 0u);
}
INSTANTIATE_TEST_SUITE_P(QueuedAndPending, TcpReconnectWriteTest, ::testing::Bool());
}  // namespace

namespace {
thread_local std::optional<wrapper::SendResult> observed_admission;
thread_local int admission_observations = 0;
void observe_admission(const wrapper::SendResult& result) {
  observed_admission = result;
  ++admission_observations;
}

struct AdmissionPeer {
  net::io_context io;
  tcp::acceptor acceptor{io, tcp::endpoint(tcp::v4(), 0)};
  std::shared_ptr<TcpClient> client;
  explicit AdmissionPeer(bool best_effort, bool pooled) {
    config::TcpClientConfig cfg;
    cfg.port = acceptor.local_endpoint().port();
    cfg.backpressure_threshold = 1024;
    cfg.enable_memory_pool = pooled;
    cfg.backpressure_strategy = best_effort ? base::constants::BackpressureStrategy::BestEffort
                                            : base::constants::BackpressureStrategy::Reliable;
    client = TcpClient::create(cfg, io);
    detail::g_tcp_write_result_hook.store(observe_admission);
  }
  ~AdmissionPeer() {
    detail::g_tcp_write_result_hook.store(nullptr);
    stop_with_context(client, io);
  }
};

class TcpAdmissionResultTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpAdmissionResultTest, RetainsDecisionAndExistingAccounting) {
  const int form = GetParam() % 6;
  const bool best_effort = (GetParam() / 6) % 2;
  AdmissionPeer peer(best_effort, GetParam() >= 12);
  auto write = [&](std::vector<uint8_t>& payload) {
    observed_admission.reset();
    admission_observations = 0;
    const auto before = payload;
    bool accepted = false;
    switch (form) {
      case 0:
        accepted = peer.client->async_write_copy({payload.data(), payload.size()});
        break;
      case 1:
        accepted = peer.client->async_write_move(std::move(payload));
        break;
      case 2:
        accepted = peer.client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(payload));
        break;
      case 3:
        accepted = peer.client->async_try_write_copy({payload.data(), payload.size()});
        break;
      case 4:
        accepted = peer.client->async_try_write_move(std::move(payload));
        break;
      case 5:
        accepted = peer.client->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(payload));
        break;
    }
    EXPECT_EQ(admission_observations, 1);
    EXPECT_TRUE(observed_admission.has_value());
    if (observed_admission) {
      EXPECT_EQ(observed_admission->accepted(), accepted);
    }
    if (!accepted) {
      EXPECT_EQ(payload, before) << "rejected move must preserve caller storage";
    }
    return accepted;
  };
  auto expect_reason = [&](wrapper::SendRejection expected) {
    ASSERT_TRUE(observed_admission.has_value());
    ASSERT_FALSE(observed_admission->accepted());
    EXPECT_EQ(observed_admission->reason(), expected);
  };

  std::vector<uint8_t> payload(16, 'a');
  EXPECT_FALSE(write(payload));
  expect_reason(wrapper::SendRejection::NotReady);

  peer.client->start();
  ASSERT_TRUE(run_until(peer.io, [&] { return peer.client->is_connected(); }));
  std::vector<uint8_t> empty;
  EXPECT_FALSE(write(empty));
  expect_reason(wrapper::SendRejection::InvalidArgument);
  std::vector<uint8_t> oversized(base::constants::MAX_BUFFER_SIZE + 1, 'x');
  EXPECT_FALSE(write(oversized));
  expect_reason(wrapper::SendRejection::TooLarge);
  oversized.clear();

  // No executor progress after readiness: reservations cannot drain before the
  // second call. Ordinary Reliable fills its hard cap; try writes fill high-water.
  const size_t capacity = form < 3 ? *peer.client->write_queue_limit() : 1024;
  std::vector<uint8_t> fill(capacity, 'f');
  EXPECT_TRUE(write(fill));
  ASSERT_TRUE(observed_admission->accepted());
  const auto before_pressure = peer.client->stats();
  EXPECT_EQ(before_pressure.messages_accepted, 1u);
  EXPECT_EQ(before_pressure.failed_sends, 3u);
  EXPECT_EQ(before_pressure.dropped_messages, 0u);
  payload.assign(16, 'a');
  const bool accepted_under_pressure = write(payload);
  const bool ordinary_best_effort = form < 3 && best_effort;
  EXPECT_EQ(accepted_under_pressure, ordinary_best_effort);
  if (!ordinary_best_effort) expect_reason(wrapper::SendRejection::WouldBlock);
  const auto after_pressure = peer.client->stats();
  if (ordinary_best_effort) {
    EXPECT_EQ(after_pressure.messages_accepted, 2u);
  } else if (best_effort) {
    EXPECT_EQ(after_pressure.dropped_messages, before_pressure.dropped_messages + 1);
    EXPECT_EQ(after_pressure.failed_sends, before_pressure.failed_sends);
  } else {
    EXPECT_EQ(after_pressure.failed_sends, before_pressure.failed_sends + 1);
    EXPECT_EQ(after_pressure.dropped_messages, before_pressure.dropped_messages);
  }
  // The result is a value, unaffected by the subsequent lifecycle transition.
  const auto saved = *observed_admission;
  stop_with_context(peer.client, peer.io);
  EXPECT_EQ(saved.accepted(), accepted_under_pressure);
  if (!saved.accepted()) {
    EXPECT_EQ(saved.reason(), wrapper::SendRejection::WouldBlock);
  }
  payload.assign(16, 'a');
  EXPECT_FALSE(write(payload));
  expect_reason(wrapper::SendRejection::NotReady);
}
INSTANTIATE_TEST_SUITE_P(FormsStrategiesAndPool, TcpAdmissionResultTest, ::testing::Range(0, 24));
}  // namespace
