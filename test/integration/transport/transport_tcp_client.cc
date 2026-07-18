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
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "test/utils/test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"

using namespace wirestead;
using namespace wirestead::transport;
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

TEST_F(TransportTcpClientTest, BackpressureTriggersWithoutConnection) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 0;                       // invalid/closed port, no real connection expected
  cfg.backpressure_threshold = 1024;  // 1KB threshold

  client_ = TcpClient::create(cfg);
  std::atomic<bool> triggered{false};
  std::atomic<size_t> bytes_seen{0};
  client_->on_backpressure([&](size_t bytes) {
    triggered = true;
    bytes_seen = bytes;
  });

  client_->start();

  // Queue data larger than threshold to trigger backpressure on the queue
  std::vector<uint8_t> payload(cfg.backpressure_threshold * 4, 0xAA);
  client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));

  bool observed = TestUtils::waitForCondition(
      [&] { return triggered.load() && bytes_seen.load() >= cfg.backpressure_threshold; }, 500);

  EXPECT_TRUE(observed);

  client_->stop();
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
  client_->stop();

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
    client_->stop();
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
    client_->stop();
    client_->stop();
    client_->start();
    client_->stop();
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

  client_->stop();
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
  ioc.run_for(150ms);

  EXPECT_EQ(error_events.load(), 0);
  // At least two Connecting states: initial + post-exception reconnect attempt
  EXPECT_GE(connecting_events.load(), 2);

  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientTest, MoveWriteRespectsQueueLimit) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();  // no real server needed
  cfg.backpressure_threshold = 1024;             // bp_high = 1KB

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t bytes) {
    if (bytes > 0) backpressure_seen = true;
  });

  // 512KB > bp_high (1KB) and < bp_limit (1MB): write is enqueued and backpressure fires
  std::vector<uint8_t> huge(cfg.backpressure_threshold * 512, 0xCD);
  client_->async_write_move(std::move(huge));

  ioc.run_for(std::chrono::milliseconds(20));

  EXPECT_TRUE(backpressure_seen.load());

  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientTest, SharedWriteRespectsQueueLimit) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();  // no real server needed
  cfg.backpressure_threshold = 1024;             // bp_high = 1KB

  client_ = TcpClient::create(cfg, ioc);

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t bytes) {
    if (bytes > 0) backpressure_seen = true;
  });

  // 512KB > bp_high (1KB) and < bp_limit (1MB): write is enqueued and backpressure fires
  auto huge = std::make_shared<const std::vector<uint8_t>>(cfg.backpressure_threshold * 512, 0xAB);
  client_->async_write_shared(huge);

  ioc.run_for(std::chrono::milliseconds(20));

  EXPECT_TRUE(backpressure_seen.load());

  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientTest, BackpressureReliefEmitsAfterDrain) {
  net::io_context ioc;
  auto guard = net::make_work_guard(ioc);
  std::thread ioc_thread([&]() { ioc.run(); });

  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 0;  // no real socket use
  cfg.backpressure_threshold = 1024;

  client_ = TcpClient::create(cfg, ioc);

  std::vector<size_t> bp_events;
  client_->on_backpressure([&](size_t queued) { bp_events.push_back(queued); });

  // Queue enough bytes to trigger backpressure
  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0xAB);
  client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size()));

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return !bp_events.empty(); }, 200));

  // Stopping clears the queue and should NOT emit a relief notification according to the Contract
  client_->stop();
  // Wait a bit to ensure no trailing events
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Expect only the initial backpressure trigger, no relief event after stop
  ASSERT_EQ(bp_events.size(), 1);
  EXPECT_GE(bp_events.front(), cfg.backpressure_threshold);

  client_.reset();
  guard.reset();
  ioc.stop();
  if (ioc_thread.joinable()) {
    ioc_thread.join();
  }
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
  ioc.run_for(std::chrono::milliseconds(500));

  EXPECT_GE(connecting_count.load(), 2);

  client_->stop();
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
  ioc.run_for(std::chrono::milliseconds(500));

  EXPECT_GE(connecting_count.load(), 2);

  client_->stop();
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

  client_->stop();

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
  ioc.run_for(std::chrono::milliseconds(500));

  EXPECT_GE(connecting_count.load(), 3);

  client_->stop();
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
  ioc.run_for(std::chrono::milliseconds(1000));

  EXPECT_GE(connecting_count.load(), 5);

  client_->stop();
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
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();

  client_ = TcpClient::create(cfg, ioc);

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

  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientTest, WritesAfterStopReturnFalse) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();

  client_ = TcpClient::create(cfg, ioc);
  client_->stop();

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
  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientTest, CallbackExceptionsAreSwallowed) {
  net::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.backpressure_threshold = 1024;

  client_ = TcpClient::create(cfg, ioc);
  client_->on_state([](base::LinkState) { throw std::runtime_error("state"); });
  client_->on_backpressure([](size_t) { throw std::runtime_error("backpressure"); });

  std::vector<uint8_t> payload(cfg.backpressure_threshold * 2, 0xAA);
  EXPECT_TRUE(client_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));

  EXPECT_NO_THROW({
    client_->start();
    ioc.run_for(100ms);
  });

  client_->on_state(nullptr);
  client_->on_backpressure(nullptr);
  client_->stop();
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
  client_->stop();
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
  ioc.run_for(200ms);

  EXPECT_GE(connecting_events.load(), 2);

  client_->on_state(nullptr);
  client_->on_bytes(nullptr);
  client_->stop();
  client_.reset();
}
