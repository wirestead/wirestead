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
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

#include <boost/asio.hpp>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

#include "test/utils/test_utils.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;
using namespace std::chrono_literals;
namespace net = boost::asio;
using tcp = net::ip::tcp;

class TransportTcpClientPolicyTest : public ::testing::Test {
 protected:
  void TearDown() override {
    if (client_) {
      // Explicitly clear callbacks to prevent use-after-free
      client_->on_state(nullptr);
      client_->on_bytes(nullptr);
      client_->on_backpressure(nullptr);
      client_->stop();
      client_.reset();
    }
    TestUtils::waitFor(50);
  }

  std::shared_ptr<TcpClient> client_;
};

TEST_F(TransportTcpClientPolicyTest, FixedIntervalPolicyRetriesWithDelay) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.retry_interval_ms = 1000;
  cfg.connection_timeout_ms = 50;

  client_ = TcpClient::create(cfg, ioc);

  client_->set_reconnect_policy(FixedInterval(20ms));

  std::atomic<int> connecting_count{0};
  std::vector<std::chrono::steady_clock::time_point> attempt_times;

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      connecting_count.fetch_add(1);
      attempt_times.push_back(std::chrono::steady_clock::now());
    }
  });

  client_->start();

  ioc.run_for(std::chrono::milliseconds(300));

  EXPECT_GE(connecting_count.load(), 3);

  // Debounce attempt times to handle platform-specific redundant state notifications
  std::vector<std::chrono::steady_clock::time_point> filtered_times;
  if (!attempt_times.empty()) {
    filtered_times.push_back(attempt_times[0]);
    for (size_t i = 1; i < attempt_times.size(); ++i) {
      if (attempt_times[i] - attempt_times[i - 1] > std::chrono::milliseconds(10)) {
        filtered_times.push_back(attempt_times[i]);
      }
    }
  }

  if (filtered_times.size() >= 3) {
    for (size_t i = 1; i < filtered_times.size(); ++i) {
      auto diff =
          std::chrono::duration_cast<std::chrono::milliseconds>(filtered_times[i] - filtered_times[i - 1]).count();
      EXPECT_LT(diff, 500) << "Interval too long, policy might not be active";
    }
  }

  // Prevent callback accessing destroyed attempt_times
  client_->on_state(nullptr);
  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientPolicyTest, ExponentialBackoffPolicyIncreasesDelay) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.connection_timeout_ms = 20;

  client_ = TcpClient::create(cfg, ioc);

  client_->set_reconnect_policy(ExponentialBackoff(50ms, 1000ms, 2.0, false));

  std::vector<std::chrono::steady_clock::time_point> attempt_times;

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      attempt_times.push_back(std::chrono::steady_clock::now());
    }
  });

  client_->start();

  ioc.run_for(std::chrono::milliseconds(1000));

  // Prevent callback accessing destroyed attempt_times
  client_->on_state(nullptr);
  client_->stop();

  // Debounce attempt times to filter out rapid-fire Connecting states (e.g. from handle_close -> schedule_retry)
  std::vector<std::chrono::steady_clock::time_point> filtered_times;
  if (!attempt_times.empty()) {
    filtered_times.push_back(attempt_times[0]);
    for (size_t i = 1; i < attempt_times.size(); ++i) {
      if (attempt_times[i] - filtered_times.back() > std::chrono::milliseconds(25)) {
        filtered_times.push_back(attempt_times[i]);
      }
    }
  }

  EXPECT_GE(filtered_times.size(), 3);

  if (filtered_times.size() >= 3) {
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(filtered_times[1] - filtered_times[0]).count();
    auto d2 = std::chrono::duration_cast<std::chrono::milliseconds>(filtered_times[2] - filtered_times[1]).count();

    EXPECT_GT(d2, d1);
  }
  client_.reset();
}

TEST_F(TransportTcpClientPolicyTest, PolicyCanStopRetries) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.connection_timeout_ms = 20;
  cfg.max_retries = -1;  // Infinite in config

  client_ = TcpClient::create(cfg, ioc);

  client_->set_reconnect_policy([](const diagnostics::ErrorInfo&, uint32_t attempt) -> ReconnectDecision {
    if (attempt >= 2) return {false, 0ms};
    return {true, 10ms};
  });

  std::atomic<int> connecting_count{0};
  std::atomic<bool> error_state{false};

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_count.fetch_add(1);
    if (state == base::LinkState::Error) error_state = true;
  });

  client_->start();

  ioc.run_for(std::chrono::milliseconds(500));

  // Initial(1) + Retry1(1) + Retry2(1) = 3 minimum.
  // Windows might add extra Connecting states due to timeouts.
  EXPECT_GE(connecting_count.load(), 3);
  EXPECT_TRUE(error_state.load());

  client_->on_state(nullptr);
  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientPolicyTest, ResetAttemptCountOnSuccess) {
  boost::asio::io_context ioc;
  // Start acceptor
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  auto port = acceptor.local_endpoint().port();

  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connection_timeout_ms = 100;

  client_ = TcpClient::create(cfg, ioc);

  // Policy: only allow attempt 0 (1 retry)
  client_->set_reconnect_policy([](const diagnostics::ErrorInfo&, uint32_t attempt) -> ReconnectDecision {
    if (attempt >= 1) return {false, 0ms};
    return {true, 10ms};
  });

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_count.fetch_add(1);
  });

  std::shared_ptr<tcp::socket> peer_socket;
  acceptor.async_accept([&](auto ec, tcp::socket socket) {
    if (!ec) {
      peer_socket = std::make_shared<tcp::socket>(std::move(socket));
    }
  });

  client_->start();

  // Connect successfully
  ioc.run_for(100ms);
  EXPECT_TRUE(client_->is_connected());

  // Close server
  if (peer_socket) peer_socket->close();
  acceptor.close();  // Ensure it cannot reconnect

  // Allow retry logic to run
  ioc.run_for(500ms);

  // Initial(1) + Disconnect->Connecting(1) + ScheduleRetry->Connecting(1) = 3 (Linux/macOS)
  // Windows might trigger additional transitions due to timeout vs immediate reset -> 4
  EXPECT_GE(connecting_count.load(), 3);

  client_->on_state(nullptr);
  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientPolicyTest, MaxRetriesEnforcedOverPolicy) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = TestUtils::getAvailableTestPort();
  cfg.connection_timeout_ms = 20;
  cfg.max_retries = 2;  // Strict limit

  client_ = TcpClient::create(cfg, ioc);

  // Policy that wants to retry forever
  client_->set_reconnect_policy(
      [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision { return {true, 10ms}; });

  std::atomic<int> connecting_count{0};
  std::atomic<bool> error_state{false};

  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_count.fetch_add(1);
    if (state == base::LinkState::Error) error_state = true;
  });

  client_->start();

  ioc.run_for(std::chrono::milliseconds(500));

  // Initial(1) + Retry1(1) + Retry2(1) = 3 attempts total.
  // max_retries=2 means 2 retries allowed.
  // Attempt 0 (start) -> fail.
  // Retry 1 (count=0) -> fail.
  // Retry 2 (count=1) -> fail.
  // Retry 3 (count=2) -> Max Retries Check: count(2) >= max_retries(2). STOP.

  // We expect connecting state to appear around 3 times.
  // Due to potential duplicates on some platforms, we check strictly it is not infinite.
  // With 10ms delay, infinite would mean > 30-40.

  EXPECT_LT(connecting_count.load(), 10);
  EXPECT_GE(connecting_count.load(), 3);
  EXPECT_TRUE(error_state.load());

  client_->on_state(nullptr);
  client_->stop();
  client_.reset();
}

TEST_F(TransportTcpClientPolicyTest, NonRetryableErrorPreventsRetry) {
  boost::asio::io_context ioc;
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 0;  // Will be set after acceptor creation
  cfg.connection_timeout_ms = 100;
  cfg.backpressure_threshold = 10;  // Low threshold

  // Connect to a real server so we can write
  tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), 0));
  cfg.port = acceptor.local_endpoint().port();

  client_ = TcpClient::create(cfg, ioc);

  // Policy always retries
  client_->set_reconnect_policy(
      [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision { return {true, 10ms}; });

  std::atomic<bool> connected{false};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) connected = true;
  });

  client_->start();

  // Accept connection
  std::shared_ptr<tcp::socket> peer;
  acceptor.async_accept([&](auto ec, tcp::socket s) {
    if (!ec) peer = std::make_shared<tcp::socket>(std::move(s));
  });

  ioc.run_for(100ms);
  ASSERT_TRUE(connected.load());

  // Write large data to trigger backpressure (not a fatal error with new drop behavior)
  // Overflow (> bp_limit 512KB) results in message drop + backpressure, not disconnection
  std::vector<uint8_t> huge_payload(1024 * 1024, 0xFF);  // 1MB exceeds 512KB limit

  std::atomic<bool> backpressure_seen{false};
  client_->on_backpressure([&](size_t) { backpressure_seen = true; });

  client_->async_write_copy(huge_payload);

  std::atomic<int> connecting_count{0};
  client_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) connecting_count.fetch_add(1);
  });

  ioc.run_for(200ms);

  // With drop-not-close behavior: message is dropped, backpressure fires, connection stays alive
  EXPECT_EQ(connecting_count.load(), 0);
  EXPECT_TRUE(client_->is_connected());
  EXPECT_TRUE(backpressure_seen.load());

  client_->on_state(nullptr);
  client_->stop();
  client_.reset();
}
