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
#include <thread>

#include "test/utils/contract_utils.hpp"
#include "test_utils.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;

class ContractComplianceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Independent io_context for testing
    ioc_ = std::make_shared<boost::asio::io_context>();
    runner_ = std::make_unique<IoContextRunner>(*ioc_);
  }

  void TearDown() override {
    if (client_) {
      client_->stop();

      client_.reset();  // Destroy client BEFORE io_context
    }

    runner_.reset();  // Stop thread

    ioc_.reset();
  }

  std::shared_ptr<boost::asio::io_context> ioc_;
  std::unique_ptr<IoContextRunner> runner_;
  std::shared_ptr<TcpClient> client_;
  CallbackRecorder recorder_;
};

/**
 * @brief Verify that NO callbacks are invoked after stop() returns.
 *
 * This is the "Stop Semantics" contract.
 */
TEST_F(ContractComplianceTest, TcpClient_StopSemantics) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 12345;            // Non-existent port to force retries
  cfg.retry_interval_ms = 10;  // Fast retry

  client_ = TcpClient::create(cfg, *ioc_);

  client_->on_state(recorder_.get_state_callback());
  client_->on_bytes(recorder_.get_bytes_callback());
  client_->on_backpressure(recorder_.get_backpressure_callback());

  client_->start();

  // Wait for some activity (e.g., Connecting state)
  ASSERT_TRUE(recorder_.wait_for_state(base::LinkState::Connecting, std::chrono::milliseconds(100)));

  // Let it run for a bit to generate some internal async operations (retries)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // --- STOP ---
  client_->stop();
  auto stop_time = std::chrono::steady_clock::now();

  // Wait a bit to see if any trailing callbacks occur
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify
  EXPECT_TRUE(recorder_.verify_no_events_after(stop_time))
      << "Found events after stop()! This violates the Channel Contract.";

  // Print violations if any
  auto events = recorder_.get_events();
  for (const auto& ev : events) {
    if (ev.timestamp > stop_time) {
      std::string type_str;
      if (ev.type == EventType::StateChange)
        type_str = "StateChange";
      else if (ev.type == EventType::DataReceived)
        type_str = "DataReceived";
      else
        type_str = "Backpressure";

      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(ev.timestamp - stop_time).count();
      std::cout << "[Violation] Event " << type_str << " occurred " << diff << "us AFTER stop()" << std::endl;
    }
  }
}

#include "wirestead/config/serial_config.hpp"
#include "wirestead/transport/serial/serial.hpp"

TEST_F(ContractComplianceTest, Serial_StopSemantics) {
  config::SerialConfig cfg;
  cfg.device = "/dev/nonexistent_device_for_test";
  cfg.retry_interval_ms = 10;

  auto serial = Serial::create(cfg, *ioc_);
  CallbackRecorder recorder;

  serial->on_state(recorder.get_state_callback());
  serial->on_bytes(recorder.get_bytes_callback());
  serial->on_backpressure(recorder.get_backpressure_callback());

  serial->start();

  // Wait for retry cycle
  ASSERT_TRUE(recorder.wait_for_state(base::LinkState::Connecting, std::chrono::milliseconds(100)));
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // --- STOP ---
  serial->stop();
  auto stop_time = std::chrono::steady_clock::now();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  EXPECT_TRUE(recorder.verify_no_events_after(stop_time))
      << "Serial: Found events after stop()! This violates the Channel Contract.";
}

#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"

TEST_F(ContractComplianceTest, TcpServer_StopSemantics) {
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  auto server = TcpServer::create(cfg);

  CallbackRecorder recorder;
  server->on_state(recorder.get_state_callback());

  server->start();

  // Wait for Listening
  ASSERT_TRUE(recorder.wait_for_state(base::LinkState::Listening, std::chrono::milliseconds(500)));

  // --- STOP ---
  server->stop();
  auto stop_time = std::chrono::steady_clock::now();

  // Give a small grace period for the final 'Closed' state event which might be
  // triggered during the stop() call itself or immediately after.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // The contract says NO callbacks after stop() RETURNS.
  // We use a small epsilon if needed, but verify_no_events_after should handle it.
  EXPECT_TRUE(recorder.verify_no_events_after(stop_time))
      << "TcpServer: Found events after stop()! This violates the Channel Contract.";
}

TEST_F(ContractComplianceTest, TcpClient_Backpressure_Contract) {
  config::TcpClientConfig cfg;

  cfg.host = "127.0.0.1";

  cfg.port = 0;  // No connection

  cfg.backpressure_threshold = 1024;  // 1KB

  client_ = TcpClient::create(cfg, *ioc_);

  // Use a local recorder to avoid shared state issues if any

  CallbackRecorder bp_recorder;

  client_->on_backpressure(bp_recorder.get_backpressure_callback());

  client_->start();

  // Queue enough data to trigger backpressure

  std::vector<uint8_t> data(2048, 'A');  // 2KB > 1KB

  client_->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));

  // Wait for backpressure event

  ASSERT_TRUE(bp_recorder.wait_for_data(
      0, std::chrono::milliseconds(100)));  // wait_for_data is not quite right, check events size

  // Custom wait loop for events

  auto start = std::chrono::steady_clock::now();

  bool triggered = false;

  while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(200)) {
    if (!bp_recorder.get_events().empty()) {
      triggered = true;

      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(triggered) << "Backpressure callback was not triggered";

  auto events = bp_recorder.get_events();

  ASSERT_EQ(events.size(), 1);

  EXPECT_EQ(events[0].type, EventType::Backpressure);

  EXPECT_GE(std::get<size_t>(events[0].data), 1024);

  // Stop should NOT trigger relief callback (queue=0)

  client_->stop();

  auto stop_time = std::chrono::steady_clock::now();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(bp_recorder.verify_no_events_after(stop_time))

      << "TcpClient: Backpressure relief callback triggered after stop! Contract violation.";
}

TEST_F(ContractComplianceTest, Serial_Backpressure_Contract) {
  config::SerialConfig cfg;

  cfg.device = "/dev/nonexistent";

  cfg.retry_interval_ms = 1000;

  cfg.backpressure_threshold = 1024;

  auto serial = Serial::create(cfg, *ioc_);

  CallbackRecorder bp_recorder;

  serial->on_backpressure(bp_recorder.get_backpressure_callback());

  serial->start();

  std::vector<uint8_t> data(2048, 'B');

  serial->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));

  // Wait for backpressure event

  auto start = std::chrono::steady_clock::now();

  bool triggered = false;

  while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(200)) {
    if (!bp_recorder.get_events().empty()) {
      triggered = true;

      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(triggered) << "Serial: Backpressure callback was not triggered";

  // Stop should NOT trigger relief callback

  serial->stop();

  auto stop_time = std::chrono::steady_clock::now();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(bp_recorder.verify_no_events_after(stop_time))

      << "Serial: Backpressure relief callback triggered after stop! Contract violation.";
}
