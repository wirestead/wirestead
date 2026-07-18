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

#pragma once

#include <gmock/gmock.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "mock_tcp_socket.hpp"
#include "wirestead/base/platform.hpp"

namespace wirestead {
namespace test {
namespace mocks {

/**
 * @brief Helper class for managing mock test scenarios
 */
class MockTestScenario {
 public:
  enum class ConnectionResult { Success, ConnectionRefused, Timeout, NetworkUnreachable, PermissionDenied };

  enum class DataTransferResult { Success, PartialTransfer, ConnectionLost, BufferOverflow };

  // Connection scenario builders
  static void setupSuccessfulConnection(MockTcpSocket& mock_socket) {
    EXPECT_CALL(mock_socket, async_connect(::testing::_, ::testing::_))
        .WillOnce(::testing::Invoke([](const boost::asio::ip::tcp::endpoint&, auto callback) {
          // Simulate successful connection
          boost::system::error_code ec;
          callback(ec);
        }));

    EXPECT_CALL(mock_socket, is_open()).WillRepeatedly(::testing::Return(true));
  }

  static void setupConnectionFailure(MockTcpSocket& mock_socket, ConnectionResult result) {
    EXPECT_CALL(mock_socket, async_connect(::testing::_, ::testing::_))
        .WillOnce(::testing::Invoke([result](const boost::asio::ip::tcp::endpoint&, auto callback) {
          boost::system::error_code ec;
          switch (result) {
            case ConnectionResult::ConnectionRefused:
              ec = boost::asio::error::connection_refused;
              break;
            case ConnectionResult::Timeout:
              ec = boost::asio::error::timed_out;
              break;
            case ConnectionResult::NetworkUnreachable:
              ec = boost::asio::error::network_unreachable;
              break;
            case ConnectionResult::PermissionDenied:
              ec = boost::system::error_code(boost::system::errc::permission_denied, boost::system::generic_category());
              break;
            default:
              ec = boost::asio::error::connection_refused;
              break;
          }
          callback(ec);
        }));

    EXPECT_CALL(mock_socket, is_open()).WillRepeatedly(::testing::Return(false));
  }

  static void setupDataReception(MockTcpSocket& mock_socket, const std::string& test_data) {
    EXPECT_CALL(mock_socket, async_read_some(::testing::_, ::testing::_))
        .WillOnce(::testing::Invoke([test_data](const boost::asio::mutable_buffer& buffer, auto callback) {
          // Copy test data to buffer
          size_t copy_size = std::min(test_data.size(), buffer.size());
          std::memcpy(buffer.data(), test_data.c_str(), copy_size);

          boost::system::error_code ec;
          callback(ec, copy_size);
        }));
  }

  static void setupDataTransmission(MockTcpSocket& mock_socket, DataTransferResult result) {
    EXPECT_CALL(mock_socket, async_write(::testing::_, ::testing::_))
        .WillOnce(::testing::Invoke([result](const boost::asio::const_buffer&, auto callback) {
          boost::system::error_code ec;
          size_t bytes_transferred = 0;

          switch (result) {
            case DataTransferResult::Success:
              // Simulate successful write
              bytes_transferred = 1024;  // Assume some data was written
              break;
            case DataTransferResult::PartialTransfer:
              ec = boost::asio::error::try_again;
              bytes_transferred = 512;  // Partial write
              break;
            case DataTransferResult::ConnectionLost:
              ec = boost::asio::error::connection_aborted;
              break;
            case DataTransferResult::BufferOverflow:
              ec = boost::asio::error::no_buffer_space;
              break;
          }

          callback(ec, bytes_transferred);
        }));
  }
};

/**
 * @brief Thread-safe state tracker for mock tests
 */
class MockStateTracker {
 public:
  enum class State { Idle, Connecting, Connected, Disconnected, Error, DataReceived, DataSent };

  MockStateTracker() : current_state_(State::Idle) {}

  void setState(State state) {
    std::lock_guard<std::mutex> lock(mutex_);
    state_history_.push_back(state);
    current_state_ = state;
    cv_.notify_all();
  }

  State getCurrentState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_state_;
  }

  std::vector<State> getStateHistory() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_history_;
  }

  bool waitForState(State expected_state, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this, expected_state]() { return current_state_ == expected_state; });
  }

  bool waitForStateCount(State state, int count, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this, state, count]() {
      return std::count(state_history_.begin(), state_history_.end(), state) >= count;
    });
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    state_history_.clear();
    current_state_ = State::Idle;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  State current_state_;
  std::vector<State> state_history_;
};

/**
 * @brief Mock test data generator
 */
class MockTestDataGenerator {
 public:
  static std::string generateTestMessage(size_t size = 1024) {
    std::string data;
    data.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      data += static_cast<char>('A' + (i % 26));
    }
    return data;
  }

  static std::vector<uint8_t> generateBinaryData(size_t size = 1024) {
    std::vector<uint8_t> data;
    data.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      data.push_back(static_cast<uint8_t>(i % 256));
    }
    return data;
  }

  static std::string generateJsonMessage(const std::string& type = "test", const std::string& content = "hello") {
    return "{\"type\":\"" + type + "\",\"content\":\"" + content + "\",\"timestamp\":" +
           std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count()) +
           "}";
  }
};

}  // namespace mocks
}  // namespace test
}  // namespace wirestead
