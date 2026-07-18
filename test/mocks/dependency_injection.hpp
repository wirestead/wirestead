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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "mock_tcp_socket.hpp"
#include "mock_test_helpers.hpp"

namespace wirestead {
namespace test {
namespace mocks {

/**
 * @brief Dependency injection system for mock objects
 *
 * This system allows injecting mock objects into the wirestead library
 * for testing purposes, enabling network-independent testing.
 */
class DependencyInjector {
 public:
  using SocketFactory = std::function<std::unique_ptr<MockTcpSocket>()>;
  using AcceptorFactory = std::function<std::unique_ptr<MockTcpAcceptor>()>;
  using SerialFactory = std::function<std::unique_ptr<MockSerialPort>()>;

  /**
   * @brief Singleton instance access
   */
  static DependencyInjector& instance() {
    static DependencyInjector instance;
    return instance;
  }

  /**
   * @brief Register mock socket factory
   */
  void registerSocketFactory(const std::string& key, SocketFactory factory) { socket_factories_[key] = factory; }

  /**
   * @brief Register mock acceptor factory
   */
  void registerAcceptorFactory(const std::string& key, AcceptorFactory factory) { acceptor_factories_[key] = factory; }

  /**
   * @brief Register mock serial factory
   */
  void registerSerialFactory(const std::string& key, SerialFactory factory) { serial_factories_[key] = factory; }

  /**
   * @brief Create mock socket
   */
  std::unique_ptr<MockTcpSocket> createSocket(const std::string& key = "default") {
    auto it = socket_factories_.find(key);
    if (it != socket_factories_.end()) {
      return it->second();
    }
    return std::make_unique<MockTcpSocket>();
  }

  /**
   * @brief Create mock acceptor
   */
  std::unique_ptr<MockTcpAcceptor> createAcceptor(const std::string& key = "default") {
    auto it = acceptor_factories_.find(key);
    if (it != acceptor_factories_.end()) {
      return it->second();
    }
    return std::make_unique<MockTcpAcceptor>();
  }

  /**
   * @brief Create mock serial port
   */
  std::unique_ptr<MockSerialPort> createSerial(const std::string& key = "default") {
    auto it = serial_factories_.find(key);
    if (it != serial_factories_.end()) {
      return it->second();
    }
    return std::make_unique<MockSerialPort>();
  }

  /**
   * @brief Check if testing mode is enabled
   */
  bool isTestingMode() const { return testing_mode_; }

  /**
   * @brief Enable/disable testing mode
   */
  void setTestingMode(bool enabled) { testing_mode_ = enabled; }

  /**
   * @brief Clear all registered factories
   */
  void clear() {
    socket_factories_.clear();
    acceptor_factories_.clear();
    serial_factories_.clear();
    testing_mode_ = false;
  }

 private:
  DependencyInjector() = default;

  std::unordered_map<std::string, SocketFactory> socket_factories_;
  std::unordered_map<std::string, AcceptorFactory> acceptor_factories_;
  std::unordered_map<std::string, SerialFactory> serial_factories_;
  bool testing_mode_ = false;
};

/**
 * @brief RAII helper for dependency injection
 */
class MockTestScope {
 public:
  MockTestScope() { DependencyInjector::instance().setTestingMode(true); }

  ~MockTestScope() { DependencyInjector::instance().clear(); }

  // Disable copy and move
  MockTestScope(const MockTestScope&) = delete;
  MockTestScope& operator=(const MockTestScope&) = delete;
  MockTestScope(MockTestScope&&) = delete;
  MockTestScope& operator=(MockTestScope&&) = delete;
};

/**
 * @brief Mock scenario builder for easy test setup
 */
class MockScenarioBuilder {
 public:
  MockScenarioBuilder()
      : failure_result_(MockTestScenario::ConnectionResult::Success),
        transfer_result_(MockTestScenario::DataTransferResult::Success) {}

  /**
   * @brief Build successful connection scenario
   */
  MockScenarioBuilder& withSuccessfulConnection() {
    scenario_type_ = "successful_connection";
    return *this;
  }

  /**
   * @brief Build connection failure scenario
   */
  MockScenarioBuilder& withConnectionFailure(MockTestScenario::ConnectionResult result) {
    scenario_type_ = "connection_failure";
    failure_result_ = result;
    return *this;
  }

  /**
   * @brief Build data reception scenario
   */
  MockScenarioBuilder& withDataReception(const std::string& test_data) {
    scenario_type_ = "data_reception";
    test_data_ = test_data;
    return *this;
  }

  /**
   * @brief Build data transmission scenario
   */
  MockScenarioBuilder& withDataTransmission(MockTestScenario::DataTransferResult result) {
    scenario_type_ = "data_transmission";
    transfer_result_ = result;
    return *this;
  }

  /**
   * @brief Apply the scenario to dependency injector
   */
  void apply() {
    auto& injector = DependencyInjector::instance();

    if (scenario_type_ == "successful_connection") {
      injector.registerSocketFactory("default", [this]() {
        auto socket = std::make_unique<MockTcpSocket>();
        MockTestScenario::setupSuccessfulConnection(*socket);
        return socket;
      });
    } else if (scenario_type_ == "connection_failure") {
      injector.registerSocketFactory("default", [this]() {
        auto socket = std::make_unique<MockTcpSocket>();
        MockTestScenario::setupConnectionFailure(*socket, failure_result_);
        return socket;
      });
    } else if (scenario_type_ == "data_reception") {
      injector.registerSocketFactory("default", [this]() {
        auto socket = std::make_unique<MockTcpSocket>();
        MockTestScenario::setupSuccessfulConnection(*socket);
        MockTestScenario::setupDataReception(*socket, test_data_);
        return socket;
      });
    } else if (scenario_type_ == "data_transmission") {
      injector.registerSocketFactory("default", [this]() {
        auto socket = std::make_unique<MockTcpSocket>();
        MockTestScenario::setupSuccessfulConnection(*socket);
        MockTestScenario::setupDataTransmission(*socket, transfer_result_);
        return socket;
      });
    }
  }

 private:
  std::string scenario_type_;
  MockTestScenario::ConnectionResult failure_result_;
  MockTestScenario::DataTransferResult transfer_result_;
  std::string test_data_;
};

}  // namespace mocks
}  // namespace test
}  // namespace wirestead
