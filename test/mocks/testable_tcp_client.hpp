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

#include <memory>

#include "dependency_injection.hpp"
#include "mock_tcp_socket.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"

namespace wirestead {
namespace test {
namespace mocks {

/**
 * @brief Testable TCP client that can use mock objects
 *
 * This class extends the regular TCP client to support dependency injection
 * of mock objects for testing purposes.
 */
class TestableTcpClient : public wrapper::TcpClient {
 public:
  /**
   * @brief Constructor with dependency injection support
   */
  TestableTcpClient(const std::string& host, uint16_t port) : wrapper::TcpClient(host, port) {
    // Check if we're in testing mode
    auto& injector = DependencyInjector::instance();
    if (injector.isTestingMode()) {
      // Create mock socket for testing
      mock_socket_ = injector.createSocket("default");
      setupMockCallbacks();
    }
  }

  /**
   * @brief Get the mock socket for test verification
   */
  MockTcpSocket* getMockSocket() const { return mock_socket_.get(); }

  /**
   * @brief Override connection method to use mock
   */
  void start() override {
    if (mock_socket_) {
      // Simulate connection using mock
      simulateConnection();
    } else {
      // Use real implementation
      wrapper::TcpClient::start();
    }
  }

  /**
   * @brief Override send method to use mock
   */
  void send(const std::string& data) override {
    if (mock_socket_) {
      // Simulate data transmission using mock
      simulateDataTransmission(data);
    } else {
      // Use real implementation
      wrapper::TcpClient::send(data);
    }
  }

 private:
  std::unique_ptr<MockTcpSocket> mock_socket_;

  void setupMockCallbacks() {
    if (!mock_socket_) return;

    // Setup mock callbacks to trigger real callbacks
    // This is a simplified version - in a real implementation,
    // you would need to properly integrate with the underlying
    // boost::asio socket implementation
  }

  void simulateConnection() {
    if (!mock_socket_) return;

    // Simulate async_connect call
    // In a real implementation, this would trigger the actual
    // boost::asio::async_connect with the mock socket
  }

  void simulateDataTransmission(const std::string& data) {
    if (!mock_socket_) return;

    // Simulate async_write call
    // In a real implementation, this would trigger the actual
    // boost::asio::async_write with the mock socket
  }
};

/**
 * @brief Testable TCP server that can use mock objects
 */
class TestableTcpServer : public wrapper::TcpServer {
 public:
  /**
   * @brief Constructor with dependency injection support
   */
  TestableTcpServer(uint16_t port) : wrapper::TcpServer(port) {
    // Check if we're in testing mode
    auto& injector = DependencyInjector::instance();
    if (injector.isTestingMode()) {
      // Create mock acceptor for testing
      mock_acceptor_ = injector.createAcceptor("default");
      setupMockCallbacks();
    }
  }

  /**
   * @brief Get the mock acceptor for test verification
   */
  MockTcpAcceptor* getMockAcceptor() const { return mock_acceptor_.get(); }

  /**
   * @brief Override start method to use mock
   */
  void start() override {
    if (mock_acceptor_) {
      // Simulate server startup using mock
      simulateServerStart();
    } else {
      // Use real implementation
      wrapper::TcpServer::start();
    }
  }

 private:
  std::unique_ptr<MockTcpAcceptor> mock_acceptor_;

  void setupMockCallbacks() {
    if (!mock_acceptor_) return;

    // Setup mock callbacks to trigger real callbacks
  }

  void simulateServerStart() {
    if (!mock_acceptor_) return;

    // Simulate server startup
    // In a real implementation, this would trigger the actual
    // boost::asio acceptor operations with the mock acceptor
  }
};

}  // namespace mocks
}  // namespace test
}  // namespace wirestead
