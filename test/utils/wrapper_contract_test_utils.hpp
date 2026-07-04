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

#include <boost/asio/io_context.hpp>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "test/utils/test_utils.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/unilink.hpp"

namespace unilink::test::wrapper_support {

class FakeChannel : public interface::Channel {
 public:
  void start() override { connected_ = true; }
  void stop() override { connected_ = false; }
  bool is_connected() const override { return connected_; }

  boost::asio::any_io_executor get_executor() override {
    static boost::asio::io_context ioc;
    return ioc.get_executor();
  }

  bool async_write_copy(memory::ConstByteSpan) override {
    ++write_count_;
    return true;
  }
  bool async_write_move(std::vector<uint8_t>&&) override {
    ++write_count_;
    return true;
  }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override {
    ++write_count_;
    return true;
  }

  bool async_try_write_copy(memory::ConstByteSpan data) override { return async_write_copy(data); }
  bool async_try_write_move(std::vector<uint8_t>&& data) override { return async_write_move(std::move(data)); }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return async_write_shared(std::move(data));
  }

  bool is_backpressure_active() const override { return backpressure_active_; }
  void set_backpressure_active(bool active) { backpressure_active_ = active; }

  void on_bytes(OnBytes cb) override { on_bytes_ = std::move(cb); }
  void on_state(OnState cb) override { on_state_ = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { on_backpressure_ = std::move(cb); }

  void emit_bytes(std::string_view text) {
    if (!on_bytes_) return;
    on_bytes_(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
  }

  void emit_state(base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected_ = true;
    } else if (state == base::LinkState::Closed || state == base::LinkState::Error || state == base::LinkState::Idle) {
      connected_ = false;
    }

    if (on_state_) {
      on_state_(state);
    }
  }

  int write_count() const { return write_count_; }

 private:
  bool connected_{false};
  bool backpressure_active_{false};
  int write_count_{0};
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_backpressure_;
};

class TcpServerLoopbackHarness {
 public:
  TcpServerLoopbackHarness() : port_(TestUtils::getAvailableTestPort()) {}

  ~TcpServerLoopbackHarness() { stop_all(); }

  std::shared_ptr<wrapper::TcpServer> start_server() {
    server_ = std::make_shared<wrapper::TcpServer>(port_);
    auto started = server_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start TCP test server");
    }
    if (!TestUtils::waitForCondition([&]() { return server_->listening(); }, 5000)) {
      throw std::runtime_error("TCP test server did not reach listening state");
    }
    return server_;
  }

  std::shared_ptr<wrapper::TcpClient> connect_client() {
    client_ = std::make_shared<wrapper::TcpClient>("127.0.0.1", port_);
    auto started = client_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start TCP test client");
    }
    if (!TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000)) {
      throw std::runtime_error("TCP test client did not connect");
    }
    return client_;
  }

  bool wait_for_client_count(size_t expected, int timeout_ms = 5000) const {
    return server_ && TestUtils::waitForCondition([&]() { return server_->client_count() >= expected; }, timeout_ms);
  }

  void stop_all() {
    if (client_) {
      client_->stop();
      client_.reset();
    }
    if (server_) {
      server_->stop();
      server_.reset();
    }
  }

 private:
  uint16_t port_;
  std::shared_ptr<wrapper::TcpServer> server_;
  std::shared_ptr<wrapper::TcpClient> client_;
};

class UdsServerLoopbackHarness {
 public:
  explicit UdsServerLoopbackHarness(std::string prefix = "wrapper-contract-uds")
      : socket_path_(TestUtils::makeUniqueUdsSocketPath(prefix).string()) {
    TestUtils::removeFileIfExists(socket_path_);
  }

  ~UdsServerLoopbackHarness() {
    stop_all();
    TestUtils::removeFileIfExists(socket_path_);
  }

  std::shared_ptr<wrapper::UdsServer> start_server() {
    server_ = std::make_shared<wrapper::UdsServer>(socket_path_);
    auto started = server_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start UDS test server");
    }
    if (!TestUtils::waitForCondition([&]() { return server_->listening(); }, 5000)) {
      throw std::runtime_error("UDS test server did not reach listening state");
    }
    return server_;
  }

  std::shared_ptr<wrapper::UdsClient> connect_client() {
    client_ = std::make_shared<wrapper::UdsClient>(socket_path_);
    auto started = client_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start UDS test client");
    }
    if (!TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000)) {
      throw std::runtime_error("UDS test client did not connect");
    }
    return client_;
  }

  bool wait_for_client_count(size_t expected, int timeout_ms = 5000) const {
    return server_ && TestUtils::waitForCondition([&]() { return server_->client_count() >= expected; }, timeout_ms);
  }

  void stop_all() {
    if (client_) {
      client_->stop();
      client_.reset();
    }
    if (server_) {
      server_->stop();
      server_.reset();
    }
  }

 private:
  std::string socket_path_;
  std::shared_ptr<wrapper::UdsServer> server_;
  std::shared_ptr<wrapper::UdsClient> client_;
};

class UdpServerLoopbackHarness {
 public:
  UdpServerLoopbackHarness() : port_(TestUtils::getAvailableTestPort()) {}

  ~UdpServerLoopbackHarness() { stop_all(); }

  std::shared_ptr<wrapper::UdpServer> start_server() {
    config::UdpConfig server_cfg;
    server_cfg.bind_address = "127.0.0.1";
    server_cfg.local_port = port_;

    server_ = std::make_shared<wrapper::UdpServer>(server_cfg);
    auto started = server_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start UDP test server");
    }
    if (!TestUtils::waitForCondition([&]() { return server_->listening(); }, 5000)) {
      throw std::runtime_error("UDP test server did not reach listening state");
    }
    return server_;
  }

  std::shared_ptr<wrapper::UdpClient> start_sender() {
    config::UdpConfig client_cfg;
    client_cfg.bind_address = "127.0.0.1";
    client_cfg.local_port = 0;
    client_cfg.remote_address = std::string("127.0.0.1");
    client_cfg.remote_port = port_;

    client_ = std::make_shared<wrapper::UdpClient>(client_cfg);
    auto started = client_->start();
    if (!started.get()) {
      throw std::runtime_error("Failed to start UDP test client");
    }
    if (!TestUtils::waitForCondition([&]() { return client_->connected(); }, 5000)) {
      throw std::runtime_error("UDP test client did not reach connected state");
    }
    return client_;
  }

  void send_from_client(std::string_view data) {
    if (!client_) {
      throw std::runtime_error("UDP test client is not started");
    }
    client_->send(data);
  }

  bool wait_for_client_count(size_t expected, int timeout_ms = 5000) const {
    return server_ && TestUtils::waitForCondition([&]() { return server_->client_count() >= expected; }, timeout_ms);
  }

  void stop_all() {
    if (client_) {
      client_->stop();
      client_.reset();
    }
    if (server_) {
      server_->stop();
      server_.reset();
    }
  }

 private:
  uint16_t port_;
  std::shared_ptr<wrapper::UdpServer> server_;
  std::shared_ptr<wrapper::UdpClient> client_;
};

}  // namespace unilink::test::wrapper_support
