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
#include <boost/asio.hpp>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace wirestead::test;
using namespace std::chrono_literals;

class TcpServerChaosTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif
    test_port_ = TestUtils::getAvailableTestPort();
  }
  uint16_t test_port_;
};

TEST_F(TcpServerChaosTest, GhostClient) {
  std::atomic<int> connect_count{0};
  auto server = tcp_server(test_port_)
                    .on_connect([&](const wrapper::ConnectionContext&) { connect_count++; })
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_TRUE(server->start().get());

  {
    boost::asio::io_context ioc;
    boost::asio::ip::tcp::socket socket(ioc);
    boost::system::error_code ec;
    socket.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), test_port_), ec);
    if (!ec) {
      std::this_thread::sleep_for(100ms);
    }
  }

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return connect_count.load() > 0; }, 5000));
  server->stop();
}

TEST_F(TcpServerChaosTest, SlowLoris) {
  std::atomic<bool> done{false};
  std::string received_data;
  auto server = tcp_server(test_port_)
                    .on_data([&](const wrapper::MessageContext& ctx) {
                      received_data += ctx.data();
                      if (received_data.find("Hello World") != std::string::npos) done = true;
                    })
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_TRUE(server->start().get());

  std::thread slow_sender([&]() {
    try {
      boost::asio::io_context ioc;
      boost::asio::ip::tcp::socket socket(ioc);
      socket.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), test_port_));

      const std::string msg = "Hello World";
      for (char c : msg) {
        boost::asio::write(socket, boost::asio::buffer(&c, 1));
        std::this_thread::sleep_for(50ms);
      }
    } catch (...) {
    }
  });

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return done.load(); }, 15000));
  if (slow_sender.joinable()) slow_sender.join();
  server->stop();
}

TEST_F(TcpServerChaosTest, GarbageSender) {
  std::atomic<size_t> total_bytes{0};
  auto server = tcp_server(test_port_)
                    .on_data([&](const wrapper::MessageContext& ctx) { total_bytes += ctx.data().size(); })
                    .on_error([](auto&&) {})
                    .build();

  ASSERT_TRUE(server->start().get());

  std::thread garbage_thread([&]() {
    try {
      boost::asio::io_context ioc;
      boost::asio::ip::tcp::socket socket(ioc);
      socket.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), test_port_));

      std::vector<uint8_t> garbage(1024 * 4, 0xff);
      for (int i = 0; i < 4; ++i) {  // Total 16KB
        boost::asio::write(socket, boost::asio::buffer(garbage));
      }
    } catch (...) {
    }
  });

  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return total_bytes.load() >= 1024 * 16; }, 15000));
  if (garbage_thread.joinable()) garbage_thread.join();
  server->stop();
}

TEST_F(TcpServerChaosTest, MaxConnections) {
  auto server = tcp_server(test_port_).max_clients(2).on_data([](auto&&) {}).on_error([](auto&&) {}).build();
  ASSERT_TRUE(server->start().get());

  auto connect_one = [&]() {
    try {
      auto ioc = std::make_shared<boost::asio::io_context>();
      auto socket = std::make_shared<boost::asio::ip::tcp::socket>(*ioc);
      socket->connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), test_port_));
      return std::make_pair(ioc, socket);
    } catch (...) {
      return std::make_pair(std::shared_ptr<boost::asio::io_context>(),
                            std::shared_ptr<boost::asio::ip::tcp::socket>());
    }
  };

  auto c1 = connect_one();
  auto c2 = connect_one();

  if (c1.second) {
    EXPECT_TRUE(c1.second->is_open());
  }
  if (c2.second) {
    EXPECT_TRUE(c2.second->is_open());
  }

  server->stop();
}
