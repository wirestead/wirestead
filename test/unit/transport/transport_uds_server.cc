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

#include <array>
#include <boost/asio.hpp>
#include <memory>
#include <thread>
#include <vector>

#include "tcp_stop_with_context.hpp"
#include "test/mocks/mock_uds_acceptor.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test::mocks;
using namespace wirestead::test;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class TransportUdsServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto temp_path = TestUtils::makeUniqueUdsSocketPath("uls");
    cfg.socket_path = temp_path.string();
    TestUtils::removeFileIfExists(temp_path);

    mock_acceptor = new MockUdsAcceptor();
    auto acceptor_ptr = std::unique_ptr<interface::UdsAcceptorInterface>(mock_acceptor);
    server = UdsServer::create(cfg, std::move(acceptor_ptr), ioc);
  }

  void TearDown() override {
    wirestead::test::stop_with_context(server, ioc);
    TestUtils::removeFileIfExists(cfg.socket_path);
    TestUtils::waitFor(constants::kShortTimeout.count());
  }

  boost::asio::io_context ioc;
  config::UdsServerConfig cfg;
  MockUdsAcceptor* mock_acceptor;
  std::shared_ptr<UdsServer> server;
};

TEST_F(TransportUdsServerTest, SuccessfulStart) {
  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, listen(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, async_accept(_))
      .WillOnce(
          Invoke([this](std::function<void(const boost::system::error_code&, net::local::stream_protocol::socket)>) {
            // Just keep the handler, don't call it yet to simulate listening
          }));

  std::atomic<bool> listening{false};
  server->on_state([&listening](base::LinkState state) {
    if (state == base::LinkState::Listening) {
      listening = true;
    }
  });

  server->start();

  // Give it time to transition
  for (int i = 0; i < 10; ++i) {
    ioc.poll();
    if (server->is_connected()) break;  // is_connected() checks for Listening state in my implementation
    TestUtils::waitFor(10);
  }

  EXPECT_TRUE(server->is_connected());
  EXPECT_TRUE(listening);
}

TEST_F(TransportUdsServerTest, WriteWithoutActiveSessionReturnsFalse) {
  std::vector<uint8_t> payload = {1, 2, 3};
  auto shared_payload = std::make_shared<const std::vector<uint8_t>>(payload);

  EXPECT_FALSE(server->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_FALSE(server->async_write_move(std::vector<uint8_t>{1, 2, 3}));
  EXPECT_FALSE(server->async_write_shared(shared_payload));
  EXPECT_FALSE(server->broadcast("payload"));
  EXPECT_FALSE(server->broadcast(memory::ConstByteSpan(payload.data(), payload.size())));
}

TEST_F(TransportUdsServerTest, BindFailure) {
  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _))
      .WillOnce(Invoke([](const net::local::stream_protocol::endpoint&, boost::system::error_code& ec) {
        ec = make_error_code(boost::asio::error::address_in_use);
      }));

  std::atomic<bool> has_error{false};
  server->on_state([&has_error](base::LinkState state) {
    if (state == base::LinkState::Error) {
      has_error = true;
    }
  });

  server->start();

  for (int i = 0; i < 10; ++i) {
    ioc.poll();
    if (has_error) break;
    TestUtils::waitFor(10);
  }

  EXPECT_FALSE(server->is_connected());
  EXPECT_TRUE(has_error);
  // #445: last_error_info() should now report detail for this transport too.
  ASSERT_TRUE(server->last_error_info().has_value());
  EXPECT_EQ(server->last_error_info()->component, "uds_server");
}

// Regression test for jwsung91/wirestead#453: a transient accept() failure
// (e.g. EMFILE) used to be logged and silently swallowed, permanently
// stopping the server from ever accepting again. It must now surface an
// Error state transition and keep retrying do_accept().
TEST_F(TransportUdsServerTest, AcceptFailureRetriesAndKeepsAccepting) {
  EXPECT_CALL(*mock_acceptor, open(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, bind(_, _)).WillOnce(Return());
  EXPECT_CALL(*mock_acceptor, listen(_, _)).WillOnce(Return());

  bool retried = false;
  EXPECT_CALL(*mock_acceptor, async_accept(_))
      .WillOnce(Invoke(
          [this](std::function<void(const boost::system::error_code&, net::local::stream_protocol::socket)> handler) {
            handler(make_error_code(boost::asio::error::no_descriptors), net::local::stream_protocol::socket(ioc));
          }))
      .WillOnce(Invoke(
          [&retried](std::function<void(const boost::system::error_code&, net::local::stream_protocol::socket)>) {
            // Second call proves do_accept() was invoked again after the failure
            // instead of the server silently giving up forever.
            retried = true;
          }));

  std::atomic<bool> has_error{false};
  server->on_state([&has_error](base::LinkState state) {
    if (state == base::LinkState::Error) {
      has_error = true;
    }
  });

  server->start();

  for (int i = 0; i < 50 && !(has_error && retried); ++i) {
    ioc.poll();
    TestUtils::waitFor(10);
  }

  EXPECT_TRUE(has_error);
  EXPECT_TRUE(retried);
}

class UdsMoveOwnershipTest : public ::testing::TestWithParam<bool> {};
TEST_P(UdsMoveOwnershipTest, RejectionWithoutTargetsPreservesSource) {
  config::UdsServerConfig cfg;
  cfg.socket_path = TestUtils::makeUniqueUdsSocketPath("move-reject").string();
  boost::asio::io_context io;
  auto native = UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
  auto send = [&](std::vector<uint8_t>& bytes) {
    return GetParam() ? native->async_try_write_move(std::move(bytes)) : native->async_write_move(std::move(bytes));
  };
  std::vector<uint8_t> payload{1, 2, 3};
  const auto original = payload;
  EXPECT_FALSE(send(payload));
  EXPECT_EQ(payload, original);
  test::stop_with_context(native, io);
  payload = original;
  EXPECT_FALSE(send(payload));
  EXPECT_EQ(payload, original);
}

#ifndef _WIN32
TEST_P(UdsMoveOwnershipTest, AllRejectedPreservesAndPartialAcceptanceConsumesSource) {
  using namespace std::chrono_literals;
  boost::asio::io_context io;
  config::UdsServerConfig cfg;
  cfg.socket_path = TestUtils::makeUniqueUdsSocketPath("move-fanout").string();
  cfg.backpressure_threshold = 1024;
  auto native = UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
  struct Cleanup {
    std::shared_ptr<UdsServer> native;
    boost::asio::io_context& io;
    std::string path;
    ~Cleanup() {
      test::stop_with_context(native, io);
      TestUtils::removeFileIfExists(path);
    }
  } cleanup{native, io, cfg.socket_path};
  auto pump = [&](auto ready) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!ready()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return true;
  };
  native->start();
  ASSERT_TRUE(pump([&] { return native->state() == base::LinkState::Listening; }));
  boost::asio::local::stream_protocol::socket first(io), second(io);
  first.connect(boost::asio::local::stream_protocol::endpoint(cfg.socket_path));
  ASSERT_TRUE(pump([&] { return native->client_count() == 1; }));
  const auto first_id = native->connected_clients().front();
  second.connect(boost::asio::local::stream_protocol::endpoint(cfg.socket_path));
  ASSERT_TRUE(pump([&] { return native->client_count() == 2; }));
  const auto limit = native->write_queue_limit(first_id);
  ASSERT_TRUE(limit.has_value());
  // Keep the executor paused. The first target has no capacity; the second
  // can still accept. The move belongs to the aggregate any-accepted result.
  ASSERT_TRUE(native->send_to_client(first_id, std::string(*limit, 'p')));
  auto send = [&](std::vector<uint8_t>& bytes) {
    return GetParam() ? native->async_try_write_move(std::move(bytes)) : native->async_write_move(std::move(bytes));
  };
  std::vector<uint8_t> oversized(*limit + 1, 'x');
  const auto original = oversized;
  EXPECT_FALSE(send(oversized));
  EXPECT_EQ(oversized, original);
  std::vector<uint8_t> payload{'o', 'k'};
  ASSERT_TRUE(send(payload));
  EXPECT_TRUE(payload.empty());
  payload.assign(2, 'z');  // The admitted data no longer belongs to the caller.
  ASSERT_TRUE(pump([&] {
    boost::system::error_code ec;
    return second.available(ec) >= 2 && !ec;
  }));
  std::array<char, 2> received{};
  boost::asio::read(second, boost::asio::buffer(received));
  EXPECT_EQ(std::string(received.data(), received.size()), "ok");
}
#endif
INSTANTIATE_TEST_SUITE_P(PlainAndTry, UdsMoveOwnershipTest, ::testing::Bool());
