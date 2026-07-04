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

#ifndef _WIN32

#include <sys/stat.h>

#include <boost/asio.hpp>
#include <chrono>
#include <fstream>
#include <memory>
#include <thread>

#include "test/utils/test_utils.hpp"
#include "unilink/config/uds_config.hpp"
#include "unilink/transport/uds/uds_server.hpp"

using namespace unilink;
using namespace unilink::transport;
namespace net = boost::asio;
using uds_socket = net::local::stream_protocol::socket;
using uds_endpoint = net::local::stream_protocol::endpoint;

class TransportUdsServerSecurityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    socket_path_ = test::TestUtils::makeUniqueUdsSocketPath("uds_sec");
    test::TestUtils::removeFileIfExists(socket_path_);
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
      server_.reset();
    }
    test::TestUtils::removeFileIfExists(socket_path_);
  }

  std::shared_ptr<UdsServer> server_;
  std::filesystem::path socket_path_;
};

TEST_F(TransportUdsServerSecurityTest, NoIdleTimeoutByDefault) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();

  server_ = UdsServer::create(cfg);
  server_->start();

  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  uds_socket client(client_ioc);
  boost::system::error_code ec;

  for (int i = 0; i < 50; ++i) {
    client = uds_socket(client_ioc);
    client.connect(uds_endpoint(socket_path_.string()), ec);
    if (!ec) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_FALSE(ec) << "Failed to connect to UDS server";

  // Idle for 2 seconds — should remain connected with no timeout configured
  std::this_thread::sleep_for(std::chrono::seconds(2));

  net::write(client, net::buffer("ping"), ec);
  EXPECT_FALSE(ec) << "Client should still be connected (no idle timeout by default)";
}

TEST_F(TransportUdsServerSecurityTest, IdleConnectionTimeout) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  cfg.idle_timeout_ms = 1000;  // 1 second

  server_ = UdsServer::create(cfg);
  server_->start();

  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  uds_socket client(client_ioc);
  boost::system::error_code ec;

  for (int i = 0; i < 50; ++i) {
    client = uds_socket(client_ioc);
    client.connect(uds_endpoint(socket_path_.string()), ec);
    if (!ec) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_FALSE(ec) << "Failed to connect to UDS server";

  // Send within timeout — should succeed
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  net::write(client, net::buffer("ping"), ec);
  EXPECT_FALSE(ec) << "Client should still be connected (not timed out yet)";

  // Now idle past the timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  char buf[16];
  client.read_some(net::buffer(buf), ec);
  EXPECT_TRUE(ec == net::error::eof || ec == net::error::connection_reset || ec == net::error::broken_pipe)
      << "Client should have been disconnected due to idle timeout. Error: " << ec.message();
}

TEST_F(TransportUdsServerSecurityTest, IdleTimeoutResetOnMessage) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  cfg.idle_timeout_ms = 1000;  // 1 second

  server_ = UdsServer::create(cfg);
  server_->start();

  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  uds_socket client(client_ioc);
  boost::system::error_code ec;

  for (int i = 0; i < 50; ++i) {
    client = uds_socket(client_ioc);
    client.connect(uds_endpoint(socket_path_.string()), ec);
    if (!ec) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_FALSE(ec) << "Failed to connect to UDS server";

  // Send messages every 600ms for 3 seconds — each send resets the 1s timer
  for (int i = 0; i < 5; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    net::write(client, net::buffer("keepalive"), ec);
    ASSERT_FALSE(ec) << "Write " << i << " failed — client was disconnected prematurely";
  }
}

// #438: a regular file at the configured socket path must never be silently
// deleted and bound over - that would destroy whatever the file was for
// with no warning, on nothing more than a path collision.
TEST_F(TransportUdsServerSecurityTest, RegularFileAtSocketPathBlocksBind) {
  {
    std::ofstream f(socket_path_);
    f << "not a socket";
  }
  ASSERT_TRUE(std::filesystem::exists(socket_path_));

  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  server_ = UdsServer::create(cfg);
  server_->start();

  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Error; }, 5000))
      << "Server should have entered Error state instead of deleting the regular file";
  EXPECT_TRUE(std::filesystem::exists(socket_path_)) << "The pre-existing regular file must not have been deleted";
}

// #438: if another process is already listening on the configured path, a
// second server must fail to start rather than silently deleting the first
// server's socket file and taking over the address (hijack).
TEST_F(TransportUdsServerSecurityTest, LiveListenerAtSocketPathBlocksSecondBind) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  server_ = UdsServer::create(cfg);
  server_->start();
  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000));

  auto second_server = UdsServer::create(cfg);
  second_server->start();
  EXPECT_TRUE(test::TestUtils::waitForCondition(
      [&] { return second_server->state() == unilink::base::LinkState::Error; }, 5000))
      << "Second server should fail to bind instead of hijacking the live listener's socket";
  second_server->stop();

  // The first server must still be reachable - proving its socket survived.
  net::io_context client_ioc;
  uds_socket client(client_ioc);
  boost::system::error_code ec;
  for (int i = 0; i < 50; ++i) {
    client = uds_socket(client_ioc);
    client.connect(uds_endpoint(socket_path_.string()), ec);
    if (!ec) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_FALSE(ec) << "First server's socket should still be live and connectable";
}

// #438: a leftover socket file from a process that's no longer running
// (nothing accepts a probe connect) must still be removable so the server
// can bind normally - the fix must not regress the ordinary restart case.
TEST_F(TransportUdsServerSecurityTest, StaleSocketFileDoesNotBlockBind) {
  {
    net::io_context probe_ioc;
    uds_socket probe(probe_ioc);
    boost::system::error_code ec;
    probe.open(net::local::stream_protocol(), ec);
    ASSERT_FALSE(ec);
    probe.bind(uds_endpoint(socket_path_.string()), ec);
    ASSERT_FALSE(ec);
    // Deliberately never call listen()/close the fd cleanly via destructor
    // going out of scope - leaves a bound-but-abandoned socket file behind,
    // matching a process that crashed without cleanup.
  }
  ASSERT_TRUE(std::filesystem::exists(socket_path_));

  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  server_ = UdsServer::create(cfg);
  server_->start();

  EXPECT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000))
      << "A stale (unlistened) socket file must not block a fresh bind";
}

TEST_F(TransportUdsServerSecurityTest, SocketPermissionsAreAppliedAfterBind) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  cfg.socket_permissions = 0600;

  server_ = UdsServer::create(cfg);
  server_->start();
  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000));

  struct stat st {};
  ASSERT_EQ(::stat(socket_path_.string().c_str(), &st), 0);
  EXPECT_EQ(st.st_mode & 0777, 0600u) << "Socket file permissions should match the configured mode";
}

TEST_F(TransportUdsServerSecurityTest, NoSocketPermissionsChangeByDefault) {
  config::UdsServerConfig cfg;
  cfg.socket_path = socket_path_.string();
  // socket_permissions left at its default (-1) - no chmod should occur.

  server_ = UdsServer::create(cfg);
  server_->start();
  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == unilink::base::LinkState::Listening; }, 5000));

  struct stat st {};
  ASSERT_EQ(::stat(socket_path_.string().c_str(), &st), 0);
  // Not asserting an exact mode here (depends on umask) - just that start()
  // didn't crash/error when socket_permissions is left unset.
}

#endif  // _WIN32
