#include <gtest/gtest.h>

#ifdef __APPLE__
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include <boost/asio.hpp>
#include <chrono>
#include <csignal>
#include <memory>
#include <thread>

#include "test/utils/test_utils.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"

using namespace wirestead;
using namespace wirestead::transport;
namespace net = boost::asio;
using tcp = net::ip::tcp;

class TransportTcpServerSecurityTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif
  }

  void TearDown() override {
    if (server_) {
      server_->stop();
      server_.reset();
    }
  }

  std::shared_ptr<TcpServer> server_;
};

TEST_F(TransportTcpServerSecurityTest, NoIdleTimeoutByDefault) {
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.enable_port_retry = true;  // Enhance robustness

  server_ = TcpServer::create(cfg);
  server_->start();

  // Wait for server to start listening (up to 5 seconds)
  ASSERT_TRUE(test::TestUtils::waitForCondition(
      [&] { return server_->state() == wirestead::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  tcp::socket client(client_ioc);
  boost::system::error_code ec;

  // Retry connect logic (increased retry for slow CI)
  for (int i = 0; i < 50; ++i) {
    client = tcp::socket(client_ioc);
    client.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), cfg.port), ec);
    if (!ec) break;
    // Log failure for debugging
    if (i % 10 == 0) {
      std::cerr << "Connect attempt " << i << " failed: " << ec.message() << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_FALSE(ec) << "Failed to connect to server";

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
  // Fix for macOS/BSD SIGPIPE on write to closed socket
  int yes = 1;
  int result = setsockopt(static_cast<int>(client.native_handle()), SOL_SOCKET, SO_NOSIGPIPE, &yes,
                          static_cast<socklen_t>(sizeof(yes)));
  if (result < 0) {
    std::cerr << "setsockopt(SO_NOSIGPIPE) failed: " << errno << std::endl;
  }
#endif

  // Wait for 2 seconds (simulating idle)
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Check if still connected by writing something
  net::write(client, net::buffer("ping"), ec);

  EXPECT_FALSE(ec) << "Client should still be connected (no timeout by default)";
}

TEST_F(TransportTcpServerSecurityTest, IdleConnectionTimeout) {
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.idle_timeout_ms = 1000;    // 1 second timeout
  cfg.enable_port_retry = true;  // Enhance robustness

  server_ = TcpServer::create(cfg);
  server_->start();

  // Wait for server to start listening (up to 5 seconds)
  ASSERT_TRUE(test::TestUtils::waitForCondition(
      [&] { return server_->state() == wirestead::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  tcp::socket client(client_ioc);
  boost::system::error_code ec;

  // Retry connect logic (increased retry for slow CI)
  for (int i = 0; i < 50; ++i) {
    client = tcp::socket(client_ioc);
    client.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), cfg.port), ec);
    if (!ec) break;
    // Log failure for debugging
    if (i % 10 == 0) {
      std::cerr << "Connect attempt " << i << " failed: " << ec.message() << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_FALSE(ec) << "Failed to connect to server";

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
  // Fix for macOS/BSD SIGPIPE on write to closed socket
  int yes = 1;
  int result = setsockopt(static_cast<int>(client.native_handle()), SOL_SOCKET, SO_NOSIGPIPE, &yes,
                          static_cast<socklen_t>(sizeof(yes)));
  if (result < 0) {
    std::cerr << "setsockopt(SO_NOSIGPIPE) failed: " << errno << std::endl;
  }
#endif

  // Wait for 0.5 seconds (should stay connected)
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  net::write(client, net::buffer("ping"), ec);
  EXPECT_FALSE(ec) << "Client should still be connected (not timed out yet)";

  // Wait for 1.5 seconds more (total > 1s idle)
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // Write should fail or return broken pipe/reset
  // Note: writing might succeed if data is buffered, but reading should fail.
  // We try to read.

  char data[10];
  client.read_some(net::buffer(data), ec);

  // Depending on platform, read might return EOF (closed cleanly) or ConnectionReset.
  EXPECT_TRUE(ec == net::error::eof || ec == net::error::connection_reset || ec == net::error::broken_pipe)
      << "Client should have been disconnected due to timeout. Error: " << ec.message();
}

TEST_F(TransportTcpServerSecurityTest, BindToLocalhostOnly) {
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.bind_address = "127.0.0.1";
  cfg.enable_port_retry = true;

  server_ = TcpServer::create(cfg);
  server_->start();

  ASSERT_TRUE(test::TestUtils::waitForCondition(
      [&] { return server_->state() == wirestead::base::LinkState::Listening; }, 5000))
      << "Server failed to enter listening state";

  net::io_context client_ioc;
  tcp::socket client(client_ioc);
  boost::system::error_code ec;

  // Try connecting via 127.0.0.1 (should work)
  client.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), cfg.port), ec);
  EXPECT_FALSE(ec) << "Failed to connect to localhost";
}

TEST_F(TransportTcpServerSecurityTest, InvalidBindAddress) {
  config::TcpServerConfig cfg;
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.bind_address = "999.999.999.999";

  // Configuration validation should fail
  EXPECT_FALSE(cfg.is_valid());

  // If we force create server (validation is usually checked by user or factory), start should fail
  server_ = TcpServer::create(cfg);
  server_->start();

  // Should transition to Error state
  ASSERT_TRUE(
      test::TestUtils::waitForCondition([&] { return server_->state() == wirestead::base::LinkState::Error; }, 1000))
      << "Server should be in Error state due to invalid address";
}
