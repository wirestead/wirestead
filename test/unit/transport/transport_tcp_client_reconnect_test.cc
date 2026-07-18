#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;
using namespace std::chrono_literals;

class TcpClientReconnectTest : public ::testing::Test {
 protected:
  void SetUp() override { port_ = TestUtils::getAvailableTestPort(); }
  uint16_t port_;
};

TEST_F(TcpClientReconnectTest, ReconnectAfterServerStop) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port_;
  cfg.max_retries = -1;  // Infinite retries
  cfg.retry_interval_ms = 100;

  std::atomic<int> connect_count{0};
  std::mutex mtx;
  std::condition_variable cv;

  auto client = TcpClient::create(cfg);
  client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connect_count++;
      cv.notify_all();
    }
  });

  // 1. Start server and connect client
  auto server_ioc = std::make_shared<boost::asio::io_context>();
  boost::asio::ip::tcp::acceptor acceptor(*server_ioc,
                                          boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port_));

  std::thread server_thread([&]() { server_ioc->run(); });

  client->start();

  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, 2s, [&] { return connect_count == 1; }));
  }

  // 2. Stop server (Force disconnect)
  acceptor.close();
  server_ioc->stop();
  if (server_thread.joinable()) server_thread.join();

  // 3. Restart server on the same port
  auto server_ioc2 = std::make_shared<boost::asio::io_context>();
  boost::asio::ip::tcp::acceptor acceptor2(*server_ioc2,
                                           boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port_));

  std::thread server_thread2([&]() { server_ioc2->run(); });

  // 4. Wait for auto-reconnect
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return connect_count == 2; }))
        << "Client failed to reconnect after server restart";
  }

  client->stop();
  server_ioc2->stop();
  if (server_thread2.joinable()) server_thread2.join();
}

// #432: TcpClient::set_retry_interval() wrote straight into cfg_.retry_interval_ms
// with no re-validation, unlike the one-time validate_and_clamp() call at
// construction. Calling it live with 0 used to produce an unbounded tight
// reconnect loop (the same bug class reported for connection_timeout_ms).
// Verifies retry_interval(0) on an already-running client is floored at
// MIN_RETRY_INTERVAL_MS, by measuring spacing between repeated failed
// connect attempts against a port nothing is listening on.
TEST_F(TcpClientReconnectTest, LiveSetRetryIntervalIsClampedNotUnbounded) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port_;  // nothing listens here
  cfg.max_retries = -1;
  cfg.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;

  auto client = TcpClient::create(cfg);

  std::mutex mtx;
  std::condition_variable cv;
  std::deque<std::chrono::steady_clock::time_point> connecting_at;

  client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connecting) {
      std::lock_guard<std::mutex> lock(mtx);
      connecting_at.push_back(std::chrono::steady_clock::now());
      cv.notify_all();
    }
  });

  client->start();

  // Wait for the first connect attempt before flipping the live setter, so
  // the 0-clamp is guaranteed to apply to every retry timer scheduled
  // afterward rather than racing the client's own startup.
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return !connecting_at.empty(); }))
        << "Client never made its first connect attempt";
  }

  client->set_retry_interval(0);  // the live setter-bypass scenario from #432

  std::unique_lock<std::mutex> lock(mtx);
  ASSERT_TRUE(cv.wait_for(lock, 10s, [&] { return connecting_at.size() >= 3; }))
      << "Client never retried enough times to measure spacing";

  // Use the last gap: guaranteed to be scheduled after set_retry_interval(0)
  // took effect.
  auto it = connecting_at.end();
  auto t_last = *(--it);
  auto t_prev = *(--it);
  auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(t_last - t_prev).count();
  lock.unlock();

  EXPECT_GE(gap, static_cast<long>(base::constants::MIN_RETRY_INTERVAL_MS) - 20)
      << "retry_interval(0) must be floored at MIN_RETRY_INTERVAL_MS, not left unbounded";

  client->stop();
}

// #446: TcpClient's move ctor/assignment are defaulted (and public, unlike
// its private regular constructors), so a moved-from instance has a null
// impl_. Destroying it must not dereference that null pointer (matches
// TcpServer/Serial/UdpChannel/UdsServer, whose destructors already
// null-guard). Reachable via the public API by move-constructing from a
// dereferenced create()'d shared_ptr, which leaves the original
// (still-shared_ptr-owned) object moved-from; its destructor runs whenever
// the shared_ptr's refcount reaches zero.
TEST_F(TcpClientReconnectTest, DestroyingAMovedFromInstanceDoesNotCrash) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port_;  // nothing needs to be listening for this test

  auto client_ptr = TcpClient::create(cfg);
  TcpClient moved_to(std::move(*client_ptr));
  client_ptr.reset();  // destroys the now moved-from original - the assertion
}
