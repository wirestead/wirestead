#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <thread>

#include "test_utils.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/config/tcp_client_config.hpp"
#include "unilink/transport/tcp_client/tcp_client.hpp"

using namespace unilink;
using namespace unilink::transport;
using namespace unilink::test;
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
  cfg.retry_interval_ms = 500;  // deliberately not the eventual live value

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
  client->set_retry_interval(0);  // the live setter-bypass scenario from #432

  std::unique_lock<std::mutex> lock(mtx);
  ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return connecting_at.size() >= 4; }))
      << "Client never retried enough times to measure spacing";

  // Compare the last two gaps (earlier ones may still reflect the
  // pre-live-set 500ms interval or in-flight timers).
  auto it = connecting_at.end();
  auto t3 = *(--it);
  auto t2 = *(--it);
  auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();
  lock.unlock();

  EXPECT_GE(gap, static_cast<long>(base::constants::MIN_RETRY_INTERVAL_MS) - 20)
      << "retry_interval(0) must be floored at MIN_RETRY_INTERVAL_MS, not left unbounded";

  client->stop();
}
