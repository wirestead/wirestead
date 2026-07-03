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

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "test_constants.hpp"
#include "test_utils.hpp"
#include "unilink/config/tcp_server_config.hpp"
#include "unilink/interface/itcp_acceptor.hpp"
#include "unilink/memory/safe_span.hpp"
#include "unilink/transport/tcp_server/tcp_server.hpp"

using namespace unilink;
using namespace unilink::transport;
using namespace unilink::test;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

class FakeTcpAcceptor : public interface::TcpAcceptorInterface {
 public:
  enum class FailureMode { None, Open, Listen, Accept };

  FakeTcpAcceptor(net::io_context& ioc, FailureMode mode) : ioc_(ioc), mode_(mode) {}

  void open(const tcp&, boost::system::error_code& ec) override {
    if (mode_ == FailureMode::Open) {
      ec = net::error::fault;
      return;
    }
    open_ = true;
    ec.clear();
  }

  void bind(const tcp::endpoint&, boost::system::error_code& ec) override { ec.clear(); }

  void listen(int, boost::system::error_code& ec) override {
    if (mode_ == FailureMode::Listen) {
      ec = net::error::fault;
      return;
    }
    ec.clear();
  }

  bool is_open() const override { return open_; }

  void close(boost::system::error_code& ec) override {
    open_ = false;
    ec.clear();
  }

  void async_accept(std::function<void(const boost::system::error_code&, tcp::socket)> handler) override {
    net::post(ioc_, [this, handler = std::move(handler)]() mutable {
      tcp::socket socket(ioc_);
      if (mode_ == FailureMode::Accept) {
        handler(net::error::connection_reset, std::move(socket));
      }
    });
  }

 private:
  net::io_context& ioc_;
  FailureMode mode_;
  bool open_{false};
};

}  // namespace

class TransportTcpServerTest : public ::testing::Test {
 protected:
  void TearDown() override {
    if (server_) {
      server_->stop();
      server_.reset();
    }
    // Give some time for io_context to cleanup
    TestUtils::waitFor(constants::kShortTimeout.count());
  }

  std::shared_ptr<TcpServer> server_;
};

TEST_F(TransportTcpServerTest, LifecycleStartStop) {
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg);

  EXPECT_NO_THROW(server_->start());
  // Wait until it enters listening state
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->state() == base::LinkState::Listening; }, 1000));

  EXPECT_NO_THROW(server_->stop());
}

TEST_F(TransportTcpServerTest, WriteWithoutActiveSessionReturnsFalse) {
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg);
  ASSERT_NE(server_, nullptr);
  server_->start();
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server_->state() == base::LinkState::Listening; }, 1000));

  std::vector<uint8_t> payload = {1, 2, 3};
  auto shared_payload = std::make_shared<const std::vector<uint8_t>>(payload);

  EXPECT_FALSE(server_->async_write_copy(memory::ConstByteSpan(payload.data(), payload.size())));
  EXPECT_FALSE(server_->async_write_move(std::vector<uint8_t>{1, 2, 3}));
  EXPECT_FALSE(server_->async_write_shared(shared_payload));
  EXPECT_FALSE(server_->broadcast("payload"));
  EXPECT_FALSE(server_->broadcast(memory::ConstByteSpan(payload.data(), payload.size())));
}

TEST_F(TransportTcpServerTest, BindFailureTriggerError) {
  // First server occupies the port

  uint16_t port = TestUtils::getAvailableTestPort();

  net::io_context ioc_occupy;  // Use a separate io_context for the occupying acceptor

  auto work_guard = net::make_work_guard(ioc_occupy);  // Prevent run() from returning

  // Explicitly disable reuse_address on ALL platforms to ensure conflict.
  // Standard acceptor constructor enables reuse_address by default, which can cause
  // flaky tests on Unix systems if the kernel decides to allow binding (e.g. SO_REUSEPORT).

  tcp::acceptor acceptor(ioc_occupy);

  boost::system::error_code ec_bind;

  acceptor.open(tcp::v4(), ec_bind);

  if (!ec_bind) {
    acceptor.set_option(net::socket_base::reuse_address(false), ec_bind);
  }

  // Bind to INADDR_ANY to match the server bind address and force a real conflict on macOS.
  acceptor.bind(tcp::endpoint(tcp::v4(), port), ec_bind);

  acceptor.listen(net::socket_base::max_listen_connections, ec_bind);

  // Ensure the occupying acceptor is actually listening

  EXPECT_FALSE(ec_bind) << "Occupying acceptor failed to bind/listen: " << ec_bind.message();

  // Run the occupying io_context in a thread to keep the port occupied

  std::thread occupy_thread([&ioc_occupy]() { ioc_occupy.run(); });

  // Guard to ensure thread is joined even if assertions fail
  struct ThreadGuard {
    std::thread& t;
    net::io_context& ioc;
    ~ThreadGuard() {
      ioc.stop();
      if (t.joinable()) t.join();
    }
  } thread_guard{occupy_thread, ioc_occupy};

  // Give it a moment to ensure port is occupied

  TestUtils::waitFor(constants::kDefaultTimeout.count());

  // Verify port is actually occupied by connecting to it (with retries)
  {
    net::io_context probe_ioc;
    boost::system::error_code probe_ec;
    // Increase retry count to handle slow macOS CI environments
    for (int i = 0; i < 50; ++i) {
      tcp::socket probe_sock(probe_ioc);
      probe_sock.connect(tcp::endpoint(net::ip::address_v4::loopback(), port), probe_ec);
      if (!probe_ec) break;

      // Log retry on failure to help debug persistent issues
      if (i > 0 && i % 10 == 0) {
        std::cerr << "Probe connection retry " << i << " failed: " << probe_ec.message() << std::endl;
      }
      std::this_thread::sleep_for(constants::kDefaultTimeout);
    }

    ASSERT_FALSE(probe_ec) << "Failed to connect to occupying acceptor on port " << port << ": " << probe_ec.message();
  }
  // Second server tries to bind to same port

  config::TcpServerConfig cfg;

  cfg.port = port;

  cfg.port_retry_interval_ms = constants::kShortTimeout.count();

  cfg.max_port_retries = 0;  // Fail immediately after first attempt

  server_ = TcpServer::create(cfg);

  std::atomic<bool> error_occurred{false};

  server_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_occurred = true;
    }
  });

  server_->start();

  // Wait for error state

  EXPECT_TRUE(TestUtils::waitForCondition([&] { return error_occurred.load(); }, 1000));  // Increased timeout

  server_->stop();
}

TEST_F(TransportTcpServerTest, MaxClientsLimit) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::TcpServerConfig cfg;
  cfg.port = port;
  cfg.max_connections = 1;

  server_ = TcpServer::create(cfg);
  server_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->state() == base::LinkState::Listening; }, 1000));

  // Client 1 connects
  net::io_context client_ioc;
  tcp::socket client1(client_ioc);
  client1.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));

  // Client 2 connects - should be accepted but then disconnected or rejected depending on implementation
  // Unilink implementation usually accepts and then immediately closes if limit reached,
  // or logic might handle it differently.
  // Let's verify behavior.

  tcp::socket client2(client_ioc);
  boost::system::error_code ec;
  client2.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port), ec);

  // Check if client2 is disconnected
  if (!ec) {
    // Try to read, should get EOF if server closed it
    char data[1];
    std::atomic<bool> read_completed{false};
    boost::system::error_code read_ec;

    client2.async_read_some(net::buffer(data), [&](const boost::system::error_code& ec, size_t) {
      read_ec = ec;
      read_completed = true;
    });

    // Run io_context to process the async read
    // Give enough time for server to accept and then close the connection
    for (int i = 0; i < 10 && !read_completed.load(); ++i) {
      client_ioc.poll();
      if (!read_completed.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    EXPECT_TRUE(read_completed.load()) << "Read from rejected client should complete (with EOF/error)";
    // Expect EOF or error
    EXPECT_TRUE(read_ec == net::error::eof || read_ec == net::error::connection_reset ||
                read_ec == net::error::broken_pipe);
  }
}

TEST_F(TransportTcpServerTest, PortBindingRetrySuccess) {
  uint16_t port = TestUtils::getAvailableTestPort();

  // Occupy port temporarily
  {
    net::io_context ioc;
    tcp::acceptor acceptor(ioc, tcp::endpoint(tcp::v4(), port));

    config::TcpServerConfig cfg;
    cfg.port = port;
    cfg.enable_port_retry = true;
    cfg.max_port_retries = 15;  // Increased to 15 to allow sufficient time for port release
    cfg.port_retry_interval_ms = constants::kShortTimeout.count();

    server_ = TcpServer::create(cfg);
    server_->start();

    // Server should be in Connecting/Retry loop (or internal wait)
    // We can't easily check internal state, but we can release the port and see if it binds

    std::this_thread::sleep_for(constants::kDefaultTimeout);
  }  // acceptor closes here

  // Now server should succeed in binding
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        // We need a way to check if listening.
        // Since we don't have public is_listening(), we can try to connect to it.
        net::io_context client_ioc;
        tcp::socket sock(client_ioc);
        boost::system::error_code ec;
        sock.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port), ec);
        return !ec;
      },
      1000));
}

TEST_F(TransportTcpServerTest, AcceptErrorHandling) {
  // To simulate accept error, we can close the acceptor externally if possible,
  // or use a mock. Since this is a real transport test, it's hard to force accept error
  // without mocking. However, we can test that server survives trivial errors.
  // For now, let's verify basic start/stop robustness under load which might trigger some internal paths.

  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();
  server_ = TcpServer::create(cfg);
  server_->start();

  // Just ensure it doesn't crash on immediate stop
  server_->stop();
}

TEST_F(TransportTcpServerTest, CallbackUpdatePropagation) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::TcpServerConfig cfg;
  cfg.port = port;

  server_ = TcpServer::create(cfg);
  server_->start();
  EXPECT_TRUE(TestUtils::waitForCondition([&]() { return server_->state() == base::LinkState::Listening; }, 1000));

  std::atomic<int> cb1_count{0};
  std::atomic<int> cb2_count{0};

  // 1. Connect Client 1
  net::io_context client_ioc;
  tcp::socket client1(client_ioc);
  client1.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  TestUtils::waitFor(50);  // Ensure accepted

  // 2. Set callback 1
  // This will overwrite client1's callback because client1 is current_session_
  server_->on_bytes([&](memory::ConstByteSpan) { cb1_count++; });

  std::string data = "test";
  net::write(client1, net::buffer(data));
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return cb1_count == 1; }, 1000));
  cb1_count = 0;

  // 3. Connect Client 2
  tcp::socket client2(client_ioc);
  client2.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  TestUtils::waitFor(50);  // Ensure accepted

  // 4. Set callback 2
  // This will overwrite client2's callback (current).
  // client1 (not current) will NOT be updated if it lost its wrapper in step 2.
  server_->on_bytes([&](memory::ConstByteSpan) { cb2_count++; });

  // Verify client2 uses cb2
  net::write(client2, net::buffer(data));
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return cb2_count == 1; }, 1000));
  cb2_count = 0;

  // Verify client1 behavior
  net::write(client1, net::buffer(data));

  // If bug is present, client1 calls cb1 (cb1_count increments), NOT cb2.
  bool called_cb2 = TestUtils::waitForCondition([&] { return cb2_count == 1; }, 1000);

  if (!called_cb2) {
    // It likely called cb1
    EXPECT_TRUE(cb1_count > 0) << "Client 1 should have called cb1 if it didn't call cb2";
    // Fail explicitly if we want to assert the fix is needed
    // But we are creating the test to FAIL initially.
  }

  // To assert correct behavior (once fixed):
  EXPECT_TRUE(called_cb2) << "Client 1 should have picked up the new callback cb2";
}

TEST_F(TransportTcpServerTest, InjectedNullAcceptorThrows) {
  net::io_context ioc;
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  EXPECT_THROW((void)TcpServer::create(cfg, nullptr, ioc), std::runtime_error);
}

TEST_F(TransportTcpServerTest, InvalidBindAddressMovesToErrorAndSwallowsStateException) {
  net::io_context ioc;
  config::TcpServerConfig cfg;
  cfg.bind_address = "not an address";
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg, std::make_unique<FakeTcpAcceptor>(ioc, FakeTcpAcceptor::FailureMode::None), ioc);
  server_->on_state([](base::LinkState) { throw std::runtime_error("state"); });

  EXPECT_NO_THROW({
    server_->start();
    ioc.run_for(std::chrono::milliseconds(50));
  });
  EXPECT_EQ(server_->state(), base::LinkState::Error);

  server_->on_state(nullptr);
  server_->stop();
  server_.reset();
}

TEST_F(TransportTcpServerTest, InjectedAcceptorOpenFailureMovesToError) {
  net::io_context ioc;
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg, std::make_unique<FakeTcpAcceptor>(ioc, FakeTcpAcceptor::FailureMode::Open), ioc);
  std::atomic<bool> error_seen{false};
  server_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  server_->start();
  ioc.run_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(error_seen.load());
  // #445: last_error_info() should now report detail for this transport too.
  ASSERT_TRUE(server_->last_error_info().has_value());
  EXPECT_EQ(server_->last_error_info()->component, "tcp_server");

  server_->stop();
  server_.reset();
}

TEST_F(TransportTcpServerTest, InjectedAcceptorListenFailureMovesToError) {
  net::io_context ioc;
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg, std::make_unique<FakeTcpAcceptor>(ioc, FakeTcpAcceptor::FailureMode::Listen), ioc);
  std::atomic<bool> error_seen{false};
  server_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  server_->start();
  ioc.run_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(error_seen.load());

  server_->stop();
  server_.reset();
}

TEST_F(TransportTcpServerTest, InjectedAcceptErrorMovesToError) {
  net::io_context ioc;
  config::TcpServerConfig cfg;
  cfg.port = TestUtils::getAvailableTestPort();

  server_ = TcpServer::create(cfg, std::make_unique<FakeTcpAcceptor>(ioc, FakeTcpAcceptor::FailureMode::Accept), ioc);
  std::atomic<bool> error_seen{false};
  server_->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_seen = true;
    }
  });

  server_->start();
  ioc.run_for(std::chrono::milliseconds(50));

  EXPECT_TRUE(error_seen.load());

  server_->stop();
  server_.reset();
}

TEST_F(TransportTcpServerTest, ConnectedClientWriteAndQueryApis) {
  uint16_t port = TestUtils::getAvailableTestPort();
  config::TcpServerConfig cfg;
  cfg.port = port;

  server_ = TcpServer::create(cfg);

  std::atomic<bool> connected{false};
  std::atomic<ClientId> client_id{0};
  server_->on_multi_connect([&](ClientId id, const std::string& info) {
    client_id = id;
    connected = true;
    EXPECT_FALSE(info.empty());
  });

  server_->start();
  ASSERT_TRUE(TestUtils::waitForCondition([&]() { return server_->state() == base::LinkState::Listening; }, 1000));

  net::io_context client_ioc;
  tcp::socket client(client_ioc);
  client.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));

  ASSERT_TRUE(TestUtils::waitForCondition([&] { return connected.load(); }, 1000));
  ASSERT_TRUE(server_->is_connected());
  EXPECT_FALSE(server_->is_backpressure_active());
  EXPECT_FALSE(server_->is_backpressure_active(client_id.load()));
  EXPECT_EQ(server_->client_count(), 1U);
  EXPECT_EQ(server_->connected_clients().size(), 1U);

  std::vector<uint8_t> copy_payload = {'c', 'o', 'p', 'y'};
  std::vector<uint8_t> span_payload = {'s', 'p', 'a', 'n'};
  auto shared_payload =
      std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{'s', 'h', 'a', 'r', 'e', 'd'});

  EXPECT_TRUE(server_->async_write_copy(memory::ConstByteSpan(copy_payload.data(), copy_payload.size())));
  EXPECT_TRUE(server_->async_write_move(std::vector<uint8_t>{'m', 'o', 'v', 'e'}));
  EXPECT_TRUE(server_->async_write_shared(shared_payload));
  EXPECT_TRUE(server_->broadcast("broadcast"));
  EXPECT_TRUE(server_->broadcast(memory::ConstByteSpan(span_payload.data(), span_payload.size())));
  EXPECT_TRUE(server_->send_to_client(client_id.load(), "direct"));
  EXPECT_TRUE(
      server_->send_to_client(client_id.load(), memory::ConstByteSpan(span_payload.data(), span_payload.size())));
  EXPECT_FALSE(server_->send_to_client(client_id.load() + 1000, "missing"));

  client.non_blocking(true);
  std::string received;
  std::array<char, 256> buffer{};
  ASSERT_TRUE(TestUtils::waitForCondition(
      [&] {
        boost::system::error_code ec;
        const auto n = client.receive(net::buffer(buffer), 0, ec);
        if (!ec && n > 0) {
          received.append(buffer.data(), n);
        }
        return received.find("copy") != std::string::npos && received.find("move") != std::string::npos &&
               received.find("shared") != std::string::npos && received.find("broadcast") != std::string::npos &&
               received.find("span") != std::string::npos && received.find("direct") != std::string::npos;
      },
      1000));

  server_->stop();
  server_.reset();
}
