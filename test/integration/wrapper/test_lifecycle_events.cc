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

#include <boost/asio.hpp>
#include <cstring>

#include "tcp_stop_with_context.hpp"
#include "test_utils.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/interface/iuds_socket.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/wrapper/serial/serial.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/uds_client/uds_client.hpp"
namespace {
using namespace wirestead;
using namespace std::chrono_literals;
namespace net = boost::asio;
using Error = boost::system::error_code;
using Handler = std::function<void(const Error&, size_t)>;
class Endpoint : public interface::SerialPortInterface, public interface::UdsSocketInterface {
 public:
  Handler read, write;
  net::mutable_buffer receive_buffer;
  size_t opens = 0, closes = 0;
  size_t bytes = 0;
  size_t starts = 0;
  bool fail_open = false;
  bool opened = false, inline_write = false, throw_write = false;
  void open(const std::string&, Error& ec) override {
    ++opens;
    if (fail_open) {
      ec = net::error::connection_refused;
      return;
    }
    ec.clear();
    opened = true;
  }
  bool is_open() const override { return opened; }
  void close(Error& ec) override {
    ec.clear();
    opened = false;
    ++closes;
    auto r = std::move(read);
    auto w = std::move(write);
    if (r) r(net::error::operation_aborted, 0);
    if (w) w(net::error::operation_aborted, 0);
  }
  void set_option(const net::serial_port_base::baud_rate&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::character_size&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::stop_bits&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::parity&, Error& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::flow_control&, Error& ec) override { ec.clear(); }
  void async_connect(const net::local::stream_protocol::endpoint&, std::function<void(const Error&)> h) override {
    ++opens;
    if (fail_open) {
      h(net::error::connection_refused);
      return;
    }
    opened = true;
    h({});
  }
  void shutdown(net::local::stream_protocol::socket::shutdown_type, Error& ec) override { ec.clear(); }
  net::local::stream_protocol::endpoint remote_endpoint(Error& ec) const override {
    ec.clear();
    return {};
  }
  void async_read_some(const net::mutable_buffer& b, Handler h) override {
    receive_buffer = b;
    read = std::move(h);
  }
  void receive(std::string_view data) {
    ASSERT_TRUE(read);
    ASSERT_LE(data.size(), receive_buffer.size());
    std::memcpy(receive_buffer.data(), data.data(), data.size());
    auto h = std::move(read);
    h({}, data.size());
  }
  void async_write(const net::const_buffer& b, Handler h) override { start(b.size(), std::move(h)); }
  void async_write(const std::vector<net::const_buffer>& buffers, Handler h) override {
    size_t n = 0;
    for (const auto& b : buffers) n += b.size();
    start(n, std::move(h));
  }
  void start(size_t n, Handler h) {
    ++starts;
    bytes = n;
    if (throw_write) throw std::runtime_error("write initiation failure");
    if (inline_write)
      h({}, n);
    else
      write = std::move(h);
  }
  void complete(Error ec, size_t n) {
    auto h = std::move(write);
    ASSERT_TRUE(h);
    h(ec, n);
  }
};

class NativeLifecycleEventsTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  std::shared_ptr<interface::Channel> native;
  std::unique_ptr<wrapper::ChannelInterface> client;
  std::unique_ptr<net::ip::tcp::acceptor> acceptor;
  std::unique_ptr<net::ip::tcp::socket> tcp;
  std::unique_ptr<net::ip::udp::socket> udp;
  Endpoint* endpoint = nullptr;
  bool accepted = false;
  std::vector<char> events;
  template <class F>
  bool pump(F ready) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!ready() && std::chrono::steady_clock::now() < deadline) {
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return ready();
  }
  void create(bool retry, bool fail_start = false) {
    if (GetParam() == 0) {
      acceptor = std::make_unique<net::ip::tcp::acceptor>(io, net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
      config::TcpClientConfig cfg;
      cfg.port = acceptor->local_endpoint().port();
      cfg.max_retries = retry ? 2 : 0;
      cfg.retry_interval_ms = 1;
      if (fail_start)
        acceptor->close();
      else
        accept();
      native = transport::TcpClient::create(cfg, io);
      client = std::make_unique<wrapper::TcpClient>(native);
    } else if (GetParam() == 1 || GetParam() == 2) {
      auto fake = std::make_unique<Endpoint>();
      endpoint = fake.get();
      endpoint->fail_open = fail_start;
      if (GetParam() == 1) {
        config::UdsClientConfig cfg;
        cfg.socket_path = "/fake/lifecycle";
        cfg.max_retries = retry ? 2 : 0;
        cfg.retry_interval_ms = 1;
        native = transport::UdsClient::create(cfg, std::move(fake), io);
        client = std::make_unique<wrapper::UdsClient>(native);
      } else {
        config::SerialConfig cfg;
        cfg.device = "/fake/lifecycle";
        cfg.reopen_on_error = retry;
        cfg.retry_interval_ms = 1;
        native = transport::Serial::create(cfg, std::move(fake), io);
        client = std::make_unique<wrapper::Serial>(native);
      }
    } else {
      udp = std::make_unique<net::ip::udp::socket>(io, net::ip::udp::endpoint(net::ip::udp::v4(), 0));
      config::UdpConfig cfg;
      cfg.bind_address = fail_start ? "203.0.113.1" : "127.0.0.1";
      cfg.remote_address = "127.0.0.1";
      cfg.remote_port = udp->local_endpoint().port();
      native = transport::UdpChannel::create(cfg, io);
      client = std::make_unique<wrapper::UdpClient>(native);
    }
    client->on_connect([&](const auto&) { events.push_back('C'); });
    client->on_disconnect([&](const auto&) { events.push_back('D'); });
    client->on_error([&](const auto&) { events.push_back('E'); });
  }
  void accept() {
    tcp = std::make_unique<net::ip::tcp::socket>(io);
    accepted = false;
    acceptor->async_accept(*tcp, [this](auto ec) {
      EXPECT_TRUE(!ec || ec == net::error::operation_aborted);
      accepted = !ec;
    });
  }
  bool start() {
    auto f = client->start();
    if (!pump([&] { return f.wait_for(0ms) == std::future_status::ready; })) return false;
    const bool ready = f.get();
    if (ready && tcp && !pump([&] { return accepted; })) return false;
    return ready;
  }
  void lose() {
    if (tcp)
      tcp->close();
    else if (endpoint) {
      ASSERT_TRUE(pump([&] { return bool(endpoint->read); }));
      net::post(native->get_executor(), [&] {
        auto h = std::move(endpoint->read);
        ASSERT_TRUE(h);
        h(net::error::connection_reset, 0);
      });
    } else {
      EXPECT_TRUE(client->send(std::string(65536, 'x')));
    }
  }
  void TearDown() override {
    if (client) test::stop_wrapper_with_context(*client, io);
    if (acceptor) {
      boost::system::error_code ec;
      acceptor->close(ec);
    }
    io.restart();
    io.poll();
  }
};
TEST_P(NativeLifecycleEventsTest, TerminalLossReportsDisconnectBeforeError) {
  create(false);
  ASSERT_TRUE(start());
  lose();
  ASSERT_TRUE(pump([&] { return events.size() >= 3; }));
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'E'}));
  test::stop_wrapper_with_context(*client, io);
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'E'}));
}
TEST_P(NativeLifecycleEventsTest, StartFailureReportsErrorWithoutDisconnect) {
  create(false, true);
  EXPECT_FALSE(start());
  EXPECT_EQ(events, (std::vector<char>{'E'}));
}
class NativeRecoverableEventsTest : public NativeLifecycleEventsTest {};
TEST_P(NativeRecoverableEventsTest, SuccessfulRetryRetainsLossNotification) {
  create(true);
  ASSERT_TRUE(start());
  lose();
  if (GetParam() == 0) accept();
  ASSERT_TRUE(pump([&] { return events.size() >= 3; }));
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'C'}));
}
class NativeRetryExhaustionEventsTest : public NativeLifecycleEventsTest {};
TEST_P(NativeRetryExhaustionEventsTest, FailedRetriesReportOneTerminalError) {
  create(true);
  ASSERT_TRUE(start());
  if (GetParam() == 0)
    acceptor->close();
  else
    endpoint->fail_open = true;
  lose();
  ASSERT_TRUE(pump([&] { return !events.empty() && events.back() == 'E'; }));
  EXPECT_EQ(events, (std::vector<char>{'C', 'D', 'E'}));
}
INSTANTIATE_TEST_SUITE_P(AllClients, NativeLifecycleEventsTest, ::testing::Values(0, 1, 2, 3));
INSTANTIATE_TEST_SUITE_P(ReconnectingClients, NativeRecoverableEventsTest, ::testing::Values(0, 1, 2));
INSTANTIATE_TEST_SUITE_P(BoundedRetries, NativeRetryExhaustionEventsTest, ::testing::Values(0, 1));
}  // namespace
