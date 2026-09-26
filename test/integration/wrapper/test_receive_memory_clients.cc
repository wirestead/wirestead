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
  bool opened = false, inline_write = false, throw_write = false;
  void open(const std::string&, Error& ec) override {
    ec.clear();
    opened = true;
    ++opens;
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
    opened = true;
    ++opens;
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

class ReceiveMemoryClientTest : public ::testing::TestWithParam<int> {
 protected:
  net::io_context io;
  std::shared_ptr<interface::Channel> native;
  std::unique_ptr<wrapper::ChannelInterface> client;
  std::unique_ptr<net::ip::tcp::acceptor> acceptor;
  std::unique_ptr<net::ip::tcp::socket> tcp;
  std::unique_ptr<net::ip::udp::socket> udp;
  Endpoint* endpoint = nullptr;
  uint16_t port = 0;
  template <class F>
  bool pump(F ready) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!ready() && std::chrono::steady_clock::now() < deadline) {
      if (io.stopped()) io.restart();
      io.run_one_for(5ms);
    }
    return ready();
  }
  void create(bool reopen = false) {
    if (GetParam() == 0) {
      acceptor = std::make_unique<net::ip::tcp::acceptor>(io, net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
      tcp = std::make_unique<net::ip::tcp::socket>(io);
      acceptor->async_accept(*tcp, [](auto ec) { EXPECT_FALSE(ec); });
      config::TcpClientConfig cfg;
      cfg.port = acceptor->local_endpoint().port();
      cfg.max_retries = 0;
      native = transport::TcpClient::create(cfg, io);
      client = std::make_unique<wrapper::TcpClient>(native);
    } else if (GetParam() == 1 || GetParam() == 2) {
      auto fake = std::make_unique<Endpoint>();
      endpoint = fake.get();
      if (GetParam() == 1) {
        config::UdsClientConfig cfg;
        cfg.socket_path = "/fake/receive-memory";
        cfg.max_retries = 0;
        native = transport::UdsClient::create(cfg, std::move(fake), io);
        client = std::make_unique<wrapper::UdsClient>(native);
      } else {
        config::SerialConfig cfg;
        cfg.device = "/fake/receive-memory";
        cfg.reopen_on_error = reopen;
        cfg.retry_interval_ms = 1;
        native = transport::Serial::create(cfg, std::move(fake), io);
        client = std::make_unique<wrapper::Serial>(native);
      }
    } else {
      udp = std::make_unique<net::ip::udp::socket>(io, net::ip::udp::endpoint(net::ip::udp::v4(), 0));
      config::UdpConfig cfg;
      cfg.bind_address = "127.0.0.1";
      cfg.local_port = port = test::TestUtils::getAvailableTestPort();
      cfg.remote_address = "127.0.0.1";
      cfg.remote_port = udp->local_endpoint().port();
      native = transport::UdpChannel::create(cfg, io);
      client = std::make_unique<wrapper::UdpClient>(native);
    }
    limits({1500, 64, 8});
    client->framer(std::make_unique<framer::LineFramer>());
    if (GetParam() == 0)
      static_cast<wrapper::TcpClient&>(*client).batch_size(100).batch_latency(1h);
    else if (GetParam() == 1)
      static_cast<wrapper::UdsClient&>(*client).batch_size(100).batch_latency(1h);
    else if (GetParam() == 2)
      static_cast<wrapper::Serial&>(*client).batch_size(100).batch_latency(1h);
    else
      static_cast<wrapper::UdpClient&>(*client).batch_size(100).batch_latency(1h);
    auto started = client->start();
    ASSERT_TRUE(pump([&] { return started.wait_for(0ms) == std::future_status::ready; }));
    ASSERT_TRUE(started.get());
    if (endpoint) {
      ASSERT_TRUE(pump([&] { return bool(endpoint->read); }));
    }
    if (tcp) {
      ASSERT_TRUE(pump([&] { return tcp->is_open(); }));
    }
  }
  void limits(wrapper::ReceiveLimits value) {
    if (GetParam() == 0)
      static_cast<wrapper::TcpClient&>(*client).receive_limits(value);
    else if (GetParam() == 1)
      static_cast<wrapper::UdsClient&>(*client).receive_limits(value);
    else if (GetParam() == 2)
      static_cast<wrapper::Serial&>(*client).receive_limits(value);
    else
      static_cast<wrapper::UdpClient&>(*client).receive_limits(value);
  }
  wrapper::ReceiveMemoryStats stats() {
    if (GetParam() == 0) return static_cast<wrapper::TcpClient&>(*client).receive_stats();
    if (GetParam() == 1) return static_cast<wrapper::UdsClient&>(*client).receive_stats();
    if (GetParam() == 2) return static_cast<wrapper::Serial&>(*client).receive_stats();
    return static_cast<wrapper::UdpClient&>(*client).receive_stats();
  }
  void send(std::string text) {
    if (tcp)
      net::write(*tcp, net::buffer(text));
    else if (udp)
      udp->send_to(net::buffer(text), {net::ip::make_address("127.0.0.1"), port});
    else
      net::post(native->get_executor(), [this, text = std::move(text)] { endpoint->receive(text); });
  }
  void TearDown() override {
    if (client) test::stop_wrapper_with_context(*client, io);
  }
};
TEST_P(ReceiveMemoryClientTest, BatchOverflowClosesStreamOrPreservesUdpQueue) {
  create();
  size_t delivered = 0;
  client->on_data_batch([&](const auto&) { ++delivered; });
  send("a");
  ASSERT_TRUE(pump([&] { return stats().reserved_bytes > 100; }));
  const auto before = stats().reserved_bytes;
  client->reset_stats();
  EXPECT_EQ(stats().reserved_bytes, before);
  send(std::string(2000, 'x'));
  ASSERT_TRUE(pump([&] { return stats().overflow_events == 1; }));
  EXPECT_EQ(stats().overflow_by_reason[0], 1u);
  EXPECT_EQ(delivered, 0u);
  if (GetParam() == 3) {
    EXPECT_TRUE(native->is_connected());
    EXPECT_EQ(stats().reserved_bytes, before);
  } else {
    ASSERT_TRUE(pump([&] { return !native->is_connected(); }));
    EXPECT_EQ(stats().reserved_bytes, 0u);
  }
}
TEST_P(ReceiveMemoryClientTest, FrameOverflowNeverExposesRejectedInput) {
  create();
  size_t raw = 0;
  std::vector<std::string> messages;
  client->on_data([&](const auto&) { ++raw; });
  client->on_message([&](const auto& ctx) { messages.push_back(ctx.data_as_string()); });
  send("a");
  ASSERT_TRUE(pump([&] { return raw == 1; }));
  send(std::string(65, 'x'));
  ASSERT_TRUE(pump([&] { return stats().overflow_events == 1; }));
  EXPECT_EQ(stats().overflow_by_reason[1], 1u);
  EXPECT_EQ(raw, 1u);
  EXPECT_TRUE(messages.empty());
  if (GetParam() == 3) {
    send("\n");
    ASSERT_TRUE(pump([&] { return messages.size() == 1; }));
    EXPECT_EQ(messages[0], "a");
  } else
    EXPECT_FALSE(native->is_connected());
}
TEST_P(ReceiveMemoryClientTest, StopReleasesStorageAndInvalidLimitsDoNotApply) {
  create();
  EXPECT_THROW(limits({0, 1, 1}), std::invalid_argument);
  EXPECT_THROW(limits({1500, 64, 8}), std::logic_error);
  client->on_data_batch([](const auto&) {});
  send("held");
  ASSERT_TRUE(pump([&] { return stats().reserved_bytes > 100; }));
  test::stop_wrapper_with_context(*client, io);
  EXPECT_EQ(stats().reserved_bytes, 0u);
  EXPECT_NO_THROW(limits({4096, 128, 8}));
}
class ReceiveMemorySerialReopenTest : public ReceiveMemoryClientTest {};
TEST_P(ReceiveMemorySerialReopenTest, OverflowFollowsReopenPolicy) {
  create(true);
  client->on_data_batch([](const auto&) {});
  send(std::string(2000, 'x'));
  ASSERT_TRUE(pump([&] { return stats().overflow_events == 1 && endpoint->opens >= 2 && bool(endpoint->read); }));
  EXPECT_GE(endpoint->closes, 1u);
  EXPECT_TRUE(native->is_connected());
  EXPECT_EQ(stats().reserved_bytes, 0u);
}
TEST_P(ReceiveMemoryClientTest, CallbackStopKeepsDeliveredBatchChargedUntilReturn) {
  create();
  if (GetParam() == 0)
    static_cast<wrapper::TcpClient&>(*client).batch_size(1);
  else if (GetParam() == 1)
    static_cast<wrapper::UdsClient&>(*client).batch_size(1);
  else if (GetParam() == 2)
    static_cast<wrapper::Serial&>(*client).batch_size(1);
  else
    static_cast<wrapper::UdpClient&>(*client).batch_size(1);
  bool done = false, retained = false;
  client->on_data_batch([&](const auto& batch) {
    client->framer(std::make_unique<framer::LineFramer>());
    client->stop();
    retained = stats().reserved_bytes >= batch[0].data().size();
    done = true;
  });
  send("held");
  ASSERT_TRUE(pump([&] { return done; }));
  EXPECT_TRUE(retained);
  test::stop_wrapper_with_context(*client, io);
  EXPECT_EQ(stats().reserved_bytes, 0u);
}
TEST_P(ReceiveMemoryClientTest, NewRunResetsCountersAndKeepsLimits) {
  create();
  client->on_data_batch([](const auto&) {});
  send(std::string(2000, 'x'));
  ASSERT_TRUE(pump([&] { return stats().overflow_events == 1; }));
  test::stop_wrapper_with_context(*client, io);
  EXPECT_EQ(stats().overflow_events, 1u);
  if (tcp) {
    boost::system::error_code ec;
    tcp->close(ec);
    acceptor->async_accept(*tcp, [](auto error) { EXPECT_FALSE(error); });
  }
  auto ready = client->start();
  ASSERT_TRUE(pump([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  if (endpoint) {
    ASSERT_TRUE(pump([&] { return bool(endpoint->read); }));
  }
  if (tcp) {
    ASSERT_TRUE(pump([&] { return tcp->is_open(); }));
  }
  EXPECT_EQ(stats().overflow_events, 0u);
  EXPECT_EQ(stats().reserved_bytes, 0u);
  send(std::string(2000, 'x'));
  ASSERT_TRUE(pump([&] { return stats().overflow_events == 1; }));
  EXPECT_EQ(stats().overflow_by_reason[0], 1u);
}
INSTANTIATE_TEST_SUITE_P(Serial, ReceiveMemorySerialReopenTest, ::testing::Values(2));
INSTANTIATE_TEST_SUITE_P(AllClients, ReceiveMemoryClientTest, ::testing::Values(0, 1, 2, 3));
}  // namespace
