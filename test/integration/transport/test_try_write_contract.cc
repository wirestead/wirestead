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
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/config/serial_config.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/config/uds_config.hpp"
#include "wirestead/interface/iserial_port.hpp"
#include "wirestead/interface/iuds_socket.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/serial/serial.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server_session.hpp"

using namespace wirestead;
using namespace wirestead::base::constants;
using namespace wirestead::transport;
using namespace std::chrono_literals;
namespace net = boost::asio;
using uds = net::local::stream_protocol;

namespace {

constexpr size_t kBpHigh = MIN_BACKPRESSURE_THRESHOLD;

void poll_for(net::io_context& ioc, const std::function<bool()>& done) {
  for (int i = 0; i < 32 && !done(); ++i) {
    ioc.restart();
    ioc.poll();
  }
}

class StallingUdsSocket : public interface::UdsSocketInterface {
 public:
  struct WriteEntry {
    size_t size;
    std::function<void(const boost::system::error_code&, std::size_t)> handler;
  };

  explicit StallingUdsSocket(net::io_context& ioc) : ioc_(ioc) {}

  void async_read_some(const net::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    writes_.push_back({buffer.size(), std::move(handler)});
  }

  void async_connect(const uds::endpoint&, std::function<void(const boost::system::error_code&)> handler) override {
    net::post(ioc_, [handler = std::move(handler)]() mutable { handler({}); });
  }

  void shutdown(uds::socket::shutdown_type, boost::system::error_code& ec) override { ec.clear(); }

  void close(boost::system::error_code& ec) override {
    ec.clear();
    if (read_handler_) {
      auto handler = std::move(read_handler_);
      net::post(ioc_, [handler = std::move(handler)]() mutable {
        handler(make_error_code(net::error::operation_aborted), 0);
      });
    }
  }

  uds::endpoint remote_endpoint(boost::system::error_code& ec) const override {
    ec.clear();
    return uds::endpoint("/tmp/wirestead-test-peer");
  }

  bool complete_one(boost::system::error_code ec = {}) {
    if (writes_.empty()) return false;
    auto entry = std::move(writes_.front());
    writes_.pop_front();
    net::post(ioc_, [handler = std::move(entry.handler), ec, n = entry.size]() mutable { handler(ec, n); });
    return true;
  }

  size_t pending_write_count() const { return writes_.size(); }

 private:
  net::io_context& ioc_;
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
  std::deque<WriteEntry> writes_;
};

class StallingSerialPort : public interface::SerialPortInterface {
 public:
  struct WriteEntry {
    size_t size;
    std::function<void(const boost::system::error_code&, std::size_t)> handler;
  };

  explicit StallingSerialPort(net::io_context& ioc) : ioc_(ioc) {}

  void open(const std::string&, boost::system::error_code& ec) override {
    open_ = true;
    ec.clear();
  }
  bool is_open() const override { return open_; }
  void close(boost::system::error_code& ec) override {
    open_ = false;
    ec.clear();
    if (read_handler_) {
      auto handler = std::move(read_handler_);
      net::post(ioc_, [handler = std::move(handler)]() mutable {
        handler(make_error_code(net::error::operation_aborted), 0);
      });
    }
  }

  void set_option(const net::serial_port_base::baud_rate&, boost::system::error_code& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::character_size&, boost::system::error_code& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::stop_bits&, boost::system::error_code& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::parity&, boost::system::error_code& ec) override { ec.clear(); }
  void set_option(const net::serial_port_base::flow_control&, boost::system::error_code& ec) override { ec.clear(); }

  void async_read_some(const net::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const net::const_buffer& buffer,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    writes_.push_back({buffer.size(), std::move(handler)});
  }

  bool complete_one(boost::system::error_code ec = {}) {
    if (writes_.empty()) return false;
    auto entry = std::move(writes_.front());
    writes_.pop_front();
    net::post(ioc_, [handler = std::move(entry.handler), ec, n = entry.size]() mutable { handler(ec, n); });
    return true;
  }

  size_t pending_write_count() const { return writes_.size(); }

 private:
  net::io_context& ioc_;
  bool open_{false};
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
  std::deque<WriteEntry> writes_;
};

template <typename Transport>
void expect_reliable_try_write_rejects_without_pending(Transport& transport, size_t payload_size = 1) {
  auto before = transport.stats();
  std::vector<uint8_t> copy_payload(payload_size, 0xA1);

  EXPECT_FALSE(transport.async_try_write_copy(memory::ConstByteSpan(copy_payload.data(), copy_payload.size())));
  EXPECT_FALSE(transport.async_try_write_move(std::vector<uint8_t>(payload_size, 0xA2)));
  EXPECT_FALSE(transport.async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(payload_size, 0xA3)));

  auto after = transport.stats();
  EXPECT_EQ(after.pending_bytes, before.pending_bytes);
  EXPECT_EQ(after.failed_sends, before.failed_sends + 3);
  EXPECT_EQ(after.dropped_messages, before.dropped_messages);
  EXPECT_EQ(after.dropped_bytes, before.dropped_bytes);
}

template <typename Transport>
void expect_best_effort_try_write_counts_drop(Transport& transport, size_t payload_size = 1) {
  auto before = transport.stats();

  EXPECT_FALSE(transport.async_try_write_move(std::vector<uint8_t>(payload_size, 0xB1)));

  auto after = transport.stats();
  EXPECT_EQ(after.pending_bytes, before.pending_bytes);
  EXPECT_EQ(after.dropped_messages, before.dropped_messages + 1);
  EXPECT_EQ(after.dropped_bytes, before.dropped_bytes + payload_size);
}

template <typename Transport>
void expect_try_write_true_return_remains_accepted(Transport& transport, net::io_context& ioc, size_t normal_size = 900,
                                                   size_t try_size = 200) {
  ASSERT_TRUE(transport.async_write_move(std::vector<uint8_t>(normal_size, 0xC1)));
  ASSERT_TRUE(transport.async_try_write_move(std::vector<uint8_t>(try_size, 0xC2)));

  auto reserved = transport.stats();
  EXPECT_EQ(reserved.failed_sends, 0u);
  EXPECT_EQ(reserved.dropped_messages, 0u);
  EXPECT_EQ(reserved.messages_accepted, 2u);
  EXPECT_EQ(reserved.bytes_accepted, normal_size + try_size);

  ioc.restart();
  ioc.poll();

  auto after = transport.stats();
  EXPECT_EQ(after.failed_sends, 0u);
  EXPECT_EQ(after.dropped_messages, 0u);
  EXPECT_EQ(after.messages_accepted, 2u);
  EXPECT_EQ(after.bytes_accepted, normal_size + try_size);
}

config::TcpClientConfig tcp_client_config(BackpressureStrategy strategy) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 9;
  cfg.retry_interval_ms = MIN_RETRY_INTERVAL_MS;
  cfg.connection_timeout_ms = MIN_CONNECTION_TIMEOUT_MS;
  cfg.backpressure_threshold = kBpHigh;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

std::shared_ptr<TcpClient> started_tcp_client(net::io_context& ioc, BackpressureStrategy strategy) {
  auto client = TcpClient::create(tcp_client_config(strategy), ioc);
  client->on_backpressure([](size_t) {});
  client->start();
  ioc.restart();
  ioc.poll_one();
  return client;
}

config::UdpConfig udp_config(BackpressureStrategy strategy) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  cfg.local_port = 0;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 9;
  cfg.backpressure_threshold = kBpHigh;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

std::shared_ptr<UdpChannel> started_udp_channel(net::io_context& ioc, BackpressureStrategy strategy) {
  auto channel = UdpChannel::create(udp_config(strategy), ioc);
  channel->on_backpressure([](size_t) {});
  channel->start();
  ioc.restart();
  ioc.poll();
  return channel;
}

config::UdsClientConfig uds_client_config(BackpressureStrategy strategy) {
  config::UdsClientConfig cfg;
  cfg.socket_path = "/tmp/wirestead-try-write-contract.sock";
  cfg.retry_interval_ms = MIN_RETRY_INTERVAL_MS;
  cfg.backpressure_threshold = kBpHigh;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

std::shared_ptr<UdsClient> started_uds_client(net::io_context& ioc, StallingUdsSocket** socket,
                                              BackpressureStrategy strategy) {
  auto fake = std::make_unique<StallingUdsSocket>(ioc);
  *socket = fake.get();
  auto client = UdsClient::create(uds_client_config(strategy), std::move(fake), ioc);
  client->on_backpressure([](size_t) {});
  client->start();
  poll_for(ioc, [&] { return client->is_connected(); });
  EXPECT_TRUE(client->is_connected());
  return client;
}

std::shared_ptr<UdsServerSession> started_uds_server_session(net::io_context& ioc, StallingUdsSocket** socket,
                                                             BackpressureStrategy strategy) {
  auto fake = std::make_unique<StallingUdsSocket>(ioc);
  *socket = fake.get();
  auto session = std::make_shared<UdsServerSession>(ioc, std::move(fake), kBpHigh, 0, strategy);
  session->on_backpressure([](size_t) {});
  session->start();
  ioc.restart();
  ioc.poll();
  EXPECT_TRUE(session->alive());
  return session;
}

config::SerialConfig serial_config(BackpressureStrategy strategy) {
  config::SerialConfig cfg;
  cfg.device = "/dev/wirestead-test";
  cfg.retry_interval_ms = MIN_RETRY_INTERVAL_MS;
  cfg.backpressure_threshold = kBpHigh;
  cfg.backpressure_strategy = strategy;
  return cfg;
}

std::shared_ptr<Serial> started_serial(net::io_context& ioc, StallingSerialPort** port, BackpressureStrategy strategy) {
  auto fake = std::make_unique<StallingSerialPort>(ioc);
  *port = fake.get();
  auto serial = Serial::create(serial_config(strategy), std::move(fake), ioc);
  serial->on_backpressure([](size_t) {});
  serial->start();
  poll_for(ioc, [&] { return serial->is_connected(); });
  EXPECT_TRUE(serial->is_connected());
  return serial;
}

template <typename Transport>
void activate_backpressure(Transport& transport, net::io_context& ioc) {
  ASSERT_TRUE(transport.async_write_move(std::vector<uint8_t>(kBpHigh, 0xD1)));
  poll_for(ioc, [&] { return transport.is_backpressure_active(); });
  ASSERT_TRUE(transport.is_backpressure_active());
}

void stop_uds_client(std::shared_ptr<UdsClient>& client, StallingUdsSocket* socket, net::io_context& ioc) {
  client->stop();
  ioc.restart();
  ioc.poll();
  while (socket && socket->pending_write_count() > 0) {
    socket->complete_one(make_error_code(net::error::operation_aborted));
    ioc.restart();
    ioc.poll();
  }
  client.reset();
}

void stop_uds_session(std::shared_ptr<UdsServerSession>& session, StallingUdsSocket* socket, net::io_context& ioc) {
  session->stop();
  ioc.restart();
  ioc.poll();
  while (socket && socket->pending_write_count() > 0) {
    socket->complete_one(make_error_code(net::error::operation_aborted));
    ioc.restart();
    ioc.poll();
  }
  session.reset();
}

void stop_serial(std::shared_ptr<Serial>& serial, StallingSerialPort* port, net::io_context& ioc) {
  serial->stop();
  ioc.restart();
  ioc.poll();
  while (port && port->pending_write_count() > 0) {
    port->complete_one(make_error_code(net::error::operation_aborted));
    ioc.restart();
    ioc.poll();
  }
  serial.reset();
}

}  // namespace

TEST(TryWriteTransportContractTest, TcpClientReliableTryWriteRejectsWithoutPending) {
  net::io_context ioc;
  auto client = started_tcp_client(ioc, BackpressureStrategy::Reliable);
  activate_backpressure(*client, ioc);

  expect_reliable_try_write_rejects_without_pending(*client);

  client->stop();
}

TEST(TryWriteTransportContractTest, UdpReliableTryWriteRejectsWithoutPending) {
  net::io_context ioc;
  auto channel = started_udp_channel(ioc, BackpressureStrategy::Reliable);
  ASSERT_TRUE(channel->async_try_write_move(std::vector<uint8_t>(kBpHigh, 0xD1)));

  expect_reliable_try_write_rejects_without_pending(*channel);

  channel->stop();
}

TEST(TryWriteTransportContractTest, UdsClientReliableTryWriteRejectsWithoutPending) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto client = started_uds_client(ioc, &socket, BackpressureStrategy::Reliable);
  activate_backpressure(*client, ioc);

  expect_reliable_try_write_rejects_without_pending(*client);

  stop_uds_client(client, socket, ioc);
}

TEST(TryWriteTransportContractTest, UdsServerSessionReliableTryWriteRejectsWithoutPending) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto session = started_uds_server_session(ioc, &socket, BackpressureStrategy::Reliable);
  activate_backpressure(*session, ioc);

  expect_reliable_try_write_rejects_without_pending(*session);

  stop_uds_session(session, socket, ioc);
}

TEST(TryWriteTransportContractTest, SerialReliableTryWriteRejectsWithoutPending) {
  net::io_context ioc;
  StallingSerialPort* port = nullptr;
  auto serial = started_serial(ioc, &port, BackpressureStrategy::Reliable);
  activate_backpressure(*serial, ioc);

  expect_reliable_try_write_rejects_without_pending(*serial);

  stop_serial(serial, port, ioc);
}

TEST(TryWriteTransportContractTest, TcpClientBestEffortTryWriteCountsDrop) {
  net::io_context ioc;
  auto client = started_tcp_client(ioc, BackpressureStrategy::BestEffort);
  activate_backpressure(*client, ioc);

  expect_best_effort_try_write_counts_drop(*client);

  client->stop();
}

TEST(TryWriteTransportContractTest, UdpBestEffortTryWriteCountsDrop) {
  net::io_context ioc;
  auto channel = started_udp_channel(ioc, BackpressureStrategy::BestEffort);
  ASSERT_TRUE(channel->async_try_write_move(std::vector<uint8_t>(kBpHigh, 0xD1)));

  expect_best_effort_try_write_counts_drop(*channel);

  channel->stop();
}

TEST(TryWriteTransportContractTest, UdsClientBestEffortTryWriteCountsDrop) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto client = started_uds_client(ioc, &socket, BackpressureStrategy::BestEffort);
  activate_backpressure(*client, ioc);

  expect_best_effort_try_write_counts_drop(*client);

  stop_uds_client(client, socket, ioc);
}

TEST(TryWriteTransportContractTest, UdsServerSessionBestEffortTryWriteCountsDrop) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto session = started_uds_server_session(ioc, &socket, BackpressureStrategy::BestEffort);
  activate_backpressure(*session, ioc);

  expect_best_effort_try_write_counts_drop(*session);

  stop_uds_session(session, socket, ioc);
}

TEST(TryWriteTransportContractTest, SerialBestEffortTryWriteCountsDrop) {
  net::io_context ioc;
  StallingSerialPort* port = nullptr;
  auto serial = started_serial(ioc, &port, BackpressureStrategy::BestEffort);
  activate_backpressure(*serial, ioc);

  expect_best_effort_try_write_counts_drop(*serial);

  stop_serial(serial, port, ioc);
}

TEST(TryWriteTransportContractTest, TcpClientTryWriteTrueReturnRemainsAccepted) {
  net::io_context ioc;
  auto client = started_tcp_client(ioc, BackpressureStrategy::Reliable);

  expect_try_write_true_return_remains_accepted(*client, ioc);

  client->stop();
}

TEST(TryWriteTransportContractTest, UdpTryWriteTrueReturnRemainsAccepted) {
  net::io_context ioc;
  auto channel = started_udp_channel(ioc, BackpressureStrategy::Reliable);

  expect_try_write_true_return_remains_accepted(*channel, ioc);

  channel->stop();
}

TEST(TryWriteTransportContractTest, UdsClientTryWriteTrueReturnRemainsAccepted) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto client = started_uds_client(ioc, &socket, BackpressureStrategy::Reliable);

  expect_try_write_true_return_remains_accepted(*client, ioc);

  stop_uds_client(client, socket, ioc);
}

TEST(TryWriteTransportContractTest, UdsServerSessionTryWriteTrueReturnRemainsAccepted) {
  net::io_context ioc;
  StallingUdsSocket* socket = nullptr;
  auto session = started_uds_server_session(ioc, &socket, BackpressureStrategy::Reliable);

  expect_try_write_true_return_remains_accepted(*session, ioc);

  stop_uds_session(session, socket, ioc);
}

TEST(TryWriteTransportContractTest, SerialTryWriteTrueReturnRemainsAccepted) {
  net::io_context ioc;
  StallingSerialPort* port = nullptr;
  auto serial = started_serial(ioc, &port, BackpressureStrategy::Reliable);

  expect_try_write_true_return_remains_accepted(*serial, ioc);

  stop_serial(serial, port, ioc);
}
