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
#include <boost/asio/ssl.hpp>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

namespace {

using wirestead::test::TestUtils;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = net::ip::tcp;

// Generated once per run rather than checked in: a committed key is a key
// somebody eventually reuses, and an expiry date is a test that breaks on a
// date nobody picked.
class SelfSignedCert {
 public:
  SelfSignedCert() {
    dir_ = std::filesystem::temp_directory_path() /
           ("wirestead-tls-" + std::to_string(::getpid()) + "-" + std::to_string(counter_++));
    std::filesystem::create_directories(dir_);
    const auto cmd = "openssl req -x509 -newkey rsa:2048 -keyout " + key().string() + " -out " + cert().string() +
                     " -days 1 -nodes -subj /CN=localhost >/dev/null 2>&1";
    ok_ = std::system(cmd.c_str()) == 0 && std::filesystem::exists(cert()) && std::filesystem::exists(key());
  }

  ~SelfSignedCert() {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  bool ok() const { return ok_; }
  std::filesystem::path cert() const { return dir_ / "cert.pem"; }
  std::filesystem::path key() const { return dir_ / "key.pem"; }

 private:
  static inline int counter_ = 0;
  std::filesystem::path dir_;
  bool ok_{false};
};

// A plain Asio TLS client. The point of testing against one rather than a
// second wirestead server is that the handshake has to satisfy something that
// is not this library.
std::string tls_round_trip(uint16_t port, const std::filesystem::path& ca, const std::string& payload) {
  net::io_context ioc;
  ssl::context ctx(ssl::context::tls_client);
  ctx.load_verify_file(ca.string());
  ctx.set_verify_mode(ssl::verify_peer);

  ssl::stream<tcp::socket> stream(ioc, ctx);
  stream.lowest_layer().connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  stream.handshake(ssl::stream_base::client);

  net::write(stream, net::buffer(payload));

  std::string reply(64, '\0');
  boost::system::error_code ec;
  const size_t n = stream.read_some(net::buffer(reply.data(), reply.size()), ec);
  reply.resize(n);

  boost::system::error_code ignored;
  stream.shutdown(ignored);
  return reply;
}

}  // namespace

TEST(TcpTlsLoopbackTest, ServesTlsAndRejectsPlaintextClients) {
  SelfSignedCert certs;
  if (!certs.ok()) {
    GTEST_SKIP() << "openssl CLI unavailable, cannot generate a test certificate";
  }

  const uint16_t port = TestUtils::getAvailableTestPort();

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->tls(certs.cert().string(), certs.key().string());

  std::atomic<int> messages{0};
  server->on_data([&](const wirestead::MessageContext& ctx) {
    messages.fetch_add(1);
    server->broadcast("pong");
  });
  server->on_error([](const wirestead::ErrorContext&) {});

  ASSERT_TRUE(server->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->listening(); }, 5000));

  EXPECT_EQ(tls_round_trip(port, certs.cert(), "ping"), "pong");
  EXPECT_EQ(messages.load(), 1);

  // A plaintext client must not get through. Its bytes are not a valid
  // ClientHello, so the handshake fails and the session closes without ever
  // reaching on_data - the failure that matters is data being served in the
  // clear, so assert the callback never fires.
  {
    net::io_context ioc;
    tcp::socket plain(ioc);
    boost::system::error_code ec;
    plain.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port), ec);
    if (!ec) {
      net::write(plain, net::buffer(std::string("plaintext-should-not-work")), ec);
      std::string buf(16, '\0');
      plain.read_some(net::buffer(buf.data(), buf.size()), ec);
    }
    plain.close(ec);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(messages.load(), 1) << "a plaintext client reached the data callback";

  server->stop();
}

// The point of client TLS: both ends are wirestead, and the traffic between
// them is encrypted. Until this existed a wirestead client could only reach a
// wirestead TLS server by not being a wirestead client.
TEST(TcpTlsLoopbackTest, WiresteadClientTalksToWiresteadTlsServer) {
  SelfSignedCert certs;
  if (!certs.ok()) {
    GTEST_SKIP() << "openssl CLI unavailable, cannot generate a test certificate";
  }

  const uint16_t port = TestUtils::getAvailableTestPort();

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->tls(certs.cert().string(), certs.key().string());
  server->on_data([&](const wirestead::MessageContext& ctx) { server->broadcast("pong-tls"); });
  server->on_error([](const wirestead::ErrorContext&) {});
  ASSERT_TRUE(server->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->listening(); }, 5000));

  // The certificate is issued to "localhost", so the client has to connect by
  // that name for host name verification to pass.
  std::atomic<int> replies{0};
  std::string received;
  auto client = std::make_shared<wirestead::wrapper::TcpClient>("localhost", port);
  client->tls(certs.cert().string());
  client->on_data([&](const wirestead::MessageContext& ctx) {
    received.assign(ctx.data());
    replies.fetch_add(1);
  });
  client->on_error([](const wirestead::ErrorContext&) {});

  ASSERT_TRUE(client->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return client->connected(); }, 10000));

  ASSERT_TRUE(client->send("ping-tls"));
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return replies.load() > 0; }, 10000));
  EXPECT_EQ(received, "pong-tls");

  client->stop();
  server->stop();
}

// Verification is the whole point of the client half. Trusting nothing but the
// system store, a self-signed server must fail rather than connect.
TEST(TcpTlsLoopbackTest, ClientRejectsAnUntrustedServerCertificate) {
  SelfSignedCert certs;
  if (!certs.ok()) {
    GTEST_SKIP() << "openssl CLI unavailable, cannot generate a test certificate";
  }

  const uint16_t port = TestUtils::getAvailableTestPort();

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->tls(certs.cert().string(), certs.key().string());
  std::atomic<int> served{0};
  server->on_data([&](const wirestead::MessageContext&) { served.fetch_add(1); });
  server->on_error([](const wirestead::ErrorContext&) {});
  ASSERT_TRUE(server->start().get());
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->listening(); }, 5000));

  auto client = std::make_shared<wirestead::wrapper::TcpClient>("localhost", port);
  client->tls();  // system trust store only - our self-signed cert is not in it
  client->on_data([](const wirestead::MessageContext&) {});
  client->on_error([](const wirestead::ErrorContext&) {});
  client->start();

  // Never reports itself connected, and nothing it sends reaches the server.
  EXPECT_FALSE(TestUtils::waitForCondition([&] { return client->connected(); }, 3000));
  EXPECT_EQ(served.load(), 0);

  client->stop();
  server->stop();
}

// Half a TLS config used to leave tls_enabled() false, which meant the server
// came up in plaintext without a word - an empty env var or a typo away from
// serving unencrypted traffic to a caller who asked for TLS.
TEST(TcpTlsLoopbackTest, InvalidTlsPairIsRejectedAndPreservesPreviousSettings) {
  SelfSignedCert certs;
  if (!certs.ok()) {
    GTEST_SKIP() << "openssl CLI unavailable, cannot generate a test certificate";
  }
  for (const auto& [cert, key] :
       std::vector<std::pair<std::string, std::string>>{{certs.cert().string(), ""}, {"", certs.key().string()}}) {
    const auto port = TestUtils::getAvailableTestPort();
    auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
    server->tls(certs.cert().string(), certs.key().string());
    EXPECT_THROW(server->tls(cert, key), std::invalid_argument);
    server->on_data([&](const wirestead::MessageContext&) { (void)server->broadcast("pong"); });
    ASSERT_TRUE(server->start().get());
    EXPECT_EQ(tls_round_trip(port, certs.cert(), "ping"), "pong");
    server->stop();
  }
}

TEST(TcpTlsLoopbackTest, StartFailsWhenTheCertificateCannotBeLoaded) {
  const uint16_t port = TestUtils::getAvailableTestPort();

  auto server = std::make_shared<wirestead::wrapper::TcpServer>(port);
  server->tls("/nonexistent/cert.pem", "/nonexistent/key.pem");
  server->on_error([](const wirestead::ErrorContext&) {});

  // Coming up in plaintext because the certificate was unreadable is the one
  // outcome that must never happen.
  EXPECT_FALSE(server->start().get());
  EXPECT_FALSE(server->listening());

  server->stop();
}
