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
#include <chrono>
#include <deque>
#include <functional>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "unilink/base/constants.hpp"
#include "unilink/builder/tcp_client_builder.hpp"
#include "unilink/builder/udp_builder.hpp"
#include "unilink/builder/uds_builder.hpp"
#include "unilink/config/tcp_client_config.hpp"
#include "unilink/config/tcp_server_config.hpp"
#include "unilink/config/udp_config.hpp"
#include "unilink/config/uds_config.hpp"
#include "unilink/interface/itcp_socket.hpp"
#include "unilink/transport/tcp_client/tcp_client.hpp"
#include "unilink/transport/tcp_server/tcp_server.hpp"
#include "unilink/transport/tcp_server/tcp_server_session.hpp"
#include "unilink/transport/udp/udp.hpp"
#include "unilink/transport/uds/uds_client.hpp"
#include "unilink/transport/uds/uds_server.hpp"

using namespace unilink;
using namespace unilink::transport;
using namespace unilink::base::constants;
using namespace std::chrono_literals;
namespace net = boost::asio;

// ─── Reliable: default strategy preserves data until hard limit ────────────────

TEST(BackpressureStrategyTest, ReliableIsDefaultInConfig) {
  config::TcpClientConfig cfg;
  EXPECT_EQ(cfg.backpressure_strategy, BackpressureStrategy::Reliable);
}

TEST(BackpressureStrategyTest, BestEffortRoundtripsInConfig) {
  config::TcpClientConfig cfg;
  cfg.backpressure_strategy = BackpressureStrategy::BestEffort;
  EXPECT_EQ(cfg.backpressure_strategy, BackpressureStrategy::BestEffort);
}

TEST(BackpressureStrategyTest, TcpServerConfigDefaultIsReliable) {
  config::TcpServerConfig cfg;
  EXPECT_EQ(cfg.backpressure_strategy, BackpressureStrategy::Reliable);
}

// ─── BestEffort: queue is cleared when threshold is exceeded ──────────────────

TEST(BackpressureStrategyTest, BestEffort_BackpressureCallbackFiredAndQueueCleared) {
  // Stand up a real loopback server + client pair to exercise the write path.
  constexpr uint16_t kPort = 19801;
  constexpr size_t kThreshold = 1024;  // 1 KB

  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  std::thread io_thread([&] { ioc.run(); });

  // Server (just accepts connection)
  config::TcpServerConfig srv_cfg;
  srv_cfg.port = kPort;
  auto server = TcpServer::create(srv_cfg);
  server->start();

  // Client with BestEffort strategy and small threshold
  config::TcpClientConfig cli_cfg;
  cli_cfg.host = "127.0.0.1";
  cli_cfg.port = kPort;
  cli_cfg.backpressure_threshold = kThreshold;
  cli_cfg.backpressure_strategy = BackpressureStrategy::BestEffort;

  auto client = TcpClient::create(cli_cfg, ioc);

  std::atomic<int> bp_count{0};
  client->on_backpressure([&](size_t) { bp_count.fetch_add(1); });

  client->start();

  // Wait for connection
  std::this_thread::sleep_for(200ms);

  // Flood with large messages to blow past the threshold
  std::vector<uint8_t> big(kThreshold * 2, 0xAB);
  for (int i = 0; i < 10; ++i) {
    client->async_write_copy(memory::ConstByteSpan(big.data(), big.size()));
  }

  std::this_thread::sleep_for(100ms);

  client->stop();
  server->stop();

  work.reset();
  ioc.stop();
  io_thread.join();

  // With BestEffort, backpressure must have fired at least once (queue was
  // flushed each time the threshold was exceeded).
  EXPECT_GE(bp_count.load(), 1);
}

// ─── set_backpressure_strategy: runtime change takes effect ──────────────────

TEST(BackpressureStrategyTest, SetBackpressureStrategyChangesMode) {
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 19802;
  cfg.backpressure_strategy = BackpressureStrategy::Reliable;

  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  std::thread io_thread([&] { ioc.run(); });

  auto client = TcpClient::create(cfg, ioc);

  // Switch to BestEffort at runtime — must not crash
  client->set_backpressure_strategy(BackpressureStrategy::BestEffort);

  // Basic smoke: queuing without a connection should not crash
  std::vector<uint8_t> data(512, 0xFF);
  client->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));
  std::this_thread::sleep_for(50ms);

  work.reset();
  ioc.stop();
  io_thread.join();
}

// ─── UdsClient: report_backpressure uses hysteresis (ON/OFF transitions only) ─

TEST(BackpressureStrategyTest, UdsClient_ReportBackpressureHysteresis) {
  constexpr size_t kThreshold = 1024;
  auto tmp_path = test::TestUtils::makeUniqueUdsSocketPath("bp_hys");
  test::TestUtils::removeFileIfExists(tmp_path);

  // Start a UDS server to accept the connection (manages its own ioc)
  config::UdsServerConfig srv_cfg;
  srv_cfg.socket_path = tmp_path.string();
  auto server = transport::UdsServer::create(srv_cfg);
  server->start();
  std::this_thread::sleep_for(50ms);

  // Client with small threshold (manages its own ioc)
  config::UdsClientConfig cli_cfg;
  cli_cfg.socket_path = tmp_path.string();
  cli_cfg.backpressure_threshold = kThreshold;
  cli_cfg.backpressure_strategy = BackpressureStrategy::BestEffort;

  auto client = transport::UdsClient::create(cli_cfg);

  std::vector<size_t> bp_events;
  std::mutex bp_mtx;
  client->on_backpressure([&](size_t queued) {
    std::lock_guard<std::mutex> lock(bp_mtx);
    bp_events.push_back(queued);
  });

  client->start();
  std::this_thread::sleep_for(100ms);

  // Flood to trigger ON transition
  std::vector<uint8_t> big(kThreshold * 2, 0xAB);
  for (int i = 0; i < 5; ++i) {
    client->async_write_copy(memory::ConstByteSpan(big.data(), big.size()));
  }
  std::this_thread::sleep_for(100ms);

  client->stop();
  server->stop();
  test::TestUtils::removeFileIfExists(tmp_path);

  // Hysteresis: with BestEffort and flooding, must fire at least once
  std::lock_guard<std::mutex> lock(bp_mtx);
  EXPECT_GE(bp_events.size(), 1u);
}

// ─── Builder: backpressure_strategy/threshold fluent API ──────────────────────

TEST(BackpressureStrategyTest, BuilderFluentBackpressureStrategy) {
  auto client = builder::TcpClientBuilder("127.0.0.1", 19810)
                    .backpressure_strategy(BackpressureStrategy::BestEffort)
                    .backpressure_threshold(512 * 1024)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  ASSERT_NE(client, nullptr);
}

TEST(BackpressureStrategyTest, UdsBuilderFluentBackpressureStrategy) {
  auto tmp_path = test::TestUtils::makeUniqueUdsSocketPath("bp_builder");
  auto client = builder::UdsClientBuilder(tmp_path.string())
                    .backpressure_strategy(BackpressureStrategy::BestEffort)
                    .backpressure_threshold(256 * 1024)
                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();
  ASSERT_NE(client, nullptr);
}

TEST(BackpressureStrategyTest, UdpBuilderFluentBackpressureStrategy) {
  auto udp = builder::UdpClientBuilder(0)
                 .backpressure_strategy(BackpressureStrategy::BestEffort)
                 .backpressure_threshold(128 * 1024)
                 .on_data([](auto&&) {})
                 .on_error([](auto&&) {})
                 .build();
  ASSERT_NE(udp, nullptr);
}

// ─── UdpChannel: set_backpressure_strategy runtime setter ────────────────────

TEST(BackpressureStrategyTest, UdpChannel_SetBackpressureStrategyRuntime) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  auto channel = transport::UdpChannel::create(cfg);
  ASSERT_NE(channel, nullptr);

  // Must not crash when called before start
  channel->set_backpressure_strategy(BackpressureStrategy::BestEffort);
  channel->set_backpressure_strategy(BackpressureStrategy::Reliable);
}

// ─── StallingTcpSocket: test double that holds write completions ──────────────
// async_write stores the handler without completing it.  complete_one() posts
// the completion to the ioc so ioc.poll() drives on_write on the strand.
// Tests must use net::make_work_guard(ioc) + ioc.poll() — not ioc.run_for() —
// because run_for() marks the ioc stopped when outstanding_work_==0, causing
// subsequent calls to silently skip all posted handlers.

namespace {
class StallingTcpSocket : public unilink::interface::TcpSocketInterface {
 public:
  struct WriteEntry {
    size_t size;
    std::function<void(const boost::system::error_code&, std::size_t)> handler;
  };

  explicit StallingTcpSocket(net::io_context& ioc) : ioc_(ioc) {}

  void async_read_some(const net::mutable_buffer&,
                       std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    read_handler_ = std::move(handler);
  }

  void async_write(const net::const_buffer& buf,
                   std::function<void(const boost::system::error_code&, std::size_t)> handler) override {
    write_queue_.push_back({buf.size(), std::move(handler)});
  }

  bool complete_one(boost::system::error_code ec = {}) {
    if (write_queue_.empty()) return false;
    auto entry = std::move(write_queue_.front());
    write_queue_.pop_front();
    total_bytes_completed_ += entry.size;
    net::post(ioc_, [h = std::move(entry.handler), ec, n = entry.size]() mutable { h(ec, n); });
    return true;
  }

  size_t total_bytes_completed() const { return total_bytes_completed_; }
  size_t pending_write_count() const { return write_queue_.size(); }

  void shutdown(tcp::socket::shutdown_type, boost::system::error_code& ec) override { ec.clear(); }

  void close(boost::system::error_code& ec) override {
    if (read_handler_) {
      auto h = std::move(read_handler_);
      net::post(ioc_, [h = std::move(h)]() mutable { h(boost::asio::error::operation_aborted, 0); });
    }
    ec.clear();
  }

  tcp::endpoint remote_endpoint(boost::system::error_code& ec) const override {
    ec.clear();
    return tcp::endpoint(net::ip::make_address("127.0.0.1"), 12345);
  }

 private:
  net::io_context& ioc_;
  std::function<void(const boost::system::error_code&, std::size_t)> read_handler_;
  std::deque<WriteEntry> write_queue_;
  size_t total_bytes_completed_ = 0;
};
}  // namespace

// drain_and_stop: stops the session and completes any stalled writes to break
// the shared_ptr cycle between the session and the write handlers it owns.
static void drain_and_stop(StallingTcpSocket* sock, std::shared_ptr<transport::TcpServerSession> session,
                           net::io_context& ioc) {
  session->stop();
  ioc.poll();
  while (sock->pending_write_count() > 0) {
    sock->complete_one();
    ioc.poll();
  }
}

// ─── Reliable auto-FC: pending_ queue tests ───────────────────────────────────

// Writes during active backpressure go to pending_ and return true (not dropped).
TEST(BackpressureStrategyTest, Reliable_PendingQueue_AccumulatesWhenBackpressureActive) {
  constexpr size_t kBpHigh = 1024;
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);  // prevents ioc from stopping between polls

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  std::vector<size_t> bp_events;
  session->on_backpressure([&](size_t q) { bp_events.push_back(q); });
  session->start();
  ioc.poll();

  // chunk1 (600 B) → tx_, socket stalls (no completion yet)
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAA)));
  ioc.poll();

  // chunk2 (600 B) → tx_, queue=1200 > bp_high_=1024 → BP ON fires
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAB)));
  ioc.poll();
  ASSERT_EQ(bp_events.size(), 1u);
  EXPECT_GE(bp_events[0], kBpHigh);
  EXPECT_TRUE(session->is_backpressure_active());

  // chunk3 during active BP → Reliable: must return true and go to pending_
  bool result = session->async_write_move(std::vector<uint8_t>(300, 0xAC));
  ioc.poll();
  EXPECT_TRUE(result);
  EXPECT_EQ(bp_events.size(), 1u);  // no new BP event; chunk3 is buffered
  EXPECT_TRUE(session->is_backpressure_active());

  work.reset();
  drain_and_stop(sock, session, ioc);
}

TEST(BackpressureStrategyTest, Reliable_TryWriteRejectsWhenBackpressureActiveWithoutPending) {
  constexpr size_t kBpHigh = 1024;
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  session->on_backpressure([](size_t) {});
  session->start();
  ioc.poll();

  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAA)));
  ioc.poll();
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAB)));
  ioc.poll();
  ASSERT_TRUE(session->is_backpressure_active());

  auto before_try = session->stats();
  EXPECT_EQ(before_try.pending_bytes, 0u);

  std::vector<uint8_t> copy_payload(300, 0xAC);
  EXPECT_FALSE(session->async_try_write_copy(memory::ConstByteSpan(copy_payload.data(), copy_payload.size())));
  EXPECT_FALSE(session->async_try_write_move(std::vector<uint8_t>(300, 0xAD)));
  EXPECT_FALSE(session->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(300, 0xAE)));
  ioc.poll();

  auto after_try = session->stats();
  EXPECT_EQ(after_try.pending_bytes, before_try.pending_bytes);
  EXPECT_EQ(after_try.failed_sends, before_try.failed_sends + 3);

  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(300, 0xAF)));
  ioc.poll();
  EXPECT_EQ(session->stats().pending_bytes, before_try.pending_bytes + 300);

  work.reset();
  drain_and_stop(sock, session, ioc);
}

TEST(BackpressureStrategyTest, BestEffort_TryWriteRejectsWhenBackpressureActiveAndCountsDrop) {
  constexpr size_t kBpHigh = 1024;
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session = std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0,
                                                               BackpressureStrategy::BestEffort);

  session->on_backpressure([](size_t) {});
  session->start();
  ioc.poll();

  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(kBpHigh, 0xAA)));
  ioc.poll();
  ASSERT_TRUE(session->is_backpressure_active());

  auto before_try = session->stats();
  EXPECT_FALSE(session->async_try_write_move(std::vector<uint8_t>(1, 0xAB)));
  ioc.poll();

  auto after_try = session->stats();
  EXPECT_EQ(after_try.pending_bytes, before_try.pending_bytes);
  EXPECT_EQ(after_try.dropped_messages, before_try.dropped_messages + 1);
  EXPECT_EQ(after_try.dropped_bytes, before_try.dropped_bytes + 1);

  work.reset();
  drain_and_stop(sock, session, ioc);
}

TEST(BackpressureStrategyTest, TryWritePressureRaceTrueReturnRemainsAccepted) {
  constexpr size_t kBpHigh = 1024;
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  session->on_backpressure([](size_t) {});
  session->start();
  ioc.poll();

  ASSERT_TRUE(session->async_write_move(std::vector<uint8_t>(900, 0xAA)));
  ASSERT_TRUE(session->async_try_write_move(std::vector<uint8_t>(200, 0xAB)));

  auto reserved = session->stats();
  EXPECT_EQ(reserved.pending_bytes, 0u);
  EXPECT_EQ(reserved.failed_sends, 0u);
  EXPECT_EQ(reserved.messages_accepted, 2u);

  ioc.poll();

  auto accepted = session->stats();
  EXPECT_TRUE(session->is_backpressure_active());
  EXPECT_EQ(accepted.pending_bytes, 0u);
  EXPECT_EQ(accepted.failed_sends, 0u);
  EXPECT_EQ(accepted.dropped_messages, 0u);
  EXPECT_EQ(accepted.messages_accepted, 2u);
  EXPECT_EQ(accepted.bytes_accepted, 1100u);
  EXPECT_EQ(accepted.queued_bytes, 1100u);

  work.reset();
  drain_and_stop(sock, session, ioc);
}

// All bytes written to pending_ during backpressure are eventually delivered.
TEST(BackpressureStrategyTest, Reliable_PendingQueue_DeliveredAfterBackpressureClears) {
  constexpr size_t kBpHigh = 1024;
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  session->on_backpressure([](size_t) {});
  session->start();
  ioc.poll();

  // Establish BP ON: chunk1 in-flight (stalled), chunk2 queued → queue=1200
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAA)));
  ioc.poll();
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAB)));
  ioc.poll();
  EXPECT_TRUE(session->is_backpressure_active());

  // chunk3 goes to pending_ (300 B)
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(300, 0xAC)));
  ioc.poll();

  // Complete chunk1: queue=600, still above bp_low_(512), no OFF yet
  EXPECT_TRUE(sock->complete_one());
  ioc.poll();

  // Complete chunk2: queue=0 <= bp_low_(512) → OFF fires, pending_ flushed to tx_
  EXPECT_TRUE(sock->complete_one());
  ioc.poll();

  // chunk3 is now in tx_ (moved from pending_), complete it
  EXPECT_TRUE(sock->complete_one());
  ioc.poll();

  // All three chunks must have reached the socket
  EXPECT_EQ(sock->total_bytes_completed(), 600u + 600u + 300u);

  work.reset();
  drain_and_stop(sock, session, ioc);
}

// async_write returns false when queue_bytes_ + pending_bytes_ + new > bp_limit_.
TEST(BackpressureStrategyTest, Reliable_CombinedLimit_LargeWriteRejected) {
  constexpr size_t kBpHigh = 1024;
  // bp_limit_ = min(max(kBpHigh*4, DEFAULT_BACKPRESSURE_THRESHOLD), MAX_BUFFER_SIZE)
  //           = min(max(4096, 1MiB), 64MiB) = 1 MiB
  constexpr size_t kBpLimit = base::constants::DEFAULT_BACKPRESSURE_THRESHOLD;  // 1 MiB
  constexpr size_t kHalfLimit = kBpLimit / 2;                                   // 512 KiB

  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  session->on_backpressure([](size_t) {});
  session->start();
  ioc.poll();

  // Establish BP ON
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAA)));
  ioc.poll();
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAB)));
  ioc.poll();
  EXPECT_TRUE(session->is_backpressure_active());

  // Fill pending_ with ~512 KiB: queue(1200) + pending(0) + 512K < 1MiB → accepted
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(kHalfLimit, 0xAC)));
  ioc.poll();  // pending_bytes_ updated to 512K on strand

  // Second 512 KiB: queue(1200) + pending(512K) + 512K > 1MiB → rejected
  bool result = session->async_write_move(std::vector<uint8_t>(kHalfLimit, 0xAD));
  EXPECT_FALSE(result);

  work.reset();
  drain_and_stop(sock, session, ioc);
}

// Reproduces jwsung91/unilink#517: the plain (blocking-capable) async_write_*
// path used to precheck queue_bytes_+pending_bytes_+added>bp_limit_ on the
// caller's own thread without reserving the bytes, then re-decide on the
// strand once the write was actually routed. Concurrent callers could all
// pass the precheck, and once routed the real combined total exceeded
// bp_limit_, so some already-`accepted` messages got silently reclassified
// as `dropped` - a real-world 0.05%-0.7% loss confirmed on Jetson benchmark
// data for Reliable strategy, which must never drop. try_reserve_limit_bytes()
// closes this by reserving atomically at accept time; this test drives many
// concurrent writers through a real io_context/strand (not manual ioc.poll()
// stepping) so the accept-time precheck and the strand's routing genuinely
// overlap across threads, and asserts every message the precheck accepted
// survives routing.
TEST(BackpressureStrategyTest, Reliable_ConcurrentPlainWritesNeverDropAcceptedMessages) {
  constexpr size_t kBpHigh = 1024;
  constexpr int kThreadCount = 8;
  constexpr int kMessagesPerThread = 3000;
  constexpr size_t kPayload = 100;  // 8*3000*100 = 2.4 MB requested, ~2.4x bp_limit_ (1 MiB)

  net::io_context ioc;
  auto work = net::make_work_guard(ioc);
  std::thread io_thread([&] { ioc.run(); });

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);
  session->on_backpressure([](size_t) {});
  session->start();

  std::atomic<int> accepted_count{0};
  std::vector<std::thread> writers;
  writers.reserve(kThreadCount);
  for (int t = 0; t < kThreadCount; ++t) {
    writers.emplace_back([&] {
      for (int i = 0; i < kMessagesPerThread; ++i) {
        if (session->async_write_move(std::vector<uint8_t>(kPayload, 0xAB))) {
          accepted_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& th : writers) th.join();

  // route_enqueued_buffer() runs on the session's strand inside io_thread;
  // wait for the posted-write backlog to fully drain (queued_bytes_ +
  // pending_bytes_ + dropped_bytes stops changing) before reading final
  // stats, rather than a single fixed sleep.
  size_t stable_bytes = 0;
  int stable_iterations = 0;
  for (int i = 0; i < 500 && stable_iterations < 5; ++i) {
    std::this_thread::sleep_for(2ms);
    auto s = session->stats();
    const size_t current = s.queued_bytes + s.pending_bytes + s.dropped_bytes;
    if (current == stable_bytes) {
      ++stable_iterations;
    } else {
      stable_bytes = current;
      stable_iterations = 0;
    }
  }

  auto stats = session->stats();
  EXPECT_EQ(stats.messages_accepted, static_cast<uint64_t>(accepted_count.load()));
  EXPECT_EQ(stats.dropped_messages, 0u) << stats.dropped_messages << " of " << accepted_count.load()
                                        << " accepted messages were dropped after routing (jwsung91/unilink#517)";

  work.reset();
  drain_and_stop(sock, session, ioc);
  ioc.stop();
  io_thread.join();
}

// When OFF fires and the flushed pending_ bytes push queue above bp_high_,
// the ON callback is re-fired immediately in the same report_backpressure call.
TEST(BackpressureStrategyTest, Reliable_BurstOnOFF_ImmediateOnReactivation) {
  constexpr size_t kBpHigh = 1024;
  constexpr size_t kBpLow = kBpHigh / 2;  // 512
  net::io_context ioc;
  auto work = net::make_work_guard(ioc);

  auto socket = std::make_unique<StallingTcpSocket>(ioc);
  auto* sock = socket.get();
  auto session =
      std::make_shared<transport::TcpServerSession>(ioc, std::move(socket), kBpHigh, 0, BackpressureStrategy::Reliable);

  std::vector<size_t> bp_events;
  session->on_backpressure([&](size_t q) { bp_events.push_back(q); });
  session->start();
  ioc.poll();

  // chunk1 (600 B) in-flight (stalled), chunk2 (600 B) in tx_ → queue=1200 → ON
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAA)));
  ioc.poll();
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(600, 0xAB)));
  ioc.poll();
  ASSERT_EQ(bp_events.size(), 1u);
  EXPECT_GE(bp_events[0], kBpHigh);

  // big_chunk (2000 B) in pending_; post-flush queue will be 2000 > kBpHigh → burst
  EXPECT_TRUE(session->async_write_move(std::vector<uint8_t>(2000, 0xAC)));
  ioc.poll();

  // Complete chunk1: queue=1200-600=600, 600 > kBpLow(512) → no OFF
  EXPECT_TRUE(sock->complete_one());
  ioc.poll();
  EXPECT_EQ(bp_events.size(), 1u);

  // Complete chunk2: queue=600-600=0, 0 <= kBpLow → OFF fires,
  // pending_ (2000 B) flushed to tx_, queue=2000 >= kBpHigh → ON re-fires
  EXPECT_TRUE(sock->complete_one());
  ioc.poll();

  ASSERT_EQ(bp_events.size(), 3u);
  EXPECT_GE(bp_events[0], kBpHigh);  // first ON
  EXPECT_LE(bp_events[1], kBpLow);   // OFF (value is pre-flush queue size)
  EXPECT_GE(bp_events[2], kBpHigh);  // ON re-fired (burst-on-OFF)

  work.reset();
  drain_and_stop(sock, session, ioc);
}
