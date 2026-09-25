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
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "test_connection_channel.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_client/tcp_client.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/callback_guard.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace {

class Signal {
 public:
  void notify() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_ = true;
    cv_.notify_all();
  }
  bool wait(std::chrono::milliseconds limit = 5s) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, limit, [this] { return set_; });
  }
  void hold() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return set_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool set_ = false;
};

struct OnExit {
  std::function<void()> action;
  ~OnExit() { action(); }
};

struct Context {
  std::shared_ptr<boost::asio::io_context> io = std::make_shared<boost::asio::io_context>();
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work{io->get_executor()};
  std::thread runner{[this] { io->run(); }};
  ~Context() {
    work.reset();
    io->stop();
    runner.join();
  }
};

// This channel preserves snapshots without borrowing an actual TCP strand.
// A parked wrapper admission must not also prevent transport cleanup.
class SavedChannel : public wirestead::test::TestConnectionChannel {
 public:
  void start() override {
    connected_ = true;
    connection_opened();
  }
  void stop() override {
    connected_ = false;
    connection_lost();
  }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override { return io_.get_executor(); }
  SendResult async_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan) override { return SendResult::accept(); }
  SendResult async_try_write_move_result(std::vector<uint8_t>&&) override { return SendResult::accept(); }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    return SendResult::accept();
  }
  void on_bytes(OnBytes cb) override { bytes = std::move(cb); }
  void on_state(OnState cb) override { state = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { bp = std::move(cb); }
  OnBytes bytes;
  OnState state;
  OnBackpressure bp;

 private:
  boost::asio::io_context io_;
  std::atomic<bool> connected_{false};
};

struct AdmissionPark {
  Signal entered, release;
  std::atomic<bool> armed{true};
};
std::atomic<AdmissionPark*> admission_park{nullptr};
void park_admission() {
  auto* park = admission_park.load();
  if (park && park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

// Both wrappers are exercised; the injected server exposes state/error
// callbacks, while real session data is covered by the loopback tests.
class TcpStopAdmissionTest : public ::testing::TestWithParam<bool> {};

TEST_P(TcpStopAdmissionTest, ParkedOldHandlerIsRefusedAfterStopAndRestart) {
  auto channel = std::make_shared<SavedChannel>();
  auto client = GetParam() ? nullptr : std::make_shared<wrapper::TcpClient>(channel);
  auto server = GetParam() ? std::make_shared<wrapper::TcpServer>(channel) : nullptr;
  std::atomic<int> delivered{0};
  if (client) client->on_data([&](const auto&) { ++delivered; });
  if (server) server->on_error([&](const auto&) { ++delivered; });
  auto start = [&] {
    auto ready = client ? client->start() : server->start();
    channel->state(client ? base::LinkState::Connected : base::LinkState::Listening);
    EXPECT_EQ(ready.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(ready.get());
  };
  auto stop = [&] {
    if (client)
      client->stop();
    else
      server->stop();
  };
  auto snapshot = [&]() -> std::function<void()> {
    if (client) {
      auto bytes = channel->bytes;
      return [bytes] {
        const uint8_t data[] = {42};
        bytes(memory::ConstByteSpan(data, 1));
      };
    }
    auto state = channel->state;
    return [state] { state(base::LinkState::Error); };
  };
  AdmissionPark park;
  std::future<void> pending;
  OnExit cleanup{[&] {
    park.release.notify();
    if (pending.valid()) pending.wait();
    wrapper::detail::g_pre_admission_hook.store(nullptr);
    admission_park.store(nullptr);
    stop();
  }};
  start();
  auto old = snapshot();
  admission_park.store(&park);
  wrapper::detail::g_pre_admission_hook.store(&park_admission);
  pending = std::async(std::launch::async, old);
  ASSERT_TRUE(park.entered.wait());
  stop();
  // Stop completed while the old handler retained a liveness reference.
  start();
  auto fresh = snapshot();
  park.release.notify();
  pending.get();
  EXPECT_EQ(delivered.load(), 0);
  old();
  EXPECT_EQ(delivered.load(), 0);
  fresh();
  EXPECT_EQ(delivered.load(), 1);
}

struct CleanupPark {
  Signal entered, release;
  std::atomic<int> waiters{0};
  std::atomic<bool> armed{true};
};
std::atomic<CleanupPark*> cleanup_park{nullptr};
void park_cleanup(const void*, bool completing) {
  auto* park = cleanup_park.load();
  if (!park) return;
  if (!completing) {
    ++park->waiters;
  } else if (park->armed.exchange(false)) {
    park->entered.notify();
    park->release.hold();
  }
}

std::shared_ptr<interface::Channel> make_transport(bool server, boost::asio::io_context& io) {
  if (server) {
    config::TcpServerConfig cfg;
    cfg.port = test::TestUtils::getAvailableTestPort();
    return transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io);
  }
  config::TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = test::TestUtils::getAvailableTestPort();
  cfg.max_retries = 0;
  return transport::TcpClient::create(cfg, io);
}

TEST_P(TcpStopAdmissionTest, BothOutsideStopsWaitUntilCleanupActuallyCompletes) {
  Context context;
  auto channel = make_transport(GetParam(), *context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  Signal started;
  boost::asio::post(channel->get_executor(), [&] { started.notify(); });
  ASSERT_TRUE(started.wait());
  cleanup_park.store(&park);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return park.waiters.load() >= 2; }, 1000));
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
  channel->stop();
  // A completed stop also permits the SAME transport instance to restart.
  channel->start();
  Signal restarted;
  boost::asio::post(channel->get_executor(), [&] { restarted.notify(); });
  ASSERT_TRUE(restarted.wait());
  channel->stop();
}

TEST_P(TcpStopAdmissionTest, StopDoesNotRunUnrelatedReadyHandlers) {
  Context context;
  auto channel = make_transport(GetParam(), *context.io);
  Signal occupied, release, marker;
  CleanupPark observed;
  observed.armed = false;
  std::atomic<std::thread::id> ran_on{};
  std::future<void> stopper;
  OnExit cleanup{[&] {
    release.notify();
    if (stopper.valid()) stopper.wait();
    transport::detail::g_stop_test_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  channel->start();
  boost::asio::post(channel->get_executor(), [&] {
    occupied.notify();
    release.hold();
  });
  ASSERT_TRUE(occupied.wait());
  boost::asio::post(*context.io, [&] {
    ran_on = std::this_thread::get_id();
    marker.notify();
  });
  cleanup_park.store(&observed);
  transport::detail::g_stop_test_hook.store(&park_cleanup);
  stopper = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_TRUE(test::TestUtils::waitForCondition([&] { return observed.waiters.load() == 1; }, 1000));
  EXPECT_FALSE(marker.wait(100ms));
  EXPECT_EQ(stopper.wait_for(100ms), std::future_status::timeout);
  release.notify();
  stopper.get();
  ASSERT_TRUE(marker.wait());
  EXPECT_EQ(ran_on.load(), context.runner.get_id());
}

TEST_P(TcpStopAdmissionTest, NeverStartedStopNeedsNoExecutorAndRetainsNoWork) {
  boost::asio::io_context io;
  auto channel = make_transport(GetParam(), io);
  std::weak_ptr<interface::Channel> weak = channel;
  channel->stop();
  channel->stop();
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}

// Validation can fail before any I/O work is dispatched. This must not
// wait on an owned thread that was never created or an unused external context.
TEST(TcpFailedStartStopTest, ValidationFailureNeedsNoExecutorForCleanup) {
  for (bool external : {false, true}) {
    boost::asio::io_context io;
    config::TcpServerConfig cfg;
    cfg.port = test::TestUtils::getAvailableTestPort();
    cfg.tls_certificate_file = "unused-certificate";
    // A certificate without a key is invalid even in builds with TLS off.
    auto server = external ? transport::TcpServer::create(cfg, std::make_unique<transport::BoostTcpAcceptor>(io), io)
                           : transport::TcpServer::create(cfg);
    std::weak_ptr<transport::TcpServer> weak = server;
    for (int cycle = 0; cycle < 2; ++cycle) {
      server->start();
      ASSERT_EQ(server->state(), base::LinkState::Error);
      server->stop();
      EXPECT_EQ(server->state(), base::LinkState::Closed);
    }
    server.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(io.poll(), 0u);
  }
}

// Closing the socket is not the same event as leaving its cancelled read
// handler. Keep that last handler alive and check both outside callers.
TEST(TcpCancelledIoCompletionTest, OutsideStopsWaitForTheLastCancelledHandler) {
  Context context;
  config::TcpServerConfig server_config;
  server_config.port = test::TestUtils::getAvailableTestPort();
  auto server = transport::TcpServer::create(server_config);
  config::TcpClientConfig client_config;
  client_config.host = "127.0.0.1";
  client_config.port = server_config.port;
  auto client = transport::TcpClient::create(client_config, *context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_tcp_io_completion_hook.store(nullptr);
    cleanup_park.store(nullptr);
    client->stop();
    server->stop();
  }};
  server->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return server->state() == base::LinkState::Listening; }, 3000));
  client->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return client->is_connected(); }, 3000));
  cleanup_park.store(&park);
  transport::detail::g_tcp_io_completion_hook.store([] { park_cleanup(nullptr, true); });
  first = std::async(std::launch::async, [&] { client->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { client->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
}

INSTANTIATE_TEST_SUITE_P(ClientAndServer, TcpStopAdmissionTest, ::testing::Bool());

}  // namespace

namespace {
class TcpWriteStopAdmissionTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpWriteStopAdmissionTest, StopCannotOvertakeCheckedWrite) {
  Context context;
  const auto port = test::TestUtils::getAvailableTestPort();
  auto server = wirestead::tcp_server(port).build();
  ASSERT_TRUE(server->start_sync());
  config::TcpClientConfig cfg;
  cfg.port = port;
  auto client = transport::TcpClient::create(cfg, *context.io);
  client->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return client->is_connected(); }, 3000));
  AdmissionPark park;
  std::future<bool> write;
  std::future<void> stopping;
  Signal stop_entered;
  OnExit cleanup{[&] {
    park.release.notify();
    if (write.valid()) write.wait();
    if (stopping.valid()) stopping.wait();
    transport::detail::g_tcp_write_admission_hook.store(nullptr);
    admission_park.store(nullptr);
    client->stop();
    server->stop();
  }};
  admission_park.store(&park);
  transport::detail::g_tcp_write_admission_hook.store(&park_admission);
  write = std::async(std::launch::async, [&] {
    std::vector<uint8_t> data(32, 42);
    auto shared = std::make_shared<const std::vector<uint8_t>>(data);
    switch (GetParam()) {
      case 0:
        return client->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));
      case 1:
        return client->async_write_move(std::move(data));
      case 2:
        return client->async_write_shared(shared);
      case 3:
        return client->async_try_write_copy(memory::ConstByteSpan(data.data(), data.size()));
      case 4:
        return client->async_try_write_move(std::move(data));
      default:
        return client->async_try_write_shared(shared);
    }
  });
  ASSERT_TRUE(park.entered.wait());
  stopping = std::async(std::launch::async, [&] {
    stop_entered.notify();
    client->stop();
  });
  ASSERT_TRUE(stop_entered.wait());
  EXPECT_EQ(stopping.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  EXPECT_EQ(write.wait_for(3s), std::future_status::ready);
  EXPECT_TRUE(write.get());
  EXPECT_EQ(stopping.wait_for(3s), std::future_status::ready);
  stopping.get();
  const auto stats = client->stats();
  EXPECT_EQ(stats.messages_accepted, 1u);
  EXPECT_EQ(stats.queued_bytes, 0u);
  EXPECT_EQ(stats.pending_bytes, 0u);
  std::vector<uint8_t> after(1, 42);
  EXPECT_FALSE(client->async_write_move(std::move(after)));
  EXPECT_EQ(client->stats().messages_accepted, 1u);
}
INSTANTIATE_TEST_SUITE_P(AllWriteForms, TcpWriteStopAdmissionTest, ::testing::Range(0, 6));

TEST(TcpWriteStopAdmissionCallbackTest, BackpressureCanRequestStopFromExecutorWrite) {
  Context context;
  const auto port = test::TestUtils::getAvailableTestPort();
  auto server = wirestead::tcp_server(port).build();
  ASSERT_TRUE(server->start_sync());
  config::TcpClientConfig cfg;
  cfg.port = port;
  cfg.backpressure_threshold = 1024;
  auto client = transport::TcpClient::create(cfg, *context.io);
  client->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return client->is_connected(); }, 3000));
  Signal callback_returned, writer_returned;
  std::atomic<bool> accepted{false};
  client->on_backpressure([&](size_t queued) {
    if (queued < 1024) return;
    client->stop();
    callback_returned.notify();
  });
  boost::asio::post(client->get_executor(), [&] {
    accepted = client->async_write_move(std::vector<uint8_t>(1024, 42));
    writer_returned.notify();
  });
  EXPECT_TRUE(writer_returned.wait());
  EXPECT_TRUE(callback_returned.wait());
  EXPECT_TRUE(accepted);
  client->stop();
  client->on_backpressure(nullptr);
  server->stop();
}
}  // namespace

namespace {
bool readiness_write(const std::shared_ptr<transport::TcpClient>& client, int form) {
  std::vector<uint8_t> data(32, 42);
  switch (form) {
    case 0:
      return client->async_write_copy(memory::ConstByteSpan(data.data(), data.size()));
    case 1:
      return client->async_write_move(std::move(data));
    case 2:
      return client->async_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
    case 3:
      return client->async_try_write_copy(memory::ConstByteSpan(data.data(), data.size()));
    case 4:
      return client->async_try_write_move(std::move(data));
    default:
      return client->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
  }
}
class TcpWriteReadinessTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpWriteReadinessTest, RejectsBeforeStartWithoutRetainingWork) {
  Context context;
  config::TcpClientConfig cfg;
  auto client = transport::TcpClient::create(cfg, *context.io);
  EXPECT_FALSE(readiness_write(client, GetParam()));
  client->stop();
  EXPECT_EQ(client->stats().messages_accepted, 0u);
  EXPECT_EQ(client->stats().queued_bytes, 0u);
  EXPECT_EQ(client->stats().pending_bytes, 0u);
}
TEST_P(TcpWriteReadinessTest, RejectsConnectingAndConnectionLossCallbacks) {
  Context context;
  const auto port = test::TestUtils::getAvailableTestPort();
  auto server = wirestead::tcp_server(port).build();
  ASSERT_TRUE(server->start_sync());
  config::TcpClientConfig cfg;
  cfg.port = port;
  auto client = transport::TcpClient::create(cfg, *context.io);
  Signal initial, lost;
  std::atomic<bool> was_connected{false}, connected_accepted{false}, initial_accepted{true}, lost_accepted{true};
  OnExit cleanup{[&] {
    client->stop();
    client->on_state(nullptr);
    server->stop();
  }};
  client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) {
      connected_accepted = readiness_write(client, GetParam());
      was_connected = true;
    }
    if (state != base::LinkState::Connecting) return;
    const bool accepted = readiness_write(client, GetParam());
    if (was_connected) {
      lost_accepted = accepted;
      lost.notify();
    } else {
      initial_accepted = accepted;
      initial.notify();
    }
  });
  client->start();
  ASSERT_TRUE(initial.wait());
  EXPECT_FALSE(initial_accepted);
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return was_connected.load(); }, 3000));
  EXPECT_TRUE(connected_accepted);
  server->stop();
  ASSERT_TRUE(lost.wait());
  EXPECT_FALSE(lost_accepted);
  client->stop();
  EXPECT_EQ(client->stats().messages_accepted, 1u);
}
INSTANTIATE_TEST_SUITE_P(AllWriteForms, TcpWriteReadinessTest, ::testing::Range(0, 6));
}  // namespace

namespace {
std::atomic<int> observed_send_result{-1};
void observe_send_result(const wrapper::SendResult& result) {
  observed_send_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}
std::atomic<int> observed_wait_result{-1};
std::atomic<AdmissionPark*> wait_result_park{nullptr};
void observe_wait_result(const wrapper::SendResult& result) {
  if (auto park = wait_result_park.load()) {
    park->entered.notify();
    park->release.hold();
  }
  observed_wait_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}

class PressuredRunChannel : public SavedChannel {
 public:
  std::atomic<bool> pressure{true};
  bool is_backpressure_active() const override { return pressure.load(); }
};
class TcpCapacityWaitRunTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpCapacityWaitRunTest, OldWaitCannotResumeInRestartedRun) {
  auto channel = std::make_shared<PressuredRunChannel>();
  wrapper::TcpClient client(channel);
  auto start = [&] {
    auto ready = client.start();
    channel->state(base::LinkState::Connected);
    EXPECT_TRUE(ready.get());
  };
  start();
  AdmissionPark park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    channel->pressure = false;
    if (writer.valid()) writer.wait();
    wrapper::detail::g_tcp_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_tcp_capacity_wait_result_hook.store(nullptr);
    admission_park.store(nullptr);
    client.stop();
  }};
  auto send = [&] {
    switch (GetParam() % 6) {
      case 0:
        return client.send("old");
      case 1:
        return client.send_line("old");
      case 2:
        return client.send_blocking("old");
      case 3:
        return client.send_line_blocking("old");
      case 4:
        return client.send_move(std::vector<uint8_t>{1, 2, 3});
      default:
        return client.send_shared(std::make_shared<const std::vector<uint8_t>>(3, 42));
    }
  };
  observed_wait_result = -1;
  wrapper::detail::g_tcp_capacity_wait_result_hook.store(&observe_wait_result);
  admission_park.store(&park);
  wrapper::detail::g_tcp_capacity_wait_hook.store(&park_admission);
  writer = std::async(std::launch::async, send);
  ASSERT_TRUE(park.entered.wait());
  client.stop();
  if (GetParam() >= 6) start();
  channel->pressure = GetParam() >= 12;
  park.release.notify();
  // Restart must not hide the stop even if the new run is also pressured.
  EXPECT_EQ(writer.wait_for(300ms), std::future_status::ready);
  channel->pressure = false;
  EXPECT_FALSE(writer.get());
  EXPECT_EQ(observed_wait_result, static_cast<int>(wrapper::SendRejection::CancelledWhileWaiting));
  if (GetParam() >= 6) {
    EXPECT_TRUE(send());
  }
}
INSTANTIATE_TEST_SUITE_P(StopAndRestartWithPressure, TcpCapacityWaitRunTest, ::testing::Range(0, 18));
}  // namespace

namespace {
class TcpCapacityWaitConnectionTest : public ::testing::TestWithParam<int> {};
TEST_P(TcpCapacityWaitConnectionTest, PreservesWaitReleaseOutcome) {
  Context context;
  namespace net = boost::asio;
  using tcp = net::ip::tcp;
  net::io_context peer_io;
  tcp::acceptor acceptor(peer_io, tcp::endpoint(tcp::v4(), 0));
  acceptor.set_option(net::socket_base::receive_buffer_size(1024));
  acceptor.non_blocking(true);
  tcp::socket first(peer_io), second(peer_io);
  config::TcpClientConfig cfg;
  cfg.port = acceptor.local_endpoint().port();
  cfg.send_buffer_size = 1024;
  cfg.backpressure_threshold = 1024;
  cfg.retry_interval_ms = 20;
  auto transport = transport::TcpClient::create(cfg, *context.io);
  wrapper::TcpClient client(transport);
  std::atomic<int> connections{0};
  AdmissionPark park, result_park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    result_park.release.notify();
    client.stop();
    if (writer.valid()) writer.wait();
    wrapper::detail::g_tcp_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_tcp_capacity_wait_result_hook.store(nullptr);
    wrapper::detail::g_tcp_send_result_hook.store(nullptr);
    wait_result_park.store(nullptr);
    transport::detail::g_tcp_pinned_write_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  auto accept = [&](tcp::socket& socket) {
    return test::TestUtils::waitForCondition(
        [&] {
          boost::system::error_code ec;
          acceptor.accept(socket, ec);
          return !ec;
        },
        3000);
  };
  client.on_connect([&](const auto&) { ++connections; });
  auto ready = client.start();
  ASSERT_TRUE(accept(first));
  ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
  ASSERT_TRUE(ready.get());
  if (GetParam() < 12 || GetParam() >= 18) {
    ASSERT_TRUE(transport->async_write_move(std::vector<uint8_t>(512 * 1024, 'x')));
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_backpressure_active(); }, 3000));
  }
  observed_wait_result = -1;
  wrapper::detail::g_tcp_capacity_wait_result_hook.store(&observe_wait_result);
  if (GetParam() >= 30 && GetParam() < 36) wait_result_park.store(&result_park);
  admission_park.store(&park);
  if (GetParam() < 12 || GetParam() >= 18) {
    wrapper::detail::g_tcp_capacity_wait_hook.store(&park_admission);
  } else {
    transport::detail::g_tcp_pinned_write_hook.store(&park_admission);
  }
  auto send = [&] {
    switch (GetParam() % 6) {
      case 0:
        return client.send("old");
      case 1:
        return client.send_line("old");
      case 2:
        return client.send_blocking("old");
      case 3:
        return client.send_line_blocking("old");
      case 4:
        return client.send_move(std::vector<uint8_t>{1, 2, 3});
      default:
        return client.send_shared(std::make_shared<const std::vector<uint8_t>>(3, 42));
    }
  };
  observed_send_result = -1;
  wrapper::detail::g_tcp_send_result_hook.store(&observe_send_result);
  if (GetParam() >= 42) {
    wrapper::detail::CallbackGuard guard;
    EXPECT_FALSE(send());
    EXPECT_EQ(observed_send_result, static_cast<int>(wrapper::SendRejection::WouldBlock));
    EXPECT_EQ(observed_wait_result, -1);
    return;
  }
  writer = std::async(std::launch::async, send);
  ASSERT_TRUE(park.entered.wait());
  if (GetParam() < 18) {
    first.close();
    ASSERT_TRUE(accept(second));
    // Final-admission seam holds the wrapper lock; readiness precedes delivery.
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_connected(); }, 3000));
    if (GetParam() >= 6 && GetParam() < 12) {
      ASSERT_TRUE(transport->async_write_move(std::vector<uint8_t>(512 * 1024, 'y')));
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return transport->is_backpressure_active(); }, 3000));
    }
  } else if (GetParam() < 24) {
    // Prevent a replacement connection while proving loss-before-stop ordering.
    acceptor.close();
    first.close();
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_connected(); }, 3000));
    client.stop();
  } else if (GetParam() < 30) {
    if (GetParam() % 2 == 0)
      transport->stop();
    else
      client.stop();
    first.close();
  } else {
    // Drain the actual socket, then freeze the selected capacity result.
    first.set_option(net::socket_base::receive_buffer_size(1024 * 1024));
    first.non_blocking(true);
    std::vector<uint8_t> buffer(512 * 1024);
    size_t received = 0;
    ASSERT_TRUE(test::TestUtils::waitForCondition(
        [&] {
          boost::system::error_code ec;
          received += first.read_some(net::buffer(buffer.data(), buffer.size()), ec);
          return received == 512 * 1024;
        },
        10000));
    ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_backpressure_active(); }, 3000));
  }
  const auto accepted_before = transport->stats().messages_accepted;
  park.release.notify();
  if (GetParam() >= 30 && GetParam() < 36) {
    ASSERT_TRUE(result_park.entered.wait());
    client.stop();
    result_park.release.notify();
  }
  const auto status = writer.wait_for(300ms);
  EXPECT_EQ(status, std::future_status::ready);
  if (status != std::future_status::ready) client.stop();
  const bool capacity_accepted = GetParam() >= 36;
  const auto send_result = writer.get();
  EXPECT_EQ(send_result.accepted(), capacity_accepted);
  EXPECT_EQ(transport->stats().messages_accepted, accepted_before + (capacity_accepted ? 1 : 0));
  const auto expected_send = capacity_accepted  ? 100
                             : GetParam() >= 30 ? static_cast<int>(wrapper::SendRejection::NotStarted)
                             : GetParam() >= 24 ? static_cast<int>(wrapper::SendRejection::CancelledWhileWaiting)
                                                : static_cast<int>(wrapper::SendRejection::NotReady);
  EXPECT_EQ(observed_send_result, expected_send);
  EXPECT_EQ(send_result.accepted() ? 100 : static_cast<int>(send_result.reason()), expected_send);
  if (GetParam() < 12 || (GetParam() >= 18 && GetParam() < 24)) {
    EXPECT_EQ(observed_wait_result, static_cast<int>(wrapper::SendRejection::NotReady));
  } else if (GetParam() >= 24 && GetParam() < 30) {
    EXPECT_EQ(observed_wait_result, static_cast<int>(wrapper::SendRejection::CancelledWhileWaiting));
  } else if (GetParam() >= 30) {
    EXPECT_EQ(observed_wait_result, 100);
  }
  if (GetParam() < 6 || (GetParam() >= 12 && GetParam() < 18)) {
    EXPECT_TRUE(client.send("new"));
    EXPECT_EQ(transport->stats().messages_accepted, accepted_before + 1);
  }
}
INSTANTIATE_TEST_SUITE_P(ReconnectAndReleaseReasons, TcpCapacityWaitConnectionTest, ::testing::Range(0, 48));
}  // namespace
