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
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server.hpp"
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

// This channel preserves snapshots without borrowing an actual UDS strand.
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
class UdsStopAdmissionTest : public ::testing::TestWithParam<bool> {};

TEST_P(UdsStopAdmissionTest, ParkedOldHandlerIsRefusedAfterStopAndRestart) {
  auto channel = std::make_shared<SavedChannel>();
  auto client = GetParam() ? nullptr : std::make_shared<wrapper::UdsClient>(channel);
  auto server = GetParam() ? std::make_shared<wrapper::UdsServer>(channel) : nullptr;
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
    config::UdsServerConfig cfg;
    cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uds-d1").string();
    return transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io);
  }
  config::UdsClientConfig cfg;
  cfg.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uds-d1").string();
  cfg.max_retries = 0;
  return transport::UdsClient::create(cfg, io);
}

TEST_P(UdsStopAdmissionTest, BothOutsideStopsWaitUntilCleanupActuallyCompletes) {
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

TEST_P(UdsStopAdmissionTest, StopDoesNotRunUnrelatedReadyHandlers) {
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

TEST_P(UdsStopAdmissionTest, NeverStartedStopNeedsNoExecutorAndRetainsNoWork) {
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
TEST(UdsFailedStartStopTest, ValidationFailureNeedsNoExecutorForCleanup) {
  boost::asio::io_context io;
  config::UdsServerConfig cfg;
  cfg.socket_path = std::string(1000, 'x');
  EXPECT_THROW(transport::UdsServer::create(cfg, std::make_unique<transport::BoostUdsAcceptor>(io), io),
               std::invalid_argument);
  EXPECT_THROW(transport::UdsServer::create(cfg), std::invalid_argument);
  EXPECT_EQ(io.poll(), 0u);
}

// Closing the socket is not the same event as leaving its cancelled read
// handler. Keep that last handler alive and check both outside callers.
TEST(UdsCancelledIoCompletionTest, OutsideStopsWaitForTheLastCancelledHandler) {
  Context context;
  config::UdsServerConfig server_config;
  server_config.socket_path = test::TestUtils::makeUniqueUdsSocketPath("uds-cancel").string();
  auto server = transport::UdsServer::create(server_config);
  config::UdsClientConfig client_config;
  client_config.socket_path = server_config.socket_path;
  auto client = transport::UdsClient::create(client_config, *context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_uds_io_completion_hook.store(nullptr);
    cleanup_park.store(nullptr);
    client->stop();
    server->stop();
  }};
  server->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return server->state() == base::LinkState::Listening; }, 3000));
  client->start();
  ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return client->is_connected(); }, 3000));
  cleanup_park.store(&park);
  transport::detail::g_uds_io_completion_hook.store([] { park_cleanup(nullptr, true); });
  first = std::async(std::launch::async, [&] { client->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { client->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
}

INSTANTIATE_TEST_SUITE_P(ClientAndServer, UdsStopAdmissionTest, ::testing::Bool());

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

class UdsCapacityWaitConnectionTest : public ::testing::TestWithParam<int> {};
TEST_P(UdsCapacityWaitConnectionTest, PreservesWaitReleaseOutcome) {
  Context context;
  namespace net = boost::asio;
  using uds = net::local::stream_protocol;
  net::io_context peer_io;
  const auto path = test::TestUtils::makeUniqueUdsSocketPath("uds-wait").string();
  OnExit remove_path{[&] { test::TestUtils::removeFileIfExists(path); }};
  // Windows AF_UNIX bind rejects SO_REUSEADDR.
  uds::acceptor acceptor(peer_io, uds::endpoint(path), false);
  acceptor.set_option(net::socket_base::receive_buffer_size(1024));
  acceptor.non_blocking(true);
  uds::socket first(peer_io), second(peer_io);
  config::UdsClientConfig cfg;
  cfg.socket_path = path;
  cfg.backpressure_threshold = 1024;
  cfg.retry_interval_ms = 100;
  auto transport = transport::UdsClient::create(cfg, *context.io);
  wrapper::UdsClient client(transport);
  std::atomic<int> connections{0};
  AdmissionPark park, result_park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    result_park.release.notify();
    client.stop();
    if (writer.valid()) writer.wait();
    wrapper::detail::g_uds_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_uds_capacity_wait_result_hook.store(nullptr);
    wrapper::detail::g_uds_send_result_hook.store(nullptr);
    wait_result_park.store(nullptr);
    transport::detail::g_uds_pinned_write_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  auto accept = [&](uds::socket& socket) {
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
  wrapper::detail::g_uds_capacity_wait_result_hook.store(&observe_wait_result);
  if (GetParam() >= 30 && GetParam() < 36) wait_result_park.store(&result_park);
  admission_park.store(&park);
  if (GetParam() < 12 || GetParam() >= 18) {
    wrapper::detail::g_uds_capacity_wait_hook.store(&park_admission);
  } else {
    transport::detail::g_uds_pinned_write_hook.store(&park_admission);
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
  wrapper::detail::g_uds_send_result_hook.store(&observe_send_result);
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
    if (GetParam() < 12) {
      ASSERT_TRUE(accept(second));
      // The native connected flag is published before the Connected state
      // notification. Wait for the replacement on_connect before using it.
      ASSERT_TRUE(test::TestUtils::waitForCondition(
          [&] { return connections.load() >= 2 && transport->is_connected(); }, 3000));
    } else {
      // UDS publishes Error before scheduling retry. The parked sender holds
      // the wrapper read lock, so let it reject before completing that callback.
      ASSERT_TRUE(test::TestUtils::waitForCondition([&] { return !transport->is_connected(); }, 3000));
    }
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
    if (GetParam() >= 12) {
      ASSERT_TRUE(accept(second));
      // The native connected flag is published before the Connected state
      // notification. Wait for the replacement on_connect before using it.
      ASSERT_TRUE(test::TestUtils::waitForCondition(
          [&] { return connections.load() >= 2 && transport->is_connected(); }, 3000));
    }
    EXPECT_TRUE(client.send("new"));
    EXPECT_EQ(transport->stats().messages_accepted, accepted_before + 1);
  }
}
INSTANTIATE_TEST_SUITE_P(ReconnectAndReleaseReasons, UdsCapacityWaitConnectionTest, ::testing::Range(0, 48));
}  // namespace
