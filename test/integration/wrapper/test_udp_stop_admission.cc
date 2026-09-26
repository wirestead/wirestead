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

#include "tcp_stop_with_context.hpp"
#include "test_connection_channel.hpp"
#include "test_utils.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/udp/udp.hpp"
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

// This channel preserves snapshots without borrowing an actual UDP strand.
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

class UdpStopAdmissionTest : public ::testing::Test {};

TEST_F(UdpStopAdmissionTest, ParkedOldHandlerIsRefusedAfterStopAndRestart) {
  auto channel = std::make_shared<SavedChannel>();
  auto client = std::make_shared<wrapper::UdpClient>(channel);
  std::atomic<int> delivered{0};
  if (client) client->on_data([&](const auto&) { ++delivered; });
  auto start = [&] {
    auto ready = client->start();
    channel->state(base::LinkState::Connected);
    EXPECT_EQ(ready.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(ready.get());
  };
  auto stop = [&] { client->stop(); };
  auto snapshot = [&]() -> std::function<void()> {
    auto bytes = channel->bytes;
    return [bytes] {
      const uint8_t data[] = {42};
      bytes(memory::ConstByteSpan(data, 1));
    };
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

std::shared_ptr<transport::UdpChannel> make_transport(boost::asio::io_context& io) {
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  return transport::UdpChannel::create(cfg, io);
}

TEST_F(UdpStopAdmissionTest, BothOutsideStopsWaitUntilCleanupActuallyCompletes) {
  Context context;
  auto channel = make_transport(*context.io);
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

TEST_F(UdpStopAdmissionTest, StopDoesNotRunUnrelatedReadyHandlers) {
  Context context;
  auto channel = make_transport(*context.io);
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

TEST_F(UdpStopAdmissionTest, NeverStartedStopNeedsNoExecutorAndRetainsNoWork) {
  boost::asio::io_context io;
  auto channel = make_transport(io);
  std::weak_ptr<interface::Channel> weak = channel;
  channel->stop();
  channel->stop();
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}

TEST(UdpWriteStopTest, UnstartedWritesRetainNoWorkAndReentrantStopIsSafe) {
  boost::asio::io_context io;
  config::UdpConfig cfg;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  auto channel = transport::UdpChannel::create(cfg, io);
  const uint8_t byte = 1;
  EXPECT_FALSE(channel->async_write_copy(memory::ConstByteSpan(&byte, 1)));
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>{byte}));
  EXPECT_FALSE(channel->async_write_shared(std::make_shared<const std::vector<uint8_t>>(1, byte)));
  EXPECT_FALSE(
      channel->async_write_to(memory::ConstByteSpan(&byte, 1), {boost::asio::ip::make_address("127.0.0.1"), 12345}));
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Closed) channel->stop();
  });
  channel->stop();
  std::weak_ptr<transport::UdpChannel> weak = channel;
  channel.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(io.poll(), 0u);
}
TEST(UdpWriteStopTest, OversizedWriteErrorCallbackCanRequestStop) {
  Context context;
  config::UdpConfig cfg;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 12345;
  auto channel = transport::UdpChannel::create(cfg, *context.io);
  Signal ready, requested;
  OnExit cleanup{[&] { channel->stop(); }};
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Connected) ready.notify();
    if (state == base::LinkState::Error) {
      channel->stop();
      requested.notify();
    }
  });
  channel->start();
  ASSERT_TRUE(ready.wait());
  EXPECT_FALSE(channel->async_write_move(std::vector<uint8_t>(*channel->write_queue_limit() + 1)));
  EXPECT_TRUE(requested.wait());
  channel->stop();
}
TEST(UdpFailedStartStopTest, ValidationFailureNeedsNoExecutorForCleanup) {
  boost::asio::io_context io;
  config::UdpConfig cfg;
  cfg.bind_address = "not-an-address";
  EXPECT_THROW(transport::UdpChannel::create(cfg, io), std::invalid_argument);
  EXPECT_THROW(transport::UdpChannel::create(cfg), std::invalid_argument);
  EXPECT_EQ(io.poll(), 0u);
}

TEST(UdpCancelledIoCompletionTest, OutsideStopsWaitForTheLastCancelledHandler) {
  Context context;
  auto channel = make_transport(*context.io);
  CleanupPark park;
  std::future<void> first, second;
  OnExit cleanup{[&] {
    park.release.notify();
    if (first.valid()) first.wait();
    if (second.valid()) second.wait();
    transport::detail::g_udp_io_completion_hook.store(nullptr);
    cleanup_park.store(nullptr);
    channel->stop();
  }};
  Signal listening;
  channel->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Listening) listening.notify();
  });
  channel->start();
  ASSERT_TRUE(listening.wait());
  cleanup_park.store(&park);
  transport::detail::g_udp_io_completion_hook.store([] { park_cleanup(nullptr, true); });
  first = std::async(std::launch::async, [&] { channel->stop(); });
  ASSERT_TRUE(park.entered.wait());
  second = std::async(std::launch::async, [&] { channel->stop(); });
  EXPECT_EQ(first.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(100ms), std::future_status::timeout);
  park.release.notify();
  first.get();
  second.get();
}

}  // namespace

namespace {
thread_local std::optional<wirestead::wrapper::SendResult> nonblocking_result;
thread_local int nonblocking_observations = 0;
void observe_nonblocking_result(const wirestead::wrapper::SendResult& result) {
  nonblocking_result = result;
  ++nonblocking_observations;
}

struct NonblockingResultPeer {
  boost::asio::io_context io;
  boost::asio::ip::udp::socket sink{io, {boost::asio::ip::udp::v4(), 0}};
  std::shared_ptr<wirestead::transport::UdpChannel> native;
  std::unique_ptr<wirestead::wrapper::UdpClient> client;
  explicit NonblockingResultPeer(bool best_effort) {
    wirestead::config::UdpConfig cfg;
    cfg.remote_address = "127.0.0.1";
    cfg.remote_port = sink.local_endpoint().port();
    cfg.backpressure_threshold = 1024;
    cfg.backpressure_strategy = best_effort ? wirestead::base::constants::BackpressureStrategy::BestEffort
                                            : wirestead::base::constants::BackpressureStrategy::Reliable;
    native = wirestead::transport::UdpChannel::create(cfg, io);
    client = std::make_unique<wirestead::wrapper::UdpClient>(native);
    client->backpressure_strategy(cfg.backpressure_strategy);
    wirestead::wrapper::detail::g_udp_send_result_hook.store(observe_nonblocking_result);
  }
  ~NonblockingResultPeer() {
    wirestead::wrapper::detail::g_udp_send_result_hook.store(nullptr);
    wirestead::test::stop_wrapper_with_context(*client, io);
    sink.close();
  }
  template <typename Predicate>
  bool until(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_for(std::chrono::milliseconds(10));
    }
    return true;
  }
};

class UdpNonblockingResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpNonblockingResultTest, OrdersValidationLifecycleAndCapacityReasons) {
  using Rejection = wirestead::wrapper::SendRejection;
  const bool best_effort = GetParam() >= 4 && GetParam() < 8;
  const int form = GetParam() % 4;
  // Explicit try methods must stay WouldBlock even on a BestEffort channel.
  NonblockingResultPeer peer(GetParam() >= 4);
  auto& client = *peer.client;
  std::optional<wirestead::wrapper::SendResult> returned_result;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    auto accepted = wirestead::wrapper::SendResult::reject(wirestead::wrapper::SendRejection::NotReady);
    if (form == 0) {
      accepted = best_effort ? client.send(text) : client.try_send(text);
    } else if (form == 1) {
      accepted = best_effort ? client.send_line(text) : client.try_send_line(text);
    } else if (form == 2) {
      std::vector<uint8_t> payload(text.begin(), text.end());
      accepted = best_effort ? client.send_move(std::move(payload)) : client.try_send_move(std::move(payload));
      if (!accepted) {
        EXPECT_EQ(std::string(payload.begin(), payload.end()), text);
      }
    } else {
      auto payload = std::make_shared<const std::vector<uint8_t>>(text.begin(), text.end());
      accepted = best_effort ? client.send_shared(payload) : client.try_send_shared(payload);
    }
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (nonblocking_result) {
      EXPECT_EQ(nonblocking_result->accepted(), accepted.accepted());
    }
    returned_result = accepted;
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(returned_result.has_value());
    ASSERT_FALSE(returned_result->accepted());
    EXPECT_EQ(returned_result->reason(), expected);
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };

  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  if (form != 1) {
    EXPECT_FALSE(write(""));
    reason(Rejection::InvalidArgument);
  }
  // Line delimiters count against the hard queue limit before state/capacity.
  const std::string oversized(*peer.native->write_queue_limit() + (form == 1 ? 0 : 1), 'x');
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  EXPECT_EQ(peer.native->stats().failed_sends, 0u);

  auto started = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  // Fill high-water without running the executor again.
  ASSERT_TRUE(client.try_send(std::string(1024, 'f')));
  EXPECT_FALSE(write("valid"));
  reason(best_effort ? Rejection::QueueFull : Rejection::WouldBlock);
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);

  bool stopped_on_executor = false;
  boost::asio::post(peer.io, [&] {
    client.stop();
    EXPECT_FALSE(write("valid"));
    reason(Rejection::Stopping);
    EXPECT_FALSE(write(oversized));
    reason(Rejection::TooLarge);
    stopped_on_executor = true;
  });
  ASSERT_TRUE(peer.until([&] { return stopped_on_executor; }));
  wirestead::test::stop_wrapper_with_context(client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  auto restarted = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(restarted.get());
  EXPECT_TRUE(write("valid"));
}

INSTANTIATE_TEST_SUITE_P(TryAndBestEffortForms, UdpNonblockingResultTest, ::testing::Range(0, 12));

TEST(UdpNonblockingResultContract, ConnectedNativeStillRequiresWrapperStart) {
  using Rejection = wirestead::wrapper::SendRejection;
  NonblockingResultPeer peer(false);
  peer.native->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  EXPECT_FALSE(peer.client->try_send("before wrapper start"));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), Rejection::NotStarted);
  EXPECT_EQ(peer.native->stats().messages_accepted, 0u);
  ASSERT_TRUE(peer.client->start().get());
  EXPECT_TRUE(peer.client->try_send("after wrapper start"));
}

TEST(UdpNonblockingResultContract, NullSharedPayloadIsInvalidBeforeStartAndAfterStop) {
  NonblockingResultPeer peer(true);
  EXPECT_FALSE(peer.client->send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
  peer.client->stop();
  EXPECT_FALSE(peer.client->try_send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::InvalidArgument);
}

class UdpReliableResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpReliableResultTest, ValidatesBeforeStateAndPreservesPayload) {
  using Rejection = wirestead::wrapper::SendRejection;
  // The last two cases exercise explicit blocking under BestEffort.
  NonblockingResultPeer peer(GetParam() >= 6);
  const int form = GetParam() >= 6 ? GetParam() - 4 : GetParam();
  auto& client = *peer.client;
  std::optional<wirestead::wrapper::SendResult> returned_result;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    auto accepted = wirestead::wrapper::SendResult::reject(wirestead::wrapper::SendRejection::NotReady);
    if (form == 0)
      accepted = client.send(text);
    else if (form == 1)
      accepted = client.send_line(text);
    else if (form == 2)
      accepted = client.send_blocking(text);
    else if (form == 3)
      accepted = client.send_line_blocking(text);
    else if (form == 4) {
      std::vector<uint8_t> payload(text.begin(), text.end());
      accepted = client.send_move(std::move(payload));
      if (!accepted) {
        EXPECT_EQ(std::string(payload.begin(), payload.end()), text);
      }
    } else
      accepted = client.send_shared(std::make_shared<const std::vector<uint8_t>>(text.begin(), text.end()));
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (nonblocking_result) {
      EXPECT_EQ(nonblocking_result->accepted(), accepted.accepted());
    }
    returned_result = accepted;
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(returned_result.has_value());
    ASSERT_FALSE(returned_result->accepted());
    EXPECT_EQ(returned_result->reason(), expected);
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };
  const bool line = form == 1 || form == 3;
  const std::string oversized(*peer.native->write_queue_limit() + (line ? 0 : 1), 'x');
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  if (!line) {
    EXPECT_FALSE(write(""));
    reason(Rejection::InvalidArgument);
  }
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  EXPECT_EQ(peer.native->stats().failed_sends, 0u);
  auto started = client.start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  {
    wirestead::wrapper::detail::CallbackGuard guard;
    EXPECT_TRUE(write("capacity available in callback"));
  }
  wirestead::test::stop_wrapper_with_context(client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  EXPECT_FALSE(write(oversized));
  reason(Rejection::TooLarge);
  EXPECT_FALSE(client.send_shared(nullptr));
  ASSERT_TRUE(nonblocking_result.has_value());
  EXPECT_EQ(nonblocking_result->reason(), Rejection::InvalidArgument);
}
INSTANTIATE_TEST_SUITE_P(ReliableAndExplicitBlocking, UdpReliableResultTest, ::testing::Range(0, 8));

TEST(UdpReliableResultContract, NativeCapacityRetriesBeyondFiveAndStopReleasesSender) {
  NonblockingResultPeer peer(false);
  auto started = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit(), 'f')));
  const auto failures = peer.native->stats().failed_sends;
  std::vector<uint8_t> payload{1, 2, 3};
  auto sender = std::async(std::launch::async, [&] { return peer.client->send_move(std::move(payload)); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (peer.native->stats().failed_sends < failures + 12 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  const bool retried = peer.native->stats().failed_sends >= failures + 12;
  auto stopper = std::async(std::launch::async, [&] { peer.client->stop(); });
  // Stop cancels the retained wait before waiting for native executor cleanup.
  const bool released = sender.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(peer.until([&] { return stopper.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready; }));
  stopper.get();
  ASSERT_TRUE(released);
  const auto result = sender.get();
  EXPECT_TRUE(retried);
  EXPECT_FALSE(result.accepted());
  EXPECT_TRUE(result.reason() == wirestead::wrapper::SendRejection::CancelledWhileWaiting ||
              result.reason() == wirestead::wrapper::SendRejection::Stopping);
  EXPECT_EQ(payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(UdpReliableResultContract, CallbackCapacityRefusalPreservesMoveStorage) {
  NonblockingResultPeer peer(false);
  auto started = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  // Keep the executor paused: inflight reservations fill the hard limit but
  // have not yet published high-water pressure. Callback callers must not retry.
  ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit(), 'f')));
  ASSERT_FALSE(peer.native->is_backpressure_active());
  wirestead::wrapper::detail::CallbackGuard callback_scope;
  const auto failures = peer.native->stats().failed_sends;
  for (int form = 0; form < 3; ++form) {
    nonblocking_observations = 0;
    std::vector<uint8_t> payload{1, 2, 3};
    if (form == 0) {
      EXPECT_FALSE(peer.client->send_blocking("copy"));
    } else if (form == 1) {
      EXPECT_FALSE(peer.client->send_move(std::move(payload)));
      EXPECT_EQ(payload, (std::vector<uint8_t>{1, 2, 3}));
    } else {
      EXPECT_FALSE(peer.client->send_shared(std::make_shared<const std::vector<uint8_t>>(payload)));
    }
    ASSERT_TRUE(nonblocking_result.has_value());
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), wirestead::wrapper::SendRejection::WouldBlock);
    EXPECT_EQ(peer.native->stats().failed_sends, failures + (form + 1));
    EXPECT_EQ(peer.native->stats().messages_accepted, 1u);
  }
}
}  // namespace

namespace {
class UdpNativeResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpNativeResultTest, ReportsLifecycleValidationAndCapacityAtAdmission) {
  using Rejection = wirestead::wrapper::SendRejection;
  NonblockingResultPeer peer(GetParam() >= 6);
  wirestead::transport::detail::g_udp_write_result_hook.store(observe_nonblocking_result);
  struct ResetHook {
    ~ResetHook() { wirestead::transport::detail::g_udp_write_result_hook.store(nullptr); }
  } reset_hook;
  auto write = [&](std::string_view text) {
    nonblocking_result.reset();
    nonblocking_observations = 0;
    std::vector<uint8_t> data(text.begin(), text.end());
    bool accepted;
    switch (GetParam() % 6) {
      case 0:
        accepted = peer.native->async_write_copy({data.data(), data.size()});
        break;
      case 1:
        accepted = peer.native->async_write_move(std::move(data));
        break;
      case 2:
        accepted = peer.native->async_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
      case 3:
        accepted = peer.native->async_try_write_copy({data.data(), data.size()});
        break;
      case 4:
        accepted = peer.native->async_try_write_move(std::move(data));
        break;
      default:
        accepted = peer.native->async_try_write_shared(std::make_shared<const std::vector<uint8_t>>(data));
        break;
    }
    EXPECT_EQ(nonblocking_observations, 1);
    EXPECT_TRUE(nonblocking_result.has_value());
    if (!accepted) {
      EXPECT_EQ(std::string(data.begin(), data.end()), text);
    }
    return accepted;
  };
  auto reason = [&](Rejection expected) {
    ASSERT_TRUE(nonblocking_result.has_value());
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
  auto started = peer.client->start();
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(started.get());
  EXPECT_FALSE(write(""));
  reason(Rejection::InvalidArgument);
  EXPECT_TRUE(write("accepted"));
  ASSERT_TRUE(nonblocking_result->accepted());
  if (GetParam() % 6 < 3) {
    ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(*peer.native->write_queue_limit() - 8, 'f')));
  } else {
    ASSERT_TRUE(peer.native->async_try_write_move(std::vector<uint8_t>(1024 - 8, 'f')));
  }
  EXPECT_FALSE(write("valid"));
  reason(Rejection::WouldBlock);
  // Stop on the executor, while completion is still pending.
  bool requested = false;
  boost::asio::post(peer.io, [&] {
    peer.native->stop();
    EXPECT_FALSE(write("valid"));
    reason(Rejection::Stopping);
    requested = true;
  });
  ASSERT_TRUE(peer.until([&] { return requested; }));
  wirestead::test::stop_wrapper_with_context(*peer.client, peer.io);
  EXPECT_FALSE(write("valid"));
  reason(Rejection::NotStarted);
}
INSTANTIATE_TEST_SUITE_P(FormsAndStrategies, UdpNativeResultTest, ::testing::Range(0, 12));
}  // namespace

namespace {
std::atomic<int> udp_wait_result{-1};
std::atomic<int> udp_final_result{-1};
void observe_udp_wait(const wrapper::SendResult& result) {
  udp_wait_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}
void observe_udp_final(const wrapper::SendResult& result) {
  udp_final_result = result.accepted() ? 100 : static_cast<int>(result.reason());
}
class UdpWaitResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpWaitResultTest, KeepsReleaseCauseAcrossStopRestartAndError) {
  NonblockingResultPeer peer(false);
  auto ready = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(ready.get());
  ASSERT_TRUE(peer.native->async_try_write_move(std::vector<uint8_t>(1024, 'f')));
  ASSERT_EQ(peer.io.poll_one(), 1u);  // Publish pressure, leave the send completion queued.
  ASSERT_TRUE(peer.native->is_backpressure_active());
  AdmissionPark park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    test::stop_wrapper_with_context(*peer.client, peer.io);
    if (writer.valid()) writer.wait();
    wrapper::detail::g_udp_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_udp_capacity_wait_result_hook.store(nullptr);
    wrapper::detail::g_udp_send_result_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  admission_park.store(&park);
  wrapper::detail::g_udp_capacity_wait_hook.store(park_admission);
  wrapper::detail::g_udp_capacity_wait_result_hook.store(observe_udp_wait);
  wrapper::detail::g_udp_send_result_hook.store(observe_udp_final);
  udp_wait_result = udp_final_result = -1;
  writer = std::async(std::launch::async, [&] {
    switch (GetParam() % 6) {
      case 0:
        return peer.client->send("old");
      case 1:
        return peer.client->send_line("old");
      case 2:
        return peer.client->send_blocking("old");
      case 3:
        return peer.client->send_line_blocking("old");
      case 4:
        return peer.client->send_move(std::vector<uint8_t>{1, 2, 3});
      default:
        return peer.client->send_shared(std::make_shared<const std::vector<uint8_t>>(3, 42));
    }
  });
  ASSERT_TRUE(park.entered.wait());
  const int event = GetParam() / 6;
  if (event == 3) {
    bool failed = false;
    peer.client->on_error([&](const auto&) { failed = true; });
    ASSERT_TRUE(peer.native->async_write_move(std::vector<uint8_t>(65536, 'x')));
    ASSERT_TRUE(peer.until([&] { return failed; }));
  }
  if (event != 2) {
    test::stop_wrapper_with_context(*peer.client, peer.io);
    if (event == 1) {
      auto restarted = peer.client->start();
      ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
      ASSERT_TRUE(restarted.get());
    }
  } else {
    ASSERT_TRUE(peer.until([&] { return !peer.native->is_backpressure_active(); }));
  }
  const auto before = peer.native->stats().messages_accepted;
  park.release.notify();
  ASSERT_EQ(writer.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(writer.get().accepted(), event == 2);
  const auto expected = event == 2 ? 100
                                   : static_cast<int>(event == 3 ? wrapper::SendRejection::NotReady
                                                                 : wrapper::SendRejection::CancelledWhileWaiting);
  EXPECT_EQ(udp_wait_result, expected);
  EXPECT_EQ(udp_final_result, expected);
  EXPECT_EQ(peer.native->stats().messages_accepted, before + (event == 2 ? 1 : 0));
}
INSTANTIATE_TEST_SUITE_P(FormsAndReleaseEvents, UdpWaitResultTest, ::testing::Range(0, 24));

TEST(UdpSendReadiness, ListeningWithoutPeerRejectsClientWritesButAcceptsExplicitTarget) {
  boost::asio::io_context io;
  boost::asio::ip::udp::socket sink(io, {boost::asio::ip::udp::v4(), 0});
  config::UdpConfig cfg;
  cfg.bind_address = "127.0.0.1";
  auto native = transport::UdpChannel::create(cfg, io);
  wrapper::UdpClient client(native);
  OnExit cleanup{[&] {
    transport::detail::g_udp_write_result_hook.store(nullptr);
    wrapper::detail::g_udp_send_result_hook.store(nullptr);
    test::stop_wrapper_with_context(client, io);
  }};
  wrapper::detail::g_udp_send_result_hook.store(observe_nonblocking_result);
  transport::detail::g_udp_write_result_hook.store(observe_nonblocking_result);
  auto started = client.start();
  EXPECT_FALSE(client.try_send("before open"));
  ASSERT_TRUE(nonblocking_result);
  EXPECT_EQ(nonblocking_result->reason(), wrapper::SendRejection::NotReady);
  const std::vector<uint8_t> payload{1, 2, 3};
  EXPECT_FALSE(native->async_write_to({payload.data(), payload.size()}, sink.local_endpoint()));
  EXPECT_EQ(nonblocking_result->reason(), wrapper::SendRejection::NotReady);
  io.run_for(10ms);
  EXPECT_FALSE(client.try_send("no learned remote"));
  EXPECT_EQ(nonblocking_result->reason(), wrapper::SendRejection::NotReady);
  EXPECT_TRUE(native->async_write_to({payload.data(), payload.size()}, sink.local_endpoint()));
}
}  // namespace

namespace {
struct UdpServerResultPeer {
  boost::asio::io_context io;
  boost::asio::ip::udp::socket peer{io, {boost::asio::ip::udp::v4(), 0}};
  boost::asio::ip::udp::endpoint endpoint;
  std::shared_ptr<transport::UdpChannel> native;
  std::unique_ptr<wrapper::UdpServer> server;
  UdpServerResultPeer(bool best_effort = false) {
    boost::asio::ip::udp::socket reservation(io, {boost::asio::ip::udp::v4(), 0});
    endpoint = {boost::asio::ip::make_address("127.0.0.1"), reservation.local_endpoint().port()};
    reservation.close();
    config::UdpConfig cfg;
    cfg.bind_address = "127.0.0.1";
    cfg.local_port = endpoint.port();
    cfg.backpressure_threshold = 1024;
    cfg.backpressure_strategy = best_effort ? base::constants::BackpressureStrategy::BestEffort
                                            : base::constants::BackpressureStrategy::Reliable;
    native = transport::UdpChannel::create(cfg, io);
    server = std::make_unique<wrapper::UdpServer>(native);
    server->backpressure_strategy(cfg.backpressure_strategy);
    wrapper::detail::g_udp_server_send_result_hook.store(observe_nonblocking_result);
  }
  ~UdpServerResultPeer() {
    wrapper::detail::g_udp_server_send_result_hook.store(nullptr);
    test::stop_wrapper_with_context(*server, io);
  }
  template <class Predicate>
  bool until(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      if (io.stopped()) io.restart();
      io.run_for(10ms);
    }
    return true;
  }
  void introduce() { peer.send_to(boost::asio::buffer("hello", 5), endpoint); }
};
class UdpServerResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpServerResultTest, OrdersValidationStateTargetAndCapacity) {
  UdpServerResultPeer peer(GetParam() == 2);
  auto write = [&](auto id, std::string_view data) {
    nonblocking_result.reset();
    const auto result = [&] {
      if (GetParam() == 0) return peer.server->try_send_to(id, data);
      if (GetParam() == 1) return peer.server->send_to_blocking(id, data);
      return peer.server->send_to(id, data);
    }();
    EXPECT_TRUE(nonblocking_result.has_value());
    if (nonblocking_result) {
      EXPECT_EQ(nonblocking_result->accepted(), result.accepted());
      if (!result.accepted() && !nonblocking_result->accepted()) {
        EXPECT_EQ(nonblocking_result->reason(), result.reason());
      }
    }
    nonblocking_result = result;
    return result;
  };
  auto reason = [&](wrapper::SendRejection expected) {
    ASSERT_TRUE(nonblocking_result);
    ASSERT_FALSE(nonblocking_result->accepted());
    EXPECT_EQ(nonblocking_result->reason(), expected);
  };
  EXPECT_FALSE(write(1, ""));
  reason(wrapper::SendRejection::InvalidArgument);
  EXPECT_FALSE(write(1, "valid"));
  reason(wrapper::SendRejection::NotStarted);
  const std::string large(*peer.native->write_queue_limit() + 1, 'x');
  EXPECT_FALSE(write(1, large));
  reason(wrapper::SendRejection::TooLarge);
  auto ready = peer.server->start();
  EXPECT_FALSE(write(1, "valid"));
  reason(wrapper::SendRejection::NotReady);
  ASSERT_TRUE(peer.until([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  EXPECT_FALSE(write(1, "unknown"));
  reason(wrapper::SendRejection::NotReady);
  peer.introduce();
  ASSERT_TRUE(peer.until([&] { return peer.server->client_count() == 1; }));
  const auto id = peer.server->connected_clients().front();
  ASSERT_TRUE(peer.server->try_send_to(id, std::string(1024, 'f')));
  ASSERT_EQ(peer.io.poll_one(), 1u);
  ASSERT_TRUE(peer.native->is_backpressure_active());
  {
    wrapper::detail::CallbackGuard guard;
    EXPECT_FALSE(write(id, "valid"));
    reason(GetParam() == 2 ? wrapper::SendRejection::QueueFull : wrapper::SendRejection::WouldBlock);
  }
  EXPECT_FALSE(write(id, large));
  reason(wrapper::SendRejection::TooLarge);
  test::stop_wrapper_with_context(*peer.server, peer.io);
  EXPECT_FALSE(write(id, "valid"));
  reason(wrapper::SendRejection::NotStarted);
}
INSTANTIATE_TEST_SUITE_P(TryReliableBestEffort, UdpServerResultTest, ::testing::Range(0, 3));

class UdpServerWaitResultTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpServerWaitResultTest, PinsSessionAndFirstTerminalCause) {
  UdpServerResultPeer peer;
  peer.server->idle_timeout(30ms);
  auto ready = peer.server->start();
  ASSERT_TRUE(peer.until([&] { return ready.wait_for(0ms) == std::future_status::ready; }));
  ASSERT_TRUE(ready.get());
  peer.introduce();
  ASSERT_TRUE(peer.until([&] { return peer.server->client_count() == 1; }));
  auto id = peer.server->connected_clients().front();
  ASSERT_TRUE(peer.server->try_send_to(id, std::string(1024, 'f')));
  ASSERT_EQ(peer.io.poll_one(), 1u);
  ASSERT_TRUE(peer.native->is_backpressure_active());
  AdmissionPark park;
  std::future<wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    test::stop_wrapper_with_context(*peer.server, peer.io);
    if (writer.valid()) writer.wait();
    wrapper::detail::g_udp_capacity_wait_hook.store(nullptr);
    wrapper::detail::g_udp_capacity_wait_result_hook.store(nullptr);
    wrapper::detail::g_udp_server_send_result_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  admission_park.store(&park);
  wrapper::detail::g_udp_capacity_wait_hook.store(park_admission);
  wrapper::detail::g_udp_capacity_wait_result_hook.store(observe_udp_wait);
  wrapper::detail::g_udp_server_send_result_hook.store(observe_udp_final);
  udp_wait_result = udp_final_result = -1;
  writer = std::async(std::launch::async, [&] { return peer.server->send_to_blocking(id, "old"); });
  ASSERT_TRUE(park.entered.wait());
  if (GetParam() < 2) {
    ASSERT_TRUE(peer.until([&] { return peer.server->client_count() == 0; }));
    if (GetParam() == 1) test::stop_wrapper_with_context(*peer.server, peer.io);
  } else {
    test::stop_wrapper_with_context(*peer.server, peer.io);
  }
  if (GetParam() == 0 || GetParam() == 3) {
    if (GetParam() == 3) {
      auto restarted = peer.server->start();
      ASSERT_TRUE(peer.until([&] { return restarted.wait_for(0ms) == std::future_status::ready; }));
      ASSERT_TRUE(restarted.get());
    }
    peer.introduce();
    ASSERT_TRUE(peer.until([&] { return peer.server->client_count() == 1; }));
  }
  const auto before = peer.native->stats().messages_accepted;
  park.release.notify();
  ASSERT_EQ(writer.wait_for(3s), std::future_status::ready);
  const auto result = writer.get();
  ASSERT_FALSE(result.accepted());
  const auto expected = static_cast<int>(GetParam() < 2 ? wrapper::SendRejection::NotReady
                                                        : wrapper::SendRejection::CancelledWhileWaiting);
  EXPECT_EQ(udp_wait_result, expected);
  EXPECT_EQ(udp_final_result, expected);
  EXPECT_EQ(static_cast<int>(result.reason()), expected);
  EXPECT_EQ(peer.native->stats().messages_accepted, before);
}
INSTANTIATE_TEST_SUITE_P(ExpiryStopAndReplacement, UdpServerWaitResultTest, ::testing::Range(0, 4));
}  // namespace

namespace {
class UdpPinnedAdmissionTest : public ::testing::TestWithParam<int> {};
TEST_P(UdpPinnedAdmissionTest, RejectsReplacementNativeRunAtFinalAdmission) {
  NonblockingResultPeer peer(false);
  auto ready = peer.client->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  ASSERT_TRUE(ready.get());
  // Replace native state observers so restarting the injected native run does
  // not change the wrapper generation. Only the native run pin can reject it.
  peer.native->on_state(nullptr);
  AdmissionPark park;
  std::future<wirestead::wrapper::SendResult> writer;
  OnExit cleanup{[&] {
    park.release.notify();
    test::stop_wrapper_with_context(*peer.client, peer.io);
    if (writer.valid()) writer.wait();
    transport::detail::g_udp_pinned_write_hook.store(nullptr);
    wrapper::detail::g_udp_send_result_hook.store(nullptr);
    admission_park.store(nullptr);
  }};
  admission_park.store(&park);
  transport::detail::g_udp_pinned_write_hook.store(park_admission);
  wrapper::detail::g_udp_send_result_hook.store(observe_udp_final);
  udp_final_result = -1;
  writer = std::async(std::launch::async, [&] {
    switch (GetParam()) {
      case 0:
        return peer.client->send("old");
      case 1:
        return peer.client->send_line("old");
      case 2:
        return peer.client->send_blocking("old");
      case 3:
        return peer.client->send_line_blocking("old");
      case 4:
        return peer.client->send_move(std::vector<uint8_t>{1, 2, 3});
      default:
        return peer.client->send_shared(std::make_shared<const std::vector<uint8_t>>(3, 42));
    }
  });
  ASSERT_TRUE(park.entered.wait());
  test::stop_with_context(peer.native, peer.io);
  peer.native->start();
  ASSERT_TRUE(peer.until([&] { return peer.native->is_connected(); }));
  park.release.notify();
  ASSERT_EQ(writer.wait_for(3s), std::future_status::ready);
  EXPECT_FALSE(writer.get());
  EXPECT_EQ(udp_final_result, static_cast<int>(wrapper::SendRejection::NotReady));
  EXPECT_EQ(peer.native->stats().messages_accepted, 0u);
}
INSTANTIATE_TEST_SUITE_P(AllReliableForms, UdpPinnedAdmissionTest, ::testing::Range(0, 6));
}  // namespace
