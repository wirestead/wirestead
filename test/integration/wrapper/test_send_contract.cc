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
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "unilink/interface/channel.hpp"
#include "unilink/unilink.hpp"

namespace {

class ContractChannel : public unilink::interface::Channel {
 public:
  void start() override { connected_ = true; }
  void stop() override { connected_ = false; }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return backpressure_active_.load(); }
  boost::asio::any_io_executor get_executor() override { return ioc_.get_executor(); }

  bool async_write_copy(unilink::memory::ConstByteSpan data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++write_copy_count_;
    if (fail_next_writes_ > 0) {
      --fail_next_writes_;
      stats_.failed_sends += 1;
      return false;
    }
    stats_.messages_accepted += 1;
    stats_.bytes_accepted += data.size();
    return true;
  }

  bool async_write_move(std::vector<uint8_t>&& data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++write_move_count_;
    if (fail_next_writes_ > 0) {
      --fail_next_writes_;
      stats_.failed_sends += 1;
      return false;
    }
    stats_.messages_accepted += 1;
    stats_.bytes_accepted += data.size();
    return true;
  }

  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    if (!data) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    ++write_shared_count_;
    if (fail_next_writes_ > 0) {
      --fail_next_writes_;
      stats_.failed_sends += 1;
      return false;
    }
    stats_.messages_accepted += 1;
    stats_.bytes_accepted += data->size();
    return true;
  }

  bool async_try_write_copy(unilink::memory::ConstByteSpan data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++try_copy_count_;
    return record_try_result(data.size());
  }

  bool async_try_write_move(std::vector<uint8_t>&& data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++try_move_count_;
    return record_try_result(data.size());
  }

  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    if (!data) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    ++try_shared_count_;
    return record_try_result(data->size());
  }

  void on_bytes(OnBytes cb) override { on_bytes_ = std::move(cb); }
  void on_state(OnState cb) override { on_state_ = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { on_backpressure_ = std::move(cb); }

  unilink::wrapper::RuntimeStats stats() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

  void reset_stats() override {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_ = {};
  }

  void set_try_accepts(bool accepts) { try_accepts_ = accepts; }
  void set_backpressure_active(bool active) { backpressure_active_.store(active); }

  // #509: makes the next `n` async_write_* calls fail (as if rejected by a
  // hard queue-byte cap) regardless of is_backpressure_active(), simulating
  // a write attempt losing the race against wait_for_backpressure_clear().
  void fail_next_writes(int n) {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_next_writes_ = n;
  }

  int write_copy_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_copy_count_;
  }
  int write_move_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_move_count_;
  }
  int write_shared_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_shared_count_;
  }
  int try_copy_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return try_copy_count_;
  }
  int try_move_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return try_move_count_;
  }
  int try_shared_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return try_shared_count_;
  }

 private:
  bool record_try_result(size_t bytes) {
    if (try_accepts_ && !backpressure_active_.load()) {
      stats_.messages_accepted += 1;
      stats_.bytes_accepted += bytes;
      return true;
    }
    stats_.failed_sends += 1;
    return false;
  }

  mutable std::mutex mutex_;
  boost::asio::io_context ioc_;
  bool connected_{true};
  std::atomic<bool> backpressure_active_{false};
  bool try_accepts_{false};
  int fail_next_writes_{0};
  int write_copy_count_{0};
  int write_move_count_{0};
  int write_shared_count_{0};
  int try_copy_count_{0};
  int try_move_count_{0};
  int try_shared_count_{0};
  unilink::wrapper::RuntimeStats stats_{};
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_backpressure_;
};

template <typename Wrapper>
void verify_try_send_is_drop_if_full_escape_hatch(std::string_view name) {
  SCOPED_TRACE(std::string(name));
  auto channel = std::make_shared<ContractChannel>();
  channel->set_try_accepts(false);

  Wrapper wrapper(channel);
  ASSERT_TRUE(wrapper.start().get());

  const auto before = wrapper.stats();
  EXPECT_FALSE(wrapper.try_send("copy"));
  EXPECT_FALSE(wrapper.try_send_move(std::vector<uint8_t>{1, 2, 3}));
  EXPECT_FALSE(wrapper.try_send_shared(std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{4, 5, 6})));

  const auto after = wrapper.stats();
  EXPECT_EQ(channel->try_copy_count(), 1);
  EXPECT_EQ(channel->try_move_count(), 1);
  EXPECT_EQ(channel->try_shared_count(), 1);
  EXPECT_EQ(channel->write_copy_count(), 0);
  EXPECT_EQ(channel->write_move_count(), 0);
  EXPECT_EQ(channel->write_shared_count(), 0);
  EXPECT_EQ(after.pending_bytes, before.pending_bytes);
  EXPECT_EQ(after.failed_sends, before.failed_sends + 3);
}

template <typename Wrapper>
void verify_reliable_send_uses_strategy_aware_write(std::string_view name) {
  SCOPED_TRACE(std::string(name));
  auto channel = std::make_shared<ContractChannel>();
  channel->set_try_accepts(false);

  Wrapper wrapper(channel);
  ASSERT_TRUE(wrapper.start().get());

  EXPECT_TRUE(wrapper.send("copy"));
  EXPECT_TRUE(wrapper.send_move(std::vector<uint8_t>{1, 2, 3}));
  EXPECT_TRUE(wrapper.send_shared(std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{4, 5, 6})));

  EXPECT_EQ(channel->write_copy_count(), 1);
  EXPECT_EQ(channel->write_move_count(), 1);
  EXPECT_EQ(channel->write_shared_count(), 1);
  EXPECT_EQ(channel->try_copy_count(), 0);
  EXPECT_EQ(channel->try_move_count(), 0);
  EXPECT_EQ(channel->try_shared_count(), 0);
}

// Regression test for jwsung91/unilink#431: the blocking Reliable-mode send
// path used to wait on a condition variable unbounded. Since the transport
// clears backpressure_active_ and calls notify_all() without holding the
// wrapper's bp_mutex_, a notification can race a waiter that's mid-way
// through checking the predicate and registering to wait, and get lost - an
// unbounded wait() would then block forever (the same class of bug fixed
// for UDP in #427/#428). Simulates a "lost" notification directly: the
// channel's backpressure flips off with no on_backpressure()/notify_all()
// call at all. A correct fix polls with a bounded timeout regardless of
// notifications, so the blocked call must still return well within a few
// poll intervals; the old unbounded wait() would hang this test forever.
template <typename Wrapper>
void verify_blocking_send_recovers_without_notify(std::string_view name) {
  SCOPED_TRACE(std::string(name));
  auto channel = std::make_shared<ContractChannel>();
  channel->set_backpressure_active(true);

  // Heap-allocated and captured by shared_ptr (not by reference) so that if
  // this test fails - i.e. reproduces the #431 hang - the sender thread can
  // be safely detached rather than joined, without leaving it referencing
  // locals that are about to go out of scope. A permanently-stuck thread
  // can't be joined without hanging the test binary itself.
  auto wrapper = std::make_shared<Wrapper>(channel);
  ASSERT_TRUE(wrapper->start().get());

  auto sent_promise = std::make_shared<std::promise<bool>>();
  auto sent_future = sent_promise->get_future();
  std::thread sender([wrapper, sent_promise] { sent_promise->set_value(wrapper->send("blocked")); });

  // Give the sender thread time to actually enter the wait loop before the
  // "lost notification" state change below.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  channel->set_backpressure_active(false);  // no notify_all() - simulates a missed wakeup

  bool recovered = sent_future.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
  if (recovered) {
    sender.join();
  } else {
    sender.detach();
  }
  ASSERT_TRUE(recovered) << "blocking send did not recover from a missed backpressure notification within a "
                            "bounded number of poll intervals";
  EXPECT_TRUE(sent_future.get());
}

// Regression test for jwsung91/unilink#509: wait_for_backpressure_clear()'s
// condition (is_backpressure_active(), tied to bp_high) and the transport's
// own hard queue-byte cap (bp_limit) are different thresholds checked at
// different times, so a write attempt can still be rejected immediately
// after the wait exits - even though nothing about the channel/connection
// state has actually changed to make the caller give up. Simulates that
// narrow race directly: backpressure is never active (so the wait returns
// immediately), but the first write attempt is made to fail anyway. A
// correct fix retries rather than giving up after one failed attempt.
template <typename Wrapper>
void verify_blocking_send_retries_after_transient_write_rejection(std::string_view name) {
  SCOPED_TRACE(std::string(name));
  auto channel = std::make_shared<ContractChannel>();
  channel->fail_next_writes(1);

  Wrapper wrapper(channel);
  ASSERT_TRUE(wrapper.start().get());

  EXPECT_TRUE(wrapper.send("blocked"))
      << "blocking send gave up after a single transient write rejection instead of retrying";
  EXPECT_GE(channel->write_copy_count(), 2) << "expected at least one retry after the first write was rejected";
}

}  // namespace

TEST(WrapperSendContractTest, BlockingSendRecoversWithoutBackpressureNotification) {
  verify_blocking_send_recovers_without_notify<unilink::wrapper::TcpClient>("TcpClient");
  verify_blocking_send_recovers_without_notify<unilink::wrapper::UdpClient>("UdpClient");
  verify_blocking_send_recovers_without_notify<unilink::wrapper::UdsClient>("UdsClient");
  verify_blocking_send_recovers_without_notify<unilink::wrapper::Serial>("Serial");
}

TEST(WrapperSendContractTest, TrySendVariantsUseExplicitTryWritePath) {
  verify_try_send_is_drop_if_full_escape_hatch<unilink::wrapper::TcpClient>("TcpClient");
  verify_try_send_is_drop_if_full_escape_hatch<unilink::wrapper::UdpClient>("UdpClient");
  verify_try_send_is_drop_if_full_escape_hatch<unilink::wrapper::UdsClient>("UdsClient");
  verify_try_send_is_drop_if_full_escape_hatch<unilink::wrapper::Serial>("Serial");
}

TEST(WrapperSendContractTest, ReliableSendVariantsDoNotUseTryWritePath) {
  verify_reliable_send_uses_strategy_aware_write<unilink::wrapper::TcpClient>("TcpClient");
  verify_reliable_send_uses_strategy_aware_write<unilink::wrapper::UdpClient>("UdpClient");
  verify_reliable_send_uses_strategy_aware_write<unilink::wrapper::UdsClient>("UdsClient");
  verify_reliable_send_uses_strategy_aware_write<unilink::wrapper::Serial>("Serial");
}

TEST(WrapperSendContractTest, BlockingSendRetriesAfterTransientWriteRejection) {
  verify_blocking_send_retries_after_transient_write_rejection<unilink::wrapper::TcpClient>("TcpClient");
  verify_blocking_send_retries_after_transient_write_rejection<unilink::wrapper::UdpClient>("UdpClient");
  verify_blocking_send_retries_after_transient_write_rejection<unilink::wrapper::UdsClient>("UdsClient");
  verify_blocking_send_retries_after_transient_write_rejection<unilink::wrapper::Serial>("Serial");
}
