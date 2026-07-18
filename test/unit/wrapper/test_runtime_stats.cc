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

#include <boost/asio/io_context.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "wirestead/interface/channel.hpp"
#include "wirestead/wirestead.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"

namespace {

class StatsChannel : public wirestead::interface::Channel {
 public:
  void start() override { connected_ = true; }
  void stop() override { connected_ = false; }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return stats_.backpressure_active; }

  boost::asio::any_io_executor get_executor() override { return ioc_.get_executor(); }

  bool async_write_copy(wirestead::memory::ConstByteSpan data) override {
    if (fail_next_write_) {
      fail_next_write_ = false;
      ++stats_.failed_sends;
      return false;
    }
    ++stats_.messages_accepted;
    stats_.bytes_accepted += data.size();
    stats_.queued_bytes += data.size();
    stats_.max_queued_bytes = std::max(stats_.max_queued_bytes, stats_.queued_bytes);
    return true;
  }

  bool async_write_move(std::vector<uint8_t>&& data) override {
    return async_write_copy(wirestead::memory::ConstByteSpan(data.data(), data.size()));
  }

  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return data ? async_write_copy(wirestead::memory::ConstByteSpan(data->data(), data->size())) : false;
  }

  bool async_try_write_copy(wirestead::memory::ConstByteSpan data) override { return async_write_copy(data); }

  bool async_try_write_move(std::vector<uint8_t>&& data) override { return async_write_move(std::move(data)); }

  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return async_write_shared(std::move(data));
  }

  void on_bytes(OnBytes cb) override { on_bytes_ = std::move(cb); }
  void on_state(OnState cb) override { on_state_ = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { on_backpressure_ = std::move(cb); }

  wirestead::wrapper::RuntimeStats stats() const override { return stats_; }

  void reset_stats() override {
    const auto queued = stats_.queued_bytes;
    const auto pending = stats_.pending_bytes;
    const auto active = stats_.backpressure_active;
    stats_ = {};
    stats_.queued_bytes = queued;
    stats_.pending_bytes = pending;
    stats_.max_queued_bytes = queued + pending;
    stats_.backpressure_active = active;
  }

  void fail_next_write() { fail_next_write_ = true; }

  void emit_bytes(std::string_view data) {
    ++stats_.messages_received;
    stats_.bytes_received += data.size();
    if (on_bytes_) {
      on_bytes_(wirestead::memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
    }
  }

 private:
  boost::asio::io_context ioc_;
  bool connected_{true};
  bool fail_next_write_{false};
  wirestead::wrapper::RuntimeStats stats_;
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_backpressure_;
};

}  // namespace

TEST(RuntimeStats, PublicAliasIsDefaultZero) {
  wirestead::RuntimeStats stats;

  EXPECT_EQ(stats.messages_accepted, 0u);
  EXPECT_EQ(stats.bytes_accepted, 0u);
  EXPECT_EQ(stats.messages_sent, 0u);
  EXPECT_EQ(stats.bytes_sent, 0u);
  EXPECT_EQ(stats.messages_received, 0u);
  EXPECT_EQ(stats.bytes_received, 0u);
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.dropped_messages, 0u);
  EXPECT_EQ(stats.dropped_bytes, 0u);
  EXPECT_EQ(stats.backpressure_events, 0u);
  EXPECT_EQ(stats.queued_bytes, 0u);
  EXPECT_EQ(stats.pending_bytes, 0u);
  EXPECT_EQ(stats.max_queued_bytes, 0u);
  EXPECT_FALSE(stats.backpressure_active);
}

TEST(RuntimeStats, TcpClientWrapperForwardsStatsSnapshot) {
  auto channel = std::make_shared<StatsChannel>();
  wirestead::wrapper::TcpClient client(channel);

  ASSERT_TRUE(client.try_send("abc"));
  auto stats = client.stats();
  EXPECT_EQ(stats.messages_accepted, 1u);
  EXPECT_EQ(stats.bytes_accepted, 3u);
  EXPECT_EQ(stats.queued_bytes, 3u);
  EXPECT_EQ(stats.max_queued_bytes, 3u);

  channel->fail_next_write();
  EXPECT_FALSE(client.try_send("x"));
  stats = client.stats();
  EXPECT_EQ(stats.failed_sends, 1u);

  bool received = false;
  client.on_data([&](const wirestead::wrapper::MessageContext& ctx) {
    received = true;
    EXPECT_EQ(ctx.data(), "rx");
  });
  channel->emit_bytes("rx");
  EXPECT_TRUE(received);
  stats = client.stats();
  EXPECT_EQ(stats.messages_received, 1u);
  EXPECT_EQ(stats.bytes_received, 2u);
}

TEST(RuntimeStats, ResetClearsCountersButKeepsGauges) {
  auto channel = std::make_shared<StatsChannel>();
  wirestead::wrapper::TcpClient client(channel);

  ASSERT_TRUE(client.try_send("abcd"));
  channel->emit_bytes("rx");

  client.reset_stats();
  auto stats = client.stats();
  EXPECT_EQ(stats.messages_accepted, 0u);
  EXPECT_EQ(stats.bytes_accepted, 0u);
  EXPECT_EQ(stats.messages_received, 0u);
  EXPECT_EQ(stats.bytes_received, 0u);
  EXPECT_EQ(stats.failed_sends, 0u);
  EXPECT_EQ(stats.queued_bytes, 4u);
  EXPECT_EQ(stats.max_queued_bytes, 4u);
}
