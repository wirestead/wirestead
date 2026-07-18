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
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "wirestead/interface/channel.hpp"
#include "wirestead/wrapper/serial/serial.hpp"
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/uds_client/uds_client.hpp"

namespace {

class CountingChannel : public wirestead::interface::Channel {
 public:
  void start() override { connected_ = true; }
  void stop() override { connected_ = false; }
  bool is_connected() const override { return connected_; }
  bool is_backpressure_active() const override { return false; }

  boost::asio::any_io_executor get_executor() override { return ioc_.get_executor(); }

  bool async_write_copy(wirestead::memory::ConstByteSpan data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++copy_calls_;
    last_payload_.assign(data.begin(), data.end());
    return write_result_;
  }

  bool async_write_move(std::vector<uint8_t>&& data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++move_calls_;
    last_payload_ = std::move(data);
    return write_result_;
  }

  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++shared_calls_;
    if (data) {
      last_payload_ = *data;
    } else {
      last_payload_.clear();
    }
    return write_result_ && data && !data->empty();
  }

  bool async_try_write_copy(wirestead::memory::ConstByteSpan data) override { return async_write_copy(data); }

  bool async_try_write_move(std::vector<uint8_t>&& data) override { return async_write_move(std::move(data)); }

  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
    return async_write_shared(std::move(data));
  }

  void on_bytes(OnBytes cb) override { on_bytes_ = std::move(cb); }
  void on_state(OnState cb) override { on_state_ = std::move(cb); }
  void on_backpressure(OnBackpressure cb) override { on_backpressure_ = std::move(cb); }

  int copy_calls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return copy_calls_;
  }

  int move_calls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return move_calls_;
  }

  int shared_calls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return shared_calls_;
  }

  std::vector<uint8_t> last_payload() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_payload_;
  }

 private:
  boost::asio::io_context ioc_;
  bool connected_{true};
  bool write_result_{true};
  mutable std::mutex mutex_;
  int copy_calls_{0};
  int move_calls_{0};
  int shared_calls_{0};
  std::vector<uint8_t> last_payload_;
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_backpressure_;
};

template <typename Wrapper>
void expect_try_send_move_uses_move_path() {
  auto channel = std::make_shared<CountingChannel>();
  Wrapper wrapper(channel);

  std::vector<uint8_t> payload{1, 2, 3, 4};
  EXPECT_TRUE(wrapper.try_send_move(std::move(payload)));

  EXPECT_EQ(channel->copy_calls(), 0);
  EXPECT_EQ(channel->move_calls(), 1);
  EXPECT_EQ(channel->shared_calls(), 0);
  EXPECT_EQ(channel->last_payload(), (std::vector<uint8_t>{1, 2, 3, 4}));
}

template <typename Wrapper>
void expect_try_send_shared_uses_shared_path() {
  auto channel = std::make_shared<CountingChannel>();
  Wrapper wrapper(channel);

  auto payload = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{5, 6, 7});
  EXPECT_TRUE(wrapper.try_send_shared(payload));

  EXPECT_EQ(channel->copy_calls(), 0);
  EXPECT_EQ(channel->move_calls(), 0);
  EXPECT_EQ(channel->shared_calls(), 1);
  EXPECT_EQ(channel->last_payload(), (std::vector<uint8_t>{5, 6, 7}));
}

template <typename Wrapper>
void expect_empty_or_null_shared_is_rejected_before_transport() {
  auto channel = std::make_shared<CountingChannel>();
  Wrapper wrapper(channel);

  EXPECT_FALSE(wrapper.try_send_shared(nullptr));
  EXPECT_FALSE(wrapper.try_send_shared(std::make_shared<const std::vector<uint8_t>>()));

  EXPECT_EQ(channel->copy_calls(), 0);
  EXPECT_EQ(channel->move_calls(), 0);
  EXPECT_EQ(channel->shared_calls(), 0);
}

template <typename Wrapper>
void expect_strategy_send_uses_move_and_shared_paths() {
  auto channel = std::make_shared<CountingChannel>();
  Wrapper wrapper(channel);
  wrapper.backpressure_strategy(wirestead::base::constants::BackpressureStrategy::BestEffort);

  EXPECT_TRUE(wrapper.send_move(std::vector<uint8_t>{8, 9}));
  auto shared = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{10, 11});
  EXPECT_TRUE(wrapper.send_shared(shared));

  EXPECT_EQ(channel->copy_calls(), 0);
  EXPECT_EQ(channel->move_calls(), 1);
  EXPECT_EQ(channel->shared_calls(), 1);
}

}  // namespace

TEST(WrapperSendBufferApis, TcpClientRoutesMoveAndSharedToTransportPaths) {
  expect_try_send_move_uses_move_path<wirestead::wrapper::TcpClient>();
  expect_try_send_shared_uses_shared_path<wirestead::wrapper::TcpClient>();
  expect_empty_or_null_shared_is_rejected_before_transport<wirestead::wrapper::TcpClient>();
  expect_strategy_send_uses_move_and_shared_paths<wirestead::wrapper::TcpClient>();
}

TEST(WrapperSendBufferApis, UdpClientRoutesMoveAndSharedToTransportPaths) {
  expect_try_send_move_uses_move_path<wirestead::wrapper::UdpClient>();
  expect_try_send_shared_uses_shared_path<wirestead::wrapper::UdpClient>();
  expect_empty_or_null_shared_is_rejected_before_transport<wirestead::wrapper::UdpClient>();
  expect_strategy_send_uses_move_and_shared_paths<wirestead::wrapper::UdpClient>();
}

TEST(WrapperSendBufferApis, SerialRoutesMoveAndSharedToTransportPaths) {
  expect_try_send_move_uses_move_path<wirestead::wrapper::Serial>();
  expect_try_send_shared_uses_shared_path<wirestead::wrapper::Serial>();
  expect_empty_or_null_shared_is_rejected_before_transport<wirestead::wrapper::Serial>();
  expect_strategy_send_uses_move_and_shared_paths<wirestead::wrapper::Serial>();
}

TEST(WrapperSendBufferApis, UdsClientRoutesMoveAndSharedToTransportPaths) {
  expect_try_send_move_uses_move_path<wirestead::wrapper::UdsClient>();
  expect_try_send_shared_uses_shared_path<wirestead::wrapper::UdsClient>();
  expect_empty_or_null_shared_is_rejected_before_transport<wirestead::wrapper::UdsClient>();
  expect_strategy_send_uses_move_and_shared_paths<wirestead::wrapper::UdsClient>();
}
