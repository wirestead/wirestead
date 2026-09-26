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
#include <stdexcept>
#include <type_traits>

#include "wirestead/interface/result_channel.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using wrapper::SendRejection;
using wrapper::SendResult;
namespace {
template <typename W>
constexpr bool public_result_contract() {
  using V = std::string_view;
  using M = std::vector<uint8_t>;
  using S = std::shared_ptr<const M>;
  static_assert(std::is_same_v<decltype(std::declval<W&>().send(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().try_send(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().send_line(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().try_send_line(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().send_blocking(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().send_line_blocking(V{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().send_move(M{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().try_send_move(M{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().send_shared(S{})), SendResult>);
  static_assert(std::is_same_v<decltype(std::declval<W&>().try_send_shared(S{})), SendResult>);
  return true;
}
static_assert(public_result_contract<wrapper::ChannelInterface>());
static_assert(public_result_contract<wrapper::TcpClient>());
static_assert(public_result_contract<wrapper::UdsClient>());
static_assert(public_result_contract<wrapper::UdpClient>());
static_assert(public_result_contract<wrapper::Serial>());

class LegacyChannel : public interface::Channel {
 public:
  int effects = 0;
  void start() override { ++effects; }
  void stop() override { ++effects; }
  bool is_connected() const override { return false; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override {
    ++effects;
    return io.get_executor();
  }
  void on_bytes(OnBytes) override { ++effects; }
  void on_state(OnState) override { ++effects; }
  void on_backpressure(OnBackpressure) override { ++effects; }
  bool async_write_copy(memory::ConstByteSpan) override {
    ++effects;
    return false;
  }
  bool async_write_move(std::vector<uint8_t>&&) override {
    ++effects;
    return false;
  }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override {
    ++effects;
    return false;
  }
  bool async_try_write_copy(memory::ConstByteSpan) override {
    ++effects;
    return false;
  }
  bool async_try_write_move(std::vector<uint8_t>&&) override {
    ++effects;
    return false;
  }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override {
    ++effects;
    return false;
  }

 private:
  boost::asio::io_context io;
};

class AdmissionOnlyChannel : public interface::ResultChannel {
 public:
  int effects = 0;
  void start() override { ++effects; }
  void stop() override { ++effects; }
  bool is_connected() const override { return false; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override {
    ++effects;
    return io.get_executor();
  }
  void on_bytes(OnBytes) override { ++effects; }
  void on_state(OnState) override { ++effects; }
  void on_backpressure(OnBackpressure) override { ++effects; }
  SendResult async_write_copy_result(memory::ConstByteSpan) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }
  SendResult async_write_move_result(std::vector<uint8_t>&&) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }
  SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }
  SendResult async_try_write_copy_result(memory::ConstByteSpan) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }
  SendResult async_try_write_move_result(std::vector<uint8_t>&&) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }
  SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>>) override {
    ++effects;
    return SendResult::reject(SendRejection::QueueFull);
  }

 private:
  boost::asio::io_context io;
};
template <typename W>
class PublicClientResultTest : public ::testing::Test {};
using Clients = ::testing::Types<wrapper::TcpClient, wrapper::UdsClient, wrapper::UdpClient, wrapper::Serial>;
TYPED_TEST_SUITE(PublicClientResultTest, Clients);

template <typename W>
std::unique_ptr<W> unstarted() {
  if constexpr (std::is_same_v<W, wrapper::TcpClient>)
    return std::make_unique<W>("127.0.0.1", 12345);
  else if constexpr (std::is_same_v<W, wrapper::UdsClient>)
    return std::make_unique<W>("unused-client-result-test.sock");
  else if constexpr (std::is_same_v<W, wrapper::UdpClient>)
    return std::make_unique<W>(config::UdpConfig{});
  else
    return std::make_unique<W>("/dev/ttyRESULT", 9600);
}
SendResult send_form(wrapper::ChannelInterface& client, int form, std::vector<uint8_t>& moved,
                     std::shared_ptr<const std::vector<uint8_t>> shared) {
  switch (form) {
    case 0:
      return client.send("data");
    case 1:
      return client.try_send("data");
    case 2:
      return client.send_line("data");
    case 3:
      return client.try_send_line("data");
    case 4:
      return client.send_blocking("data");
    case 5:
      return client.send_line_blocking("data");
    case 6:
      return client.send_move(std::move(moved));
    case 7:
      return client.try_send_move(std::move(moved));
    case 8:
      return client.send_shared(std::move(shared));
    default:
      return client.try_send_shared(std::move(shared));
  }
}
TYPED_TEST(PublicClientResultTest, AllFormsExposeNotStartedThroughCommonInterface) {
  auto client = unstarted<TypeParam>();
  std::vector<uint8_t> moved{1, 2, 3};
  auto shared = std::make_shared<const std::vector<uint8_t>>(moved);
  for (int form = 0; form < 10; ++form) {
    SCOPED_TRACE(form);
    const auto result = send_form(*client, form, moved, shared);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.reason(), SendRejection::NotStarted);
    EXPECT_EQ(moved, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(shared.use_count(), 1);
  }
}
TYPED_TEST(PublicClientResultTest, ValidationPrecedesLifecycleAndLineCountsDelimiter) {
  auto client = unstarted<TypeParam>();
  for (auto result : {client->send(""), client->try_send(""), client->send_blocking(""), client->send_move({}),
                      client->try_send_move({}), client->send_shared(nullptr), client->try_send_shared(nullptr)}) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.reason(), SendRejection::InvalidArgument);
  }
  // Empty lines still contain a valid delimiter.
  EXPECT_EQ(client->send_line("").reason(), SendRejection::NotStarted);
  EXPECT_EQ(client->try_send_line("").reason(), SendRejection::NotStarted);
  const std::string maximum(base::constants::MAX_BUFFER_SIZE, 'x');
  for (auto result :
       {client->send_line(maximum), client->try_send_line(maximum), client->send_line_blocking(maximum)}) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.reason(), SendRejection::TooLarge);
  }
}
TYPED_TEST(PublicClientResultTest, RejectsUnsupportedInjectionBeforeAnySideEffect) {
  auto legacy = std::make_shared<LegacyChannel>();
  auto admission = std::make_shared<AdmissionOnlyChannel>();
  EXPECT_THROW({ TypeParam client(legacy); }, std::invalid_argument);
  EXPECT_THROW({ TypeParam client(admission); }, std::invalid_argument);
  EXPECT_THROW({ TypeParam client(std::shared_ptr<interface::Channel>{}); }, std::invalid_argument);
  EXPECT_EQ(legacy->effects, 0);
  EXPECT_EQ(admission->effects, 0);
  // The low-level legacy Channel surface remains usable directly.
  EXPECT_FALSE(legacy->async_write_copy({}));
  EXPECT_FALSE(admission->async_write_copy({}));
}
}  // namespace
