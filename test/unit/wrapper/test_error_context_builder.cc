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

#include "unilink/wrapper/error_context_builder.hpp"

using namespace unilink;
using namespace unilink::wrapper;
using namespace unilink::diagnostics;

namespace {

// Minimal fake exercising only what build_error_context() touches
// (last_error_info()); every other pure virtual is a trivial stub.
class FakeChannel : public interface::Channel {
 public:
  void start() override {}
  void stop() override {}
  bool is_connected() const override { return false; }
  bool is_backpressure_active() const override { return false; }
  boost::asio::any_io_executor get_executor() override { return {}; }
  bool async_write_copy(memory::ConstByteSpan) override { return false; }
  bool async_write_move(std::vector<uint8_t>&&) override { return false; }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override { return false; }
  bool async_try_write_copy(memory::ConstByteSpan) override { return false; }
  bool async_try_write_move(std::vector<uint8_t>&&) override { return false; }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>>) override { return false; }
  void on_bytes(OnBytes) override {}
  void on_state(OnState) override {}
  void on_backpressure(OnBackpressure) override {}

  std::optional<ErrorInfo> last_error_info() const override { return error_; }

  std::optional<ErrorInfo> error_;
};

}  // namespace

TEST(ErrorContextBuilderTest, FallsBackToGenericMessageWhenNoDetailAvailable) {
  FakeChannel channel;  // error_ stays nullopt, matching an unmigrated transport

  auto ctx = wrapper::detail::build_error_context(channel, "Connection error");

  EXPECT_EQ(ctx.code(), ErrorCode::IoError);
  EXPECT_EQ(ctx.message(), "Connection error");
  EXPECT_FALSE(ctx.client_id().has_value());
}

TEST(ErrorContextBuilderTest, UsesDetailedInfoWhenAvailable) {
  FakeChannel channel;
  channel.error_ = ErrorInfo(ErrorLevel::ERROR, ErrorCategory::CONNECTION, "test_component", "connect",
                             "Connection refused", boost::asio::error::connection_refused, true);

  auto ctx = wrapper::detail::build_error_context(channel, "Connection error");

  EXPECT_EQ(ctx.code(), ErrorCode::ConnectionRefused);
  EXPECT_EQ(ctx.message(), "Connection refused");
}

TEST(ErrorContextBuilderTest, ThreadsClientIdThroughBothPaths) {
  FakeChannel channel;

  auto fallback_ctx = wrapper::detail::build_error_context(channel, "Server error", ClientId{7});
  ASSERT_TRUE(fallback_ctx.client_id().has_value());
  EXPECT_EQ(*fallback_ctx.client_id(), 7u);

  channel.error_ = ErrorInfo(ErrorLevel::ERROR, ErrorCategory::CONNECTION, "test_component", "read", "Read failed");
  auto detailed_ctx = wrapper::detail::build_error_context(channel, "Server error", ClientId{7});
  ASSERT_TRUE(detailed_ctx.client_id().has_value());
  EXPECT_EQ(*detailed_ctx.client_id(), 7u);
}
