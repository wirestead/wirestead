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

#include "wirestead/wrapper/bounded_receive.hpp"

using namespace wirestead;
using namespace wirestead::wrapper;
using namespace wirestead::wrapper::detail;
namespace {
memory::ConstByteSpan bytes(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
struct Receiver {
  std::shared_ptr<ReceiveBudget> budget;
  ReceiveState state;
  explicit Receiver(ReceiveLimits limits = {})
      : budget(std::make_shared<ReceiveBudget>(limits)), state(budget->open_scope()) {}
};

TEST(BoundedReceiveTest, RawBatchRemainsChargedThroughCallbackOwnership) {
  Receiver r;
  ReceiveBatch batch;
  {
    auto input = prepare_receive(r.state, {}, 7, bytes("abc"), true);
    batch.emplace_back(std::move(*input.raw));
  }
  const auto charged = r.budget->stats().reserved_bytes;
  EXPECT_GT(charged, 3u);
  auto delivery = std::move(batch);
  batch.clear();
  EXPECT_EQ(r.budget->stats().reserved_bytes, charged);
  const std::vector<MessageContext>& messages = delivery;
  EXPECT_EQ(messages[0].data(), "abc");
  EXPECT_EQ(messages[0].client_id(), 7u);
  delivery.clear();
  EXPECT_EQ(r.budget->stats().reserved_bytes, 0u);
}

TEST(BoundedReceiveTest, PreparingDoesNotInvokeUserMessageCallbacks) {
  Receiver r;
  auto f = std::make_shared<framer::LineFramer>();
  std::vector<std::string> received;
  f->on_message(
      [&](memory::ConstByteSpan b) { received.emplace_back(reinterpret_cast<const char*>(b.data()), b.size()); });
  auto input = prepare_receive(r.state, f, 1, bytes("one\ntwo\n"), false);
  EXPECT_TRUE(received.empty());
  input.deliver();
  EXPECT_EQ(received, (std::vector<std::string>{"one", "two"}));
}

TEST(BoundedReceiveTest, OverflowPreservesExistingPartialFrameAndExposesNothing) {
  Receiver r({4096, 4, 2});
  auto f = std::make_shared<framer::LineFramer>();
  std::string received;
  f->on_message([&](memory::ConstByteSpan b) { received.append(reinterpret_cast<const char*>(b.data()), b.size()); });
  prepare_receive(r.state, f, 1, bytes("ab"), false).deliver();
  auto before = r.budget->stats().reserved_bytes;
  EXPECT_THROW(prepare_receive(r.state, f, 1, bytes("cdef\n"), false), ReceiveOverflow);
  EXPECT_TRUE(received.empty());
  EXPECT_EQ(ReceiveFramerAccess::size(f.get()), 2u);
  EXPECT_EQ(r.budget->stats().reserved_bytes, before);
  prepare_receive(r.state, f, 1, bytes("\n"), false).deliver();
  EXPECT_EQ(received, "ab");
}

TEST(BoundedReceiveTest, LaterMessageOverflowRollsBackWholeInput) {
  Receiver r({1200, 100, 2});
  auto f = std::make_shared<framer::LineFramer>();
  size_t calls = 0;
  f->on_message([&](auto) { ++calls; });
  // Many complete frames exhaust message-record allowance, not frame length.
  EXPECT_THROW(prepare_receive(r.state, f, 1, bytes("a\nb\nc\nd\ne\n"), false), ReceiveOverflow);
  EXPECT_EQ(calls, 0u);
  EXPECT_EQ(ReceiveFramerAccess::size(f.get()), 0u);
  EXPECT_EQ(r.budget->stats().reserved_bytes, 0u);
}

TEST(BoundedReceiveTest, MessageBatchAdoptsAlreadyReservedStorage) {
  Receiver r;
  ReceiveBatch batch;
  auto f = std::make_shared<framer::LineFramer>();
  f->on_message([&](auto b) {
    auto prepared = take_prepared_message(3, b);
    ASSERT_TRUE(prepared);
    batch.emplace_back(std::move(*prepared));
  });
  size_t prepared_bytes = 0;
  {
    auto input = prepare_receive(r.state, f, 3, bytes("one\ntwo\n"), false);
    prepared_bytes = r.budget->stats().reserved_bytes;
    input.deliver();
    EXPECT_EQ(r.budget->stats().reserved_bytes, prepared_bytes);
  }
  EXPECT_EQ(batch.size(), 2u);
  EXPECT_GT(r.budget->stats().reserved_bytes, 0u);
  batch.clear();
  r.state.reset(f.get());
  EXPECT_EQ(r.budget->stats().reserved_bytes, 0u);
}

TEST(BoundedReceiveTest, FailedRawReservationDoesNotMutateFramer) {
  Receiver r({100, 50, 2});
  auto f = std::make_shared<framer::LineFramer>();
  EXPECT_THROW(prepare_receive(r.state, f, 1, bytes("ok\n"), true), ReceiveOverflow);
  EXPECT_EQ(ReceiveFramerAccess::size(f.get()), 0u);
  EXPECT_EQ(r.budget->stats().reserved_bytes, 0u);
}

TEST(BoundedReceiveTest, LengthPrefixPayloadAtOldMaximumStillFitsDefaultLimit) {
  Receiver r;
  auto f = std::make_shared<framer::LengthPrefixFramer>(4);
  std::vector<uint8_t> packet(65540, 'x');
  packet[0] = 0;
  packet[1] = 1;
  packet[2] = 0;
  packet[3] = 0;
  size_t length = 0;
  f->on_message([&](auto b) { length = b.size(); });
  auto input = prepare_receive(r.state, f, 1, memory::ConstByteSpan(packet), false);
  input.deliver();
  EXPECT_EQ(length, 65536u);
  r.state.reset(f.get());
  EXPECT_EQ(ReceiveFramerAccess::capacity(f.get()), 0u);
}

TEST(BoundedReceiveTest, ScopedPreparedOwnershipRestoresAfterNestedCallback) {
  Receiver r;
  auto outer = retain_received(r.state.scope, 1, bytes("outer"));
  auto inner = retain_received(r.state.scope, 2, bytes("inner"));
  EXPECT_EQ(prepared_message, nullptr);
  {
    PreparedMessageGuard a(outer);
    EXPECT_EQ(prepared_message, &outer);
    {
      PreparedMessageGuard b(inner);
      EXPECT_EQ(prepared_message, &inner);
    }
    EXPECT_EQ(prepared_message, &outer);
  }
  EXPECT_EQ(prepared_message, nullptr);
}

TEST(BoundedReceiveTest, NestedRawInputCannotStealPreparedMessageOwnership) {
  Receiver first, second;
  auto outer = retain_received(first.state.scope, 0, bytes("outer"));
  auto borrowed = outer.context.safe_data().as_span();
  const auto charged = first.budget->stats().reserved_bytes;
  {
    PreparedMessageGuard guard(outer);
    auto nested = retain_received(second.state.scope, 0, borrowed);
    EXPECT_TRUE(outer.charge);
    EXPECT_EQ(first.budget->stats().reserved_bytes, charged);
    EXPECT_GT(second.budget->stats().reserved_bytes, 0u);
    EXPECT_EQ(nested.context.data(), "outer");
  }
  EXPECT_EQ(outer.context.data(), "outer");
  EXPECT_EQ(second.budget->stats().reserved_bytes, 0u);
}
TEST(BoundedReceiveTest, MessageBoundaryConsumesTokenBeforeUserReentry) {
  Receiver r;
  auto message = retain_received(r.state.scope, 0, bytes("value"));
  auto borrowed = message.context.safe_data().as_span();
  PreparedMessageGuard guard(message);
  auto owner = take_prepared_message(0, borrowed);
  ASSERT_TRUE(owner);
  EXPECT_FALSE(take_prepared_message(0, borrowed));
  EXPECT_EQ(owner->context.data(), "value");
}
}  // namespace
