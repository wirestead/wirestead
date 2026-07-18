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

#include <vector>

#include "wirestead/framer/packet_framer.hpp"

using namespace wirestead;
using namespace wirestead::framer;

namespace wirestead {
namespace test {

class PacketFramerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    start_ = {'S', 'T'};
    end_ = {'E', 'N'};
    framer_ = std::make_unique<PacketFramer>(start_, end_, 1024);
    framer_->on_message([this](memory::ConstByteSpan msg) {
      std::vector<uint8_t> v(msg.begin(), msg.end());
      messages_.push_back(v);
    });
  }

  std::vector<uint8_t> start_;
  std::vector<uint8_t> end_;
  std::unique_ptr<PacketFramer> framer_;
  std::vector<std::vector<uint8_t>> messages_;
};

TEST_F(PacketFramerTest, SimplePacket) {
  std::vector<uint8_t> data = {'S', 'T', 'D', 'A', 'T', 'A', 'E', 'N'};
  framer_->push_bytes(memory::ConstByteSpan(data.data(), data.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], data);
}

TEST_F(PacketFramerTest, SyncGarbage) {
  std::vector<uint8_t> garbage = {'X', 'Y'};
  std::vector<uint8_t> packet = {'S', 'T', 'D', 'E', 'N'};
  std::vector<uint8_t> data = garbage;
  data.insert(data.end(), packet.begin(), packet.end());

  framer_->push_bytes(memory::ConstByteSpan(data.data(), data.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], packet);
}

TEST_F(PacketFramerTest, SplitPacket) {
  std::vector<uint8_t> part1 = {'S', 'T', 'D'};
  std::vector<uint8_t> part2 = {'A', 'E', 'N'};
  framer_->push_bytes(memory::ConstByteSpan(part1.data(), part1.size()));
  ASSERT_EQ(messages_.size(), 0);
  framer_->push_bytes(memory::ConstByteSpan(part2.data(), part2.size()));
  ASSERT_EQ(messages_.size(), 1);
  std::vector<uint8_t> expected = {'S', 'T', 'D', 'A', 'E', 'N'};
  EXPECT_EQ(messages_[0], expected);
}

TEST_F(PacketFramerTest, MergedPackets) {
  std::vector<uint8_t> p1 = {'S', 'T', '1', 'E', 'N'};
  std::vector<uint8_t> p2 = {'S', 'T', '2', 'E', 'N'};
  std::vector<uint8_t> data = p1;
  data.insert(data.end(), p2.begin(), p2.end());

  framer_->push_bytes(memory::ConstByteSpan(data.data(), data.size()));
  ASSERT_EQ(messages_.size(), 2);
  EXPECT_EQ(messages_[0], p1);
  EXPECT_EQ(messages_[1], p2);
}

TEST_F(PacketFramerTest, MaxLengthExceeded) {
  // Max len 6. "ST12EN" = 6.
  framer_ = std::make_unique<PacketFramer>(start_, end_, 6);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    std::vector<uint8_t> v(msg.begin(), msg.end());
    messages_.push_back(v);
  });

  // Too long: "ST123EN" = 7
  std::vector<uint8_t> bad = {'S', 'T', '1', '2', '3', 'E', 'N'};
  framer_->push_bytes(memory::ConstByteSpan(bad.data(), bad.size()));
  ASSERT_EQ(messages_.size(), 0);

  // Valid: "ST1EN" = 5
  std::vector<uint8_t> valid = {'S', 'T', '1', 'E', 'N'};
  framer_->push_bytes(memory::ConstByteSpan(valid.data(), valid.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], valid);
}

TEST_F(PacketFramerTest, RejectEmptyPatterns) {
  std::vector<uint8_t> empty_start;
  std::vector<uint8_t> empty_end;
  EXPECT_THROW({ PacketFramer(empty_start, empty_end, 1024); }, std::invalid_argument);
}

// --- Coverage & Edge Case Tests ---

TEST_F(PacketFramerTest, EmptyDataInput) {
  int count = 0;
  framer_->on_message([&](memory::ConstByteSpan) { count++; });
  framer_->push_bytes(memory::ConstByteSpan(nullptr, 0));
  EXPECT_EQ(count, 0);
}

TEST_F(PacketFramerTest, FastPathEmptyEndPattern) {
  PacketFramer framer({0x01}, {}, 1024);
  std::vector<uint8_t> messages;
  framer.on_message([&](memory::ConstByteSpan data) { messages.insert(messages.end(), data.begin(), data.end()); });

  std::vector<uint8_t> input = {0x01, 0x01, 0x01};
  framer.push_bytes(memory::ConstByteSpan(input.data(), input.size()));
  EXPECT_EQ(messages.size(), 3);
}

TEST_F(PacketFramerTest, PartialMatchAtEnd) {
  PacketFramer framer({0x01, 0x02}, {0x03}, 1024);
  std::vector<uint8_t> input = {0x00, 0x01};  // 0x01 is partial start match
  framer.push_bytes(memory::ConstByteSpan(input.data(), input.size()));

  // Now push the rest
  input = {0x02, 0x00, 0x03};  // Completes 01 02 ... 03
  int count = 0;
  framer.on_message([&](memory::ConstByteSpan) { count++; });
  framer.push_bytes(memory::ConstByteSpan(input.data(), input.size()));
  EXPECT_EQ(count, 1);
}

TEST_F(PacketFramerTest, BufferPathEmptyStartPattern) {
  PacketFramer framer({}, {0x02}, 1024);  // empty start
  int count = 0;
  framer.on_message([&](memory::ConstByteSpan) { count++; });

  // First push partial to trigger buffer path
  std::vector<uint8_t> input1 = {0x01};
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  // Then push end
  std::vector<uint8_t> input2 = {0x02};
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));
  EXPECT_EQ(count, 1);
}

TEST_F(PacketFramerTest, BufferPathStartPatternNotFoundPartialKeep) {
  PacketFramer framer({0x01, 0x02}, {0x03}, 1024);
  // Buffer path: push data that does not contain start pattern but ends with partial
  std::vector<uint8_t> input1 = {0x00, 0x00};  // Force buffer path
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  std::vector<uint8_t> input2 = {0x05, 0x05, 0x01};  // Ends with partial 0x01
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));

  // Next push completes start pattern
  std::vector<uint8_t> input3 = {0x02, 0x03};
  int count = 0;
  framer.on_message([&](memory::ConstByteSpan) { count++; });
  framer.push_bytes(memory::ConstByteSpan(input3.data(), input3.size()));
  EXPECT_EQ(count, 1);
}

TEST_F(PacketFramerTest, BufferPathDiscardsGarbageBeforeStart) {
  PacketFramer framer({0x01, 0x02}, {0x03}, 1024);
  std::vector<std::vector<uint8_t>> messages;
  framer.on_message([&](memory::ConstByteSpan data) { messages.emplace_back(data.begin(), data.end()); });

  std::vector<uint8_t> input1 = {0x00, 0x01};
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  std::vector<uint8_t> input2 = {0x00, 0x01, 0x02, 0x7F, 0x03};
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));

  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], (std::vector<uint8_t>{0x01, 0x02, 0x7F, 0x03}));
}

TEST_F(PacketFramerTest, BufferPathEmptyEndPatternCompletesPartialStart) {
  PacketFramer framer({0x01, 0x02}, {}, 1024);
  std::vector<std::vector<uint8_t>> messages;
  framer.on_message([&](memory::ConstByteSpan data) { messages.emplace_back(data.begin(), data.end()); });

  std::vector<uint8_t> input1 = {0x01};
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  std::vector<uint8_t> input2 = {0x02, 0x99};
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));

  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], (std::vector<uint8_t>{0x01, 0x02}));
}

TEST_F(PacketFramerTest, BufferPathDiscardsPacketThatExceedsMaxLengthWhenEndArrives) {
  PacketFramer framer({'S'}, {'E'}, 4);
  std::vector<std::vector<uint8_t>> messages;
  framer.on_message([&](memory::ConstByteSpan data) { messages.emplace_back(data.begin(), data.end()); });

  std::vector<uint8_t> input1 = {'S', 'a', 'b'};
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  std::vector<uint8_t> input2 = {'c', 'E', 'S', 'E'};
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));

  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], (std::vector<uint8_t>{'S', 'E'}));
}

TEST_F(PacketFramerTest, BufferPathClearsCollectedPacketWhenMaxLengthExceededWithoutEnd) {
  PacketFramer framer({'S'}, {'E'}, 3);
  std::vector<std::vector<uint8_t>> messages;
  framer.on_message([&](memory::ConstByteSpan data) { messages.emplace_back(data.begin(), data.end()); });

  std::vector<uint8_t> input1 = {'S', 'a', 'b', 'c'};
  framer.push_bytes(memory::ConstByteSpan(input1.data(), input1.size()));

  std::vector<uint8_t> input2 = {'d'};
  framer.push_bytes(memory::ConstByteSpan(input2.data(), input2.size()));

  std::vector<uint8_t> valid = {'S', 'E'};
  framer.push_bytes(memory::ConstByteSpan(valid.data(), valid.size()));

  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], valid);
}

TEST_F(PacketFramerTest, ResetClearsState) {
  framer_->push_bytes(memory::ConstByteSpan(std::vector<uint8_t>{0x01, 0x00}.data(), 2));
  framer_->reset();
  // After reset, pushing 0x02 should NOT trigger message because 0x01 was cleared
  int count = 0;
  framer_->on_message([&](memory::ConstByteSpan) { count++; });
  // Set start pattern for this test to be something known
  start_ = {0x01};
  end_ = {0x02};
  framer_ = std::make_unique<PacketFramer>(start_, end_, 1024);
  framer_->on_message([&](memory::ConstByteSpan) { count++; });

  framer_->push_bytes(memory::ConstByteSpan(std::vector<uint8_t>{0x01, 0x00}.data(), 2));
  framer_->reset();
  framer_->push_bytes(memory::ConstByteSpan(std::vector<uint8_t>{0x02}.data(), 1));
  EXPECT_EQ(count, 0);
}

}  // namespace test
}  // namespace wirestead
