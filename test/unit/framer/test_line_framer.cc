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

#include <string>
#include <vector>

#include "wirestead/framer/line_framer.hpp"

using namespace wirestead;
using namespace wirestead::framer;

namespace wirestead {
namespace test {

class LineFramerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    framer_ = std::make_unique<LineFramer>("\n", false, 1024);
    framer_->on_message([this](memory::ConstByteSpan msg) {
      std::string s(reinterpret_cast<const char*>(msg.data()), msg.size());
      messages_.push_back(s);
    });
  }

  std::unique_ptr<LineFramer> framer_;
  std::vector<std::string> messages_;
};

TEST_F(LineFramerTest, SingleMessage) {
  std::string data = "Hello\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "Hello");
}

TEST_F(LineFramerTest, SplitMessage) {
  std::string part1 = "He";
  std::string part2 = "llo\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part1.data()), part1.size()));
  ASSERT_EQ(messages_.size(), 0);
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part2.data()), part2.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "Hello");
}

TEST_F(LineFramerTest, MergedMessages) {
  std::string data = "Msg1\nMsg2\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  ASSERT_EQ(messages_.size(), 2);
  EXPECT_EQ(messages_[0], "Msg1");
  EXPECT_EQ(messages_[1], "Msg2");
}

TEST_F(LineFramerTest, IncludeDelimiter) {
  framer_ = std::make_unique<LineFramer>("\n", true, 1024);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    std::string s(reinterpret_cast<const char*>(msg.data()), msg.size());
    messages_.push_back(s);
  });

  std::string data = "Hello\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "Hello\n");
}

TEST_F(LineFramerTest, MaxLengthReset) {
  // Max length 5. "12345" is ok. "123456" triggers reset.
  framer_ = std::make_unique<LineFramer>("\n", false, 5);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    std::string s(reinterpret_cast<const char*>(msg.data()), msg.size());
    messages_.push_back(s);
  });

  std::string data = "123456";  // No delimiter, exceeds max
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
  // Should have cleared the buffer and entered discard-until-delimiter mode.

  // The bytes up to the next delimiter might be the untransmitted tail of
  // the message that was just discarded, so they must not be delivered as a
  // fresh, trusted message (jwsung91/wirestead review finding: emitting them
  // would silently hand the caller corrupted/truncated data).
  std::string maybe_tail_of_discarded_message = "Hi\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(maybe_tail_of_discarded_message.data()),
                                            maybe_tail_of_discarded_message.size()));
  EXPECT_EQ(messages_.size(), 0);

  // Once resynchronized past that first post-overflow delimiter, normal
  // delivery resumes for subsequent messages.
  std::string valid = "Hi\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(valid.data()), valid.size()));

  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "Hi");
}

TEST_F(LineFramerTest, LargeChunkProcessing) {
  // Create a payload larger than max_length but consisting of valid short lines
  std::string large_payload;
  large_payload.reserve(2048);
  int expected_count = 0;

  for (int i = 0; i < 100; ++i) {
    std::string line = "Line" + std::to_string(i) + "\n";
    if (large_payload.size() + line.size() > 2000) break;
    large_payload += line;
    expected_count++;
  }

  // Pass the entire payload at once. Total size > 1024 (max_length).
  framer_->push_bytes(
      memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(large_payload.data()), large_payload.size()));

  ASSERT_EQ(messages_.size(), expected_count);
  EXPECT_EQ(messages_[0], "Line0");
  EXPECT_EQ(messages_.back(), "Line" + std::to_string(expected_count - 1));
}

TEST_F(LineFramerTest, HugeLineRejection) {
  std::string valid1 = "Valid1\n";
  std::string huge_line(2000, 'A');
  huge_line += "\n";
  std::string valid2 = "Valid2\n";

  std::string payload = valid1 + huge_line + valid2;

  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));

  bool found_valid1 = false;
  bool found_valid2 = false;
  bool found_huge = false;

  for (const auto& msg : messages_) {
    if (msg == "Valid1") found_valid1 = true;
    if (msg == "Valid2") found_valid2 = true;
    if (msg.length() >= 2000) found_huge = true;
  }

  EXPECT_TRUE(found_valid1) << "Valid1 not found";
  EXPECT_FALSE(found_huge) << "Huge line should be rejected";
  EXPECT_TRUE(found_valid2) << "Valid2 not found";
}

TEST_F(LineFramerTest, SplitDelimiter) {
  // Delimiter "\r\n"
  framer_ = std::make_unique<LineFramer>("\r\n", false, 1024);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    std::string s(reinterpret_cast<const char*>(msg.data()), msg.size());
    messages_.push_back(s);
  });

  std::string part1 = "Hello\r";
  std::string part2 = "\nWorld\r\n";

  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part1.data()), part1.size()));
  ASSERT_EQ(messages_.size(), 0);

  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part2.data()), part2.size()));
  ASSERT_EQ(messages_.size(), 2);
  EXPECT_EQ(messages_[0], "Hello");
  EXPECT_EQ(messages_[1], "World");
}

TEST_F(LineFramerTest, ResetFromCallbackWhileScanningBufferedData) {
  framer_->on_message([this](memory::ConstByteSpan msg) {
    messages_.emplace_back(reinterpret_cast<const char*>(msg.data()), msg.size());
    framer_->reset();
  });

  std::string part1 = "ab";  // buffered, so the next push scans buffer_ itself
  std::string part2 = "c\nd\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part1.data()), part1.size()));
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part2.data()), part2.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "abc");

  std::string next = "ok\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(next.data()), next.size()));
  ASSERT_EQ(messages_.size(), 2);
  EXPECT_EQ(messages_[1], "ok");
}

TEST_F(LineFramerTest, OverflowTailIsNotDeliveredAsAMessage) {
  // Max length 5. A 9-byte message split across two calls overflows on the
  // first call (6 bytes, no delimiter yet); its last 3 bytes plus a
  // delimiter arrive in the second call.
  framer_ = std::make_unique<LineFramer>("\n", false, 5);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    std::string s(reinterpret_cast<const char*>(msg.data()), msg.size());
    messages_.push_back(s);
  });

  std::string overflow_head = "123456";  // No delimiter, exceeds max_length (5)
  framer_->push_bytes(
      memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(overflow_head.data()), overflow_head.size()));
  ASSERT_EQ(messages_.size(), 0);

  std::string overflow_tail = "789\n";  // The rest of the SAME discarded message
  framer_->push_bytes(
      memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(overflow_tail.data()), overflow_tail.size()));

  // Before the fix, this would incorrectly deliver "789" as if it were a
  // complete, valid message - it's actually a fragment of the 9-byte message
  // that was already discarded for exceeding max_length.
  EXPECT_EQ(messages_.size(), 0);

  // A genuinely new message after resynchronization is delivered normally.
  std::string next = "ok\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(next.data()), next.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "ok");
}

TEST_F(LineFramerTest, MaxLengthMessageWithSplitMultiByteDelimiterIsDelivered) {
  // A max_length body whose "\r\n" is split across pushes leaves max_length + 1
  // bytes buffered. That tail is still a valid message, not an overflow.
  framer_ = std::make_unique<LineFramer>("\r\n", false, 10);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    messages_.emplace_back(reinterpret_cast<const char*>(msg.data()), msg.size());
  });

  std::string part1 = "0123456789\r";
  std::string part2 = "\nok\r\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part1.data()), part1.size()));
  ASSERT_EQ(messages_.size(), 0);
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part2.data()), part2.size()));
  ASSERT_EQ(messages_.size(), 2);
  EXPECT_EQ(messages_[0], "0123456789");
  EXPECT_EQ(messages_[1], "ok");
}

TEST_F(LineFramerTest, OverMaxLengthWithSplitMultiByteDelimiterIsStillRejected) {
  framer_ = std::make_unique<LineFramer>("\r\n", false, 10);
  framer_->on_message([this](memory::ConstByteSpan msg) {
    messages_.emplace_back(reinterpret_cast<const char*>(msg.data()), msg.size());
  });

  // 11 bytes fit the split-delimiter allowance while buffered, but the body
  // still exceeds max_length once its delimiter arrives.
  std::string part1 = "0123456789A";
  std::string part2 = "\r\nok\r\n";
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part1.data()), part1.size()));
  framer_->push_bytes(memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(part2.data()), part2.size()));
  ASSERT_EQ(messages_.size(), 1);
  EXPECT_EQ(messages_[0], "ok");
}

}  // namespace test
}  // namespace wirestead
