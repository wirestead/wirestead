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

#include <stdexcept>
#include <string>
#include <vector>

#include "wirestead/framer/length_prefix_framer.hpp"

using wirestead::framer::LengthPrefixFramer;
using Endian = wirestead::framer::LengthPrefixFramer::Endian;

namespace {

std::vector<std::string> collect(LengthPrefixFramer& framer, const std::vector<uint8_t>& bytes) {
  std::vector<std::string> out;
  framer.on_message([&](wirestead::memory::ConstByteSpan msg) {
    out.emplace_back(reinterpret_cast<const char*>(msg.data()), msg.size());
  });
  framer.push_bytes(wirestead::memory::ConstByteSpan(bytes.data(), bytes.size()));
  return out;
}

}  // namespace

// The entire reason this framer exists: the payload may contain any byte,
// including whatever a delimiter-based framer would have stopped at.
TEST(LengthPrefixFramerTest, PayloadMayContainAnyByteValue) {
  LengthPrefixFramer framer(2, Endian::Big, 65536);

  // A payload full of the bytes PacketFramer would treat as structure.
  const std::vector<uint8_t> payload = {0x00, 0xFF, 0x0A, 0x0D, 0xFF, 0xFF, 0x00};
  std::vector<uint8_t> wire = {0x00, static_cast<uint8_t>(payload.size())};
  wire.insert(wire.end(), payload.begin(), payload.end());

  std::vector<std::vector<uint8_t>> got;
  framer.on_message([&](wirestead::memory::ConstByteSpan m) { got.emplace_back(m.data(), m.data() + m.size()); });
  framer.push_bytes(wirestead::memory::ConstByteSpan(wire.data(), wire.size()));

  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0], payload);
}

TEST(LengthPrefixFramerTest, ExtractsBackToBackFramesFromOneChunk) {
  LengthPrefixFramer framer(2, Endian::Big);
  const std::vector<uint8_t> wire = {0x00, 0x02, 'h', 'i', 0x00, 0x03, 'y', 'e', 's'};

  const auto msgs = collect(framer, wire);
  ASSERT_EQ(msgs.size(), 2u);
  EXPECT_EQ(msgs[0], "hi");
  EXPECT_EQ(msgs[1], "yes");
}

// A stream arrives in whatever chunks the transport chose, including one that
// splits the length field itself.
TEST(LengthPrefixFramerTest, ReassemblesAcrossChunkBoundaries) {
  LengthPrefixFramer framer(2, Endian::Big);
  std::vector<std::string> msgs;
  framer.on_message([&](wirestead::memory::ConstByteSpan m) {
    msgs.emplace_back(reinterpret_cast<const char*>(m.data()), m.size());
  });

  const std::vector<std::vector<uint8_t>> chunks = {{0x00}, {0x05, 'h'}, {'e', 'l'}, {'l', 'o'}};
  for (const auto& c : chunks) {
    framer.push_bytes(wirestead::memory::ConstByteSpan(c.data(), c.size()));
  }

  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "hello");
}

TEST(LengthPrefixFramerTest, HonoursLittleEndianLengths) {
  LengthPrefixFramer framer(2, Endian::Little);
  const std::vector<uint8_t> wire = {0x03, 0x00, 'a', 'b', 'c'};

  const auto msgs = collect(framer, wire);
  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "abc");
}

// Both conventions exist for what the length counts, and getting it wrong
// shifts every frame by the prefix width - so it has to be selectable.
TEST(LengthPrefixFramerTest, LengthCanIncludeThePrefixItself) {
  LengthPrefixFramer framer(2, Endian::Big, 65536, /*length_includes_prefix=*/true);
  const std::vector<uint8_t> wire = {0x00, 0x06, 'a', 'b', 'c', 'd'};  // 2 header + 4 payload

  const auto msgs = collect(framer, wire);
  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "abcd");
}

// A corrupt or hostile header must not be allowed to size an allocation.
TEST(LengthPrefixFramerTest, OversizedDeclaredLengthIsDroppedNotAllocated) {
  LengthPrefixFramer framer(4, Endian::Big, /*max_length=*/16);
  // Declares ~4 GiB of payload.
  const std::vector<uint8_t> wire = {0xFF, 0xFF, 0xFF, 0xFF, 'x'};

  const auto msgs = collect(framer, wire);
  EXPECT_TRUE(msgs.empty());

  // Framing restarts cleanly afterwards.
  LengthPrefixFramer fresh(4, Endian::Big, 16);
  const std::vector<uint8_t> good = {0x00, 0x00, 0x00, 0x02, 'o', 'k'};
  const auto after = collect(fresh, good);
  ASSERT_EQ(after.size(), 1u);
  EXPECT_EQ(after[0], "ok");
}

// A frame claiming to be shorter than its own header cannot be either.
TEST(LengthPrefixFramerTest, LengthShorterThanThePrefixIsRejected) {
  LengthPrefixFramer framer(4, Endian::Big, 65536, /*length_includes_prefix=*/true);
  const std::vector<uint8_t> wire = {0x00, 0x00, 0x00, 0x01, 'x', 'y'};

  const auto msgs = collect(framer, wire);
  EXPECT_TRUE(msgs.empty());
}

TEST(LengthPrefixFramerTest, EmptyPayloadIsAValidFrame) {
  LengthPrefixFramer framer(2, Endian::Big);
  const std::vector<uint8_t> wire = {0x00, 0x00, 0x00, 0x01, 'z'};

  const auto msgs = collect(framer, wire);
  ASSERT_EQ(msgs.size(), 2u);
  EXPECT_EQ(msgs[0], "");
  EXPECT_EQ(msgs[1], "z");
}

TEST(LengthPrefixFramerTest, ResetDiscardsAPartialFrame) {
  LengthPrefixFramer framer(2, Endian::Big);
  std::vector<std::string> msgs;
  framer.on_message([&](wirestead::memory::ConstByteSpan m) {
    msgs.emplace_back(reinterpret_cast<const char*>(m.data()), m.size());
  });

  const std::vector<uint8_t> partial = {0x00, 0x04, 'a', 'b'};
  framer.push_bytes(wirestead::memory::ConstByteSpan(partial.data(), partial.size()));
  framer.reset();

  // The tail of the abandoned frame must not be glued onto the next one.
  const std::vector<uint8_t> next = {0x00, 0x02, 'o', 'k'};
  framer.push_bytes(wirestead::memory::ConstByteSpan(next.data(), next.size()));

  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "ok");
}

TEST(LengthPrefixFramerTest, ResetFromCallbackStopsProcessingTheClearedBuffer) {
  LengthPrefixFramer framer(2, Endian::Big);
  std::vector<std::string> msgs;
  framer.on_message([&](wirestead::memory::ConstByteSpan m) {
    msgs.emplace_back(reinterpret_cast<const char*>(m.data()), m.size());
    framer.reset();
  });

  const std::vector<uint8_t> two_frames = {0x00, 0x01, 'a', 0x00, 0x01, 'b'};
  framer.push_bytes(wirestead::memory::ConstByteSpan(two_frames.data(), two_frames.size()));
  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "a");

  const std::vector<uint8_t> next = {0x00, 0x02, 'o', 'k'};
  framer.push_bytes(wirestead::memory::ConstByteSpan(next.data(), next.size()));
  ASSERT_EQ(msgs.size(), 2u);
  EXPECT_EQ(msgs[1], "ok");
}

TEST(LengthPrefixFramerTest, RejectsUnsupportedConfiguration) {
  EXPECT_THROW(LengthPrefixFramer(3), std::invalid_argument);
  EXPECT_THROW(LengthPrefixFramer(0), std::invalid_argument);
  EXPECT_THROW(LengthPrefixFramer(2, Endian::Big, 0), std::invalid_argument);
  EXPECT_NO_THROW(LengthPrefixFramer(1));
  EXPECT_NO_THROW(LengthPrefixFramer(4));
}
