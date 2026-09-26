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

#pragma once

#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/framer/iframer.hpp"

namespace wirestead {
namespace wrapper::detail {
struct ReceiveFramerAccess;
}
namespace framer {

/**
 * @brief Framer for binary packet protocols delimited by byte patterns.
 *
 * Syncs by searching for the start pattern, then collects data until the end
 * pattern is found.
 *
 * @warning **The payload must not be able to contain the end pattern.** There
 * is no escaping or byte stuffing: the end pattern is located with a plain
 * search over the collected bytes, so the first occurrence ends the frame
 * wherever it appears. A binary protocol whose payload spans arbitrary byte
 * values will therefore be cut short as soon as those bytes happen to match,
 * and the remainder is resynchronised as if it were a new packet - silently,
 * since a truncated frame is indistinguishable from a short one.
 *
 * That makes this framer a fit for protocols with a reserved delimiter (a
 * text-ish or escaped wire format), and a poor fit for the common
 * length-prefixed binary layout. Use LengthPrefixFramer for that - the length
 * field says exactly how many bytes to collect, so no byte value is ever
 * special:
 *
 * ```cpp
 * auto ch = wirestead::tcp_client("127.0.0.1", 9000)
 *               .use_length_prefix_framer(4)
 *               .on_message(...)
 *               .build();
 * ```
 */
class WIRESTEAD_API PacketFramer : public IFramer {
 public:
  /**
   * @brief Construct a new Packet Framer
   *
   * @param start_pattern The start pattern bytes
   * @param end_pattern The end pattern bytes
   * @param max_length Maximum packet length (including patterns) before reset
   */
  PacketFramer(const std::vector<uint8_t>& start_pattern, const std::vector<uint8_t>& end_pattern, size_t max_length);

  ~PacketFramer() override = default;

  void push_bytes(memory::ConstByteSpan data) override;
  void on_message(MessageCallback cb) override;
  void reset() override;

 private:
  friend struct wrapper::detail::ReceiveFramerAccess;
  enum class State {
    Sync,    // Waiting for start pattern
    Collect  // Collecting data until end pattern
  };

  std::vector<uint8_t> start_pattern_;
  std::vector<uint8_t> end_pattern_;
  size_t max_length_;

  State state_;
  std::vector<uint8_t> buffer_;
  MessageCallback on_message_;

  // Optimization: Track where we stopped scanning for end pattern
  // to avoid re-scanning the entire buffer on each push.
  size_t scanned_idx_ = 0;
};

}  // namespace framer
}  // namespace wirestead
