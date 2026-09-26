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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/framer/iframer.hpp"

namespace wirestead {
namespace wrapper::detail {
struct ReceiveFramerAccess;
}
namespace framer {

/**
 * @brief Framer for text-based protocols (e.g., ASCII, NMEA).
 *
 * Buffers incoming data and extracts messages delimited by a specific sequence (e.g., "\n").
 */
class WIRESTEAD_API LineFramer : public IFramer {
 public:
  /**
   * @brief Construct a new Line Framer
   *
   * @param delimiter The delimiter string (default: "\n")
   * @param include_delimiter Whether to include the delimiter in the extracted message (default: false)
   * @param max_length Maximum message length before forcing a reset (default: 65536)
   */
  explicit LineFramer(std::string_view delimiter = "\n", bool include_delimiter = false, size_t max_length = 65536);

  ~LineFramer() override = default;

  void push_bytes(memory::ConstByteSpan data) override;
  void on_message(MessageCallback cb) override;
  void reset() override;

 private:
  friend struct wrapper::detail::ReceiveFramerAccess;
  std::string delimiter_;
  bool include_delimiter_;
  size_t max_length_;

  size_t scanned_idx_ = 0;
  std::vector<uint8_t> buffer_;
  MessageCallback on_message_;

  // Set when an in-progress message exceeded max_length_ and buffer_ was
  // discarded before a delimiter was found. While true, push_bytes_internal
  // discards incoming bytes (instead of buffering them) until it finds a
  // delimiter, so the untransmitted tail of the discarded message can't be
  // mistaken for the start of a fresh, valid message.
  bool discarding_ = false;

  /**
   * @brief Helper to scan data for delimiters and process messages.
   *
   * @param data The data to scan.
   * @param search_start_offset The offset in data to start searching from.
   * @return The number of bytes processed (emitted as messages).
   */
  size_t scan_and_process(memory::ConstByteSpan data, size_t search_start_offset);

  /**
   * @brief Find the end of the next delimiter in data, if any.
   *
   * @param data The data to scan (searched from the beginning).
   * @return The number of bytes up to and including the delimiter, or
   *         std::nullopt if no delimiter was found in data.
   */
  std::optional<size_t> skip_until_delimiter(memory::ConstByteSpan data) const;

  /**
   * @brief Internal helper to process a manageable chunk of data.
   */
  void push_bytes_internal(memory::ConstByteSpan data);
};

}  // namespace framer
}  // namespace wirestead
