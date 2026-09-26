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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/framer/iframer.hpp"

namespace wirestead {
namespace wrapper::detail {
struct ReceiveFramerAccess;
}
namespace framer {

/**
 * @brief Framer for binary protocols that prefix each message with its length.
 *
 * The layout most binary protocols use, and the one PacketFramer cannot carry:
 * because the length says exactly how many bytes to collect, no byte value is
 * special and the payload may contain anything at all.
 *
 * ```cpp
 * auto client = wirestead::tcp_client("127.0.0.1", 9000)
 *                   .use_length_prefix_framer(4)   // 4-byte big-endian length
 *                   .on_message(...)
 *                   .build();
 * ```
 *
 * @warning **The stream must start on a frame boundary.** This framer has no
 * sync word and cannot resynchronise: joining mid-message, or one corrupt
 * length, misreads the following bytes as a header and stays wrong. That is
 * the normal case for TCP and UDS, where the connection begins at a boundary.
 * A serial line you can attach to mid-stream, or any protocol carrying a sync
 * word, needs framing that hunts for that word - implement IFramer and pass it
 * to `framer()`.
 *
 * A declared length above `max_length` is treated as corruption: the buffer is
 * dropped and framing restarts, rather than allocating whatever a bad or
 * hostile header asked for.
 */
class WIRESTEAD_API LengthPrefixFramer : public IFramer {
 public:
  enum class Endian { Big, Little };

  /**
   * @brief Construct a new Length Prefix Framer
   *
   * @param prefix_bytes Width of the length field: 1, 2 or 4.
   * @param endian Byte order of the length field. Big by default, which is
   *        network order and what most wire protocols specify.
   * @param max_length Largest payload accepted; a larger declared length is
   *        treated as corruption.
   * @param length_includes_prefix Whether the declared length counts the
   *        prefix itself. Both conventions exist; getting it wrong shifts
   *        every frame by the prefix width.
   * @throws std::invalid_argument if prefix_bytes is not 1, 2 or 4, or
   *         max_length is 0.
   */
  explicit LengthPrefixFramer(size_t prefix_bytes = 2, Endian endian = Endian::Big, size_t max_length = 65536,
                              bool length_includes_prefix = false);

  ~LengthPrefixFramer() override = default;

  void push_bytes(memory::ConstByteSpan data) override;
  void on_message(MessageCallback cb) override;
  void reset() override;

 private:
  friend struct wrapper::detail::ReceiveFramerAccess;
  // Returns false when the buffer does not yet hold a whole header.
  bool read_length(size_t offset, size_t& out) const;

  size_t prefix_bytes_;
  Endian endian_;
  size_t max_length_;
  bool length_includes_prefix_;

  std::vector<uint8_t> buffer_;
  MessageCallback on_message_;
};

}  // namespace framer
}  // namespace wirestead
