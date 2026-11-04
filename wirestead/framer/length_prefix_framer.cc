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

#include "wirestead/framer/length_prefix_framer.hpp"

#include <stdexcept>
#include <utility>

#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {
namespace framer {

LengthPrefixFramer::LengthPrefixFramer(size_t prefix_bytes, Endian endian, size_t max_length,
                                       bool length_includes_prefix)
    : prefix_bytes_(prefix_bytes),
      endian_(endian),
      max_length_(max_length),
      length_includes_prefix_(length_includes_prefix) {
  if (prefix_bytes != 1 && prefix_bytes != 2 && prefix_bytes != 4) {
    throw std::invalid_argument("LengthPrefixFramer: prefix_bytes must be 1, 2 or 4");
  }
  if (max_length == 0) {
    throw std::invalid_argument("LengthPrefixFramer: max_length must be greater than 0");
  }
}

bool LengthPrefixFramer::read_length(size_t offset, size_t& out) const {
  if (buffer_.size() - offset < prefix_bytes_) return false;

  uint64_t value = 0;
  for (size_t i = 0; i < prefix_bytes_; ++i) {
    const size_t idx = endian_ == Endian::Big ? offset + i : offset + (prefix_bytes_ - 1 - i);
    value = (value << 8) | buffer_[idx];
  }
  out = static_cast<size_t>(value);
  return true;
}

void LengthPrefixFramer::push_bytes(memory::ConstByteSpan data) {
  if (!data.empty()) {
    buffer_.insert(buffer_.end(), data.data(), data.data() + data.size());
  }

  size_t consumed = 0;
  while (true) {
    size_t declared = 0;
    if (!read_length(consumed, declared)) break;

    // Both conventions exist for what the length counts. A frame claiming to
    // be shorter than its own header cannot be either, so it is corruption.
    size_t payload_len = declared;
    if (length_includes_prefix_) {
      if (declared < prefix_bytes_) {
        WIRESTEAD_LOG_WARNING("framer", "length_prefix",
                              "Declared length is shorter than the prefix itself; resynchronising");
        reset();
        return;
      }
      payload_len = declared - prefix_bytes_;
    }

    if (payload_len > max_length_) {
      // Never allocate what a bad or hostile header asked for. Without a sync
      // word there is nothing to hunt for, so the only honest recovery is to
      // drop what we have and start over.
      WIRESTEAD_LOG_WARNING("framer", "length_prefix",
                            "Declared payload length exceeds max_length; dropping the buffer");
      reset();
      return;
    }

    const size_t frame_end = consumed + prefix_bytes_ + payload_len;
    if (buffer_.size() < frame_end) break;  // wait for the rest of the payload

    if (on_message_) {
      on_message_(memory::ConstByteSpan(buffer_.data() + consumed + prefix_bytes_, payload_len));
      // reset() from the callback cleared the buffer this loop indexes into.
      if (buffer_.empty()) return;
    }
    consumed = frame_end;
  }

  if (consumed > 0) {
    buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(consumed));
  }
}

void LengthPrefixFramer::on_message(MessageCallback cb) { on_message_ = std::move(cb); }

void LengthPrefixFramer::reset() { buffer_.clear(); }

}  // namespace framer
}  // namespace wirestead
