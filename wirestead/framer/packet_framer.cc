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

#include "wirestead/framer/packet_framer.hpp"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace wirestead {
namespace framer {
namespace {

void append_bytes(std::vector<uint8_t>& buffer, memory::ConstByteSpan data, size_t offset = 0) {
  if (offset >= data.size()) {
    return;
  }

  const size_t append_size = data.size() - offset;
  const size_t old_size = buffer.size();

  // Check for size_t overflow before resize
  if (append_size > std::numeric_limits<size_t>::max() - old_size) {
    throw std::length_error("append_bytes: size overflow");
  }

  buffer.resize(old_size + append_size);
  std::memcpy(buffer.data() + old_size, data.data() + offset, append_size);
}

}  // namespace

PacketFramer::PacketFramer(const std::vector<uint8_t>& start_pattern, const std::vector<uint8_t>& end_pattern,
                           size_t max_length)
    : start_pattern_(start_pattern), end_pattern_(end_pattern), max_length_(max_length), state_(State::Sync) {
  if (start_pattern_.empty() && end_pattern_.empty()) {
    throw std::invalid_argument("PacketFramer: start_pattern and end_pattern cannot both be empty.");
  }
}

void PacketFramer::push_bytes(memory::ConstByteSpan data) {
  if (data.empty()) return;

  // Fast Path: Zero-copy processing if buffer is empty
  if (buffer_.empty()) {
    size_t processed_count = 0;
    while (processed_count < data.size()) {
      // Find start pattern
      auto search_start = data.begin() + static_cast<std::ptrdiff_t>(processed_count);
      auto it_start = std::search(search_start, data.end(), start_pattern_.begin(), start_pattern_.end());

      if (it_start == data.end()) {
        // Start pattern not found.
        // Keep partial match at the end if applicable.
        size_t remaining = data.size() - processed_count;
        size_t keep_len = (start_pattern_.size() > 1) ? (start_pattern_.size() - 1) : 0;
        if (remaining > keep_len) {
          processed_count = data.size() - keep_len;
        }
        break;
      }

      // Found start pattern
      size_t start_idx = static_cast<size_t>(std::distance(data.begin(), it_start));
      size_t search_end_start_idx = start_idx + start_pattern_.size();

      if (end_pattern_.empty()) {
        // Assume minimal packet is start pattern only
        size_t packet_len = start_pattern_.size();
        if (on_message_) {
          on_message_(data.subspan(start_idx, packet_len));
        }
        processed_count = start_idx + packet_len;
        continue;
      }

      // Find end pattern
      auto search_end_start = data.begin() + static_cast<std::ptrdiff_t>(search_end_start_idx);
      auto it_end = std::search(search_end_start, data.end(), end_pattern_.begin(), end_pattern_.end());

      if (it_end == data.end()) {
        // Found start pattern but not end pattern.
        // Buffer everything starting from start_idx.
        processed_count = start_idx;
        // The remaining data starts with start_pattern, so we will transition to Collect state
        // after appending to buffer.
        break;
      }

      // Found end pattern
      size_t packet_len = static_cast<size_t>(std::distance(data.begin(), it_end)) + end_pattern_.size() - start_idx;

      if (packet_len <= max_length_) {
        if (on_message_) {
          on_message_(data.subspan(start_idx, packet_len));
        }
      }
      // If > max_length, discard by advancing processed_count past it

      processed_count = start_idx + packet_len;
    }

    if (processed_count < data.size()) {
      append_bytes(buffer_, data, processed_count);

      // Update state if we buffered a partial packet starting with start_pattern
      if (state_ == State::Sync && !buffer_.empty()) {
        if (buffer_.size() >= start_pattern_.size()) {
          if (std::equal(start_pattern_.begin(), start_pattern_.end(), buffer_.begin())) {
            state_ = State::Collect;
            scanned_idx_ = start_pattern_.size();
          }
        }
      }
    }
    return;
  }

  append_bytes(buffer_, data);

  while (true) {
    if (state_ == State::Sync) {
      if (start_pattern_.empty()) {
        state_ = State::Collect;
        continue;
      }

      auto it = std::search(buffer_.begin(), buffer_.end(), start_pattern_.begin(), start_pattern_.end());
      if (it != buffer_.end()) {
        // Found start pattern.
        // Discard everything before start pattern.
        if (it != buffer_.begin()) {
          buffer_.erase(buffer_.begin(), it);
        }
        state_ = State::Collect;
        // Start scanning for end pattern after the start pattern we just found
        scanned_idx_ = start_pattern_.size();
        // Continue to check for end pattern immediately
      } else {
        // Start pattern not found.
        // Keep partial match at the end.
        if (start_pattern_.size() > 1) {
          size_t keep_len = start_pattern_.size() - 1;
          if (buffer_.size() > keep_len) {
            buffer_.erase(buffer_.begin(), buffer_.end() - static_cast<std::ptrdiff_t>(keep_len));
          }
        } else {
          buffer_.clear();
        }
        break;  // Need more data
      }
    } else if (state_ == State::Collect) {
      if (end_pattern_.empty()) {
        // If end pattern is empty, packet ends immediately after start pattern?
        // Assume minimal packet is start pattern only
        size_t packet_len = start_pattern_.size();
        if (on_message_) {
          on_message_(memory::ConstByteSpan(buffer_.data(), packet_len));
        }
        if (buffer_.empty()) return;

        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(packet_len));
        state_ = State::Sync;
        continue;
      }

      // Search for end pattern *after* start pattern
      // Optimization: use scanned_idx_ to avoid re-scanning
      size_t search_offset = std::max(start_pattern_.size(), scanned_idx_);

      // Back up slightly to catch split end pattern if we are resuming search
      if (search_offset > start_pattern_.size()) {
        size_t overlap = (end_pattern_.size() > 1) ? (end_pattern_.size() - 1) : 0;
        if (search_offset >= overlap) {
          search_offset -= overlap;
        } else {
          search_offset = 0;
        }
      }

      // Safety clamp to ensure we don't search inside start pattern
      if (search_offset < start_pattern_.size()) {
        search_offset = start_pattern_.size();
      }

      if (buffer_.size() < search_offset) {
        // Should not happen if Sync worked correctly
        break;
      }

      auto search_start = buffer_.begin() + static_cast<std::ptrdiff_t>(search_offset);
      auto it = std::search(search_start, buffer_.end(), end_pattern_.begin(), end_pattern_.end());

      if (it != buffer_.end()) {
        // Found end pattern.
        size_t packet_len = static_cast<size_t>(std::distance(buffer_.begin(), it)) + end_pattern_.size();

        if (packet_len <= max_length_) {
          if (on_message_) {
            on_message_(memory::ConstByteSpan(buffer_.data(), packet_len));
          }
          if (buffer_.empty()) return;

          buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(packet_len));
          state_ = State::Sync;
          scanned_idx_ = 0;
        } else {
          // Exceeded max length, discard packet
          buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(packet_len));
          state_ = State::Sync;
          scanned_idx_ = 0;
        }
      } else {
        // End pattern not found.
        scanned_idx_ = buffer_.size();
        if (buffer_.size() > max_length_) {
          // Exceeded limit while collecting. Reset.
          buffer_.clear();
          state_ = State::Sync;
          scanned_idx_ = 0;
        }
        break;  // Need more data
      }
    }
  }
}

void PacketFramer::on_message(MessageCallback cb) { on_message_ = std::move(cb); }

void PacketFramer::reset() {
  buffer_.clear();
  state_ = State::Sync;
  scanned_idx_ = 0;
}

}  // namespace framer
}  // namespace wirestead
