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
namespace framer {

/**
 * @brief Framer for binary packet protocols.
 *
 * Handles protocols with start and end patterns.
 * Syncs by searching for start pattern, then collects data until end pattern is found.
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
