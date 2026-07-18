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

#include <functional>
#include <memory>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/memory/safe_span.hpp"

namespace wirestead {
namespace framer {

/**
 * @brief Abstract base class for message framing strategies.
 *
 * Handles stream-based data segmentation (e.g., separating TCP/Serial streams
 * into distinct messages by delimiters or packet patterns).
 */
class WIRESTEAD_API IFramer {
 public:
  virtual ~IFramer() = default;

  /**
   * @brief Push raw bytes into the framer's internal buffer.
   *
   * The framer will buffer the data and invoke the message callback
   * whenever a complete message is extracted.
   *
   * @param data The raw data chunk to process.
   */
  virtual void push_bytes(memory::ConstByteSpan data) = 0;

  /**
   * @brief Register a callback to be invoked when a complete message is extracted.
   *
   * @param cb The callback function taking a span of the message bytes.
   */
  using MessageCallback = std::function<void(memory::ConstByteSpan)>;
  virtual void on_message(MessageCallback cb) = 0;

  /**
   * @brief Reset internal state/buffer.
   *
   * Should be called on connection loss or when resynchronization is needed.
   */
  virtual void reset() = 0;
};

}  // namespace framer
}  // namespace wirestead
