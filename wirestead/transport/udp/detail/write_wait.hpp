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
#include <cstdint>
#include <optional>

#include "wirestead/wrapper/send_result.hpp"

namespace wirestead::transport::detail {
// Access is serialized by UdpChannel's submission mutex.
struct UdpWriteWait {
  uint64_t sequence;
  bool require_remote;
  std::optional<wrapper::SendRejection> ended_by;
  void end(wrapper::SendRejection reason) {
    if (!ended_by) ended_by = reason;
  }
};
}  // namespace wirestead::transport::detail
