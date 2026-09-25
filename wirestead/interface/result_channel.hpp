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

#include "wirestead/interface/channel.hpp"
#include "wirestead/wrapper/send_result.hpp"

namespace wirestead::interface {

/**
 * Single-target write admission capability for built-in and custom channels.
 *
 * Results describe local queue acceptance only. Implementations choose rejection
 * reasons at admission; they must not infer them after a bool-only write.
 * These calls do not wait for capacity. A refused move retains its input.
 * Shared ownership may be retained only for accepted work.
 *
 * Channel remains the legacy bool interface, including transports whose writes
 * fan out. A fanout implementation must not collapse partial results into this
 * capability. The bool adapters below intentionally expose acceptance only.
 */
class WIRESTEAD_API ResultChannel : public Channel {
 public:
  using SendResult = wrapper::SendResult;

  [[nodiscard]] virtual SendResult async_write_copy_result(memory::ConstByteSpan data) = 0;
  [[nodiscard]] virtual SendResult async_write_move_result(std::vector<uint8_t>&& data) = 0;
  [[nodiscard]] virtual SendResult async_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) = 0;
  [[nodiscard]] virtual SendResult async_try_write_copy_result(memory::ConstByteSpan data) = 0;
  [[nodiscard]] virtual SendResult async_try_write_move_result(std::vector<uint8_t>&& data) = 0;
  [[nodiscard]] virtual SendResult async_try_write_shared_result(std::shared_ptr<const std::vector<uint8_t>> data) = 0;

  bool async_write_copy(memory::ConstByteSpan data) final { return async_write_copy_result(data).accepted(); }
  bool async_write_move(std::vector<uint8_t>&& data) final {
    return async_write_move_result(std::move(data)).accepted();
  }
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) final {
    return async_write_shared_result(std::move(data)).accepted();
  }
  bool async_try_write_copy(memory::ConstByteSpan data) final { return async_try_write_copy_result(data).accepted(); }
  bool async_try_write_move(std::vector<uint8_t>&& data) final {
    return async_try_write_move_result(std::move(data)).accepted();
  }
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) final {
    return async_try_write_shared_result(std::move(data)).accepted();
  }
};

}  // namespace wirestead::interface
