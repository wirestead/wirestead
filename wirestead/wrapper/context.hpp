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

#include "wirestead/base/common.hpp"
#include "wirestead/base/error_codes.hpp"
#include "wirestead/memory/safe_data_buffer.hpp"
#include "wirestead/memory/safe_span.hpp"

namespace wirestead {
namespace wrapper {

/**
 * @brief Context for data/message related events
 */
class MessageContext {
 public:
  MessageContext(ClientId client_id, memory::SafeDataBuffer data, std::string client_info = "")
      : client_id_(client_id), data_(std::move(data)), client_info_(std::move(client_info)) {}

  /** @brief Get the client identifier */
  ClientId client_id() const { return client_id_; }

  /**
   * @brief Get the received data as a view.
   *
   * The returned view is only valid for the lifetime of this MessageContext.
   * Do not store it past the callback's return — use data_as_string() instead.
   */
  std::string_view data() const { return std::string_view(reinterpret_cast<const char*>(data_.data()), data_.size()); }

  /** @brief Get the received data as a SafeDataBuffer reference */
  const memory::SafeDataBuffer& safe_data() const { return data_; }

  /** @brief Get the received data as a std::string */
  std::string data_as_string() const { return data_.as_string(); }

  /** @brief Get the received data as a std::vector<uint8_t> */
  std::vector<uint8_t> data_as_vector() const {
    const auto* d = data_.data();
    return std::vector<uint8_t>(d, d + data_.size());
  }

  /** @brief Get the client information (e.g., endpoint address) */
  const std::string& client_info() const { return client_info_; }

 private:
  ClientId client_id_;
  memory::SafeDataBuffer data_;
  std::string client_info_;
};

/**
 * @brief Context for connection/disconnection events
 */
class ConnectionContext {
 public:
  ConnectionContext(ClientId client_id, std::string client_info = "")
      : client_id_(client_id), client_info_(std::move(client_info)) {}

  /** @brief Get the client identifier */
  ClientId client_id() const { return client_id_; }

  /** @brief Get the client information (e.g., endpoint address) */
  const std::string& client_info() const { return client_info_; }

 private:
  ClientId client_id_;
  std::string client_info_;
};

/**
 * @brief Context for error events
 */
class ErrorContext {
 public:
  ErrorContext(ErrorCode code, std::string_view message, std::optional<ClientId> client_id = std::nullopt)
      : code_(code), message_(message), client_id_(client_id) {}

  /** @brief Get the error code */
  ErrorCode code() const { return code_; }

  /** @brief Get the error message */
  std::string_view message() const { return message_; }

  /** @brief Get the associated client ID, if any */
  std::optional<ClientId> client_id() const { return client_id_; }

 private:
  ErrorCode code_;
  std::string message_;
  std::optional<ClientId> client_id_;
};

}  // namespace wrapper
}  // namespace wirestead
