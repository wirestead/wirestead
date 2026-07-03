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
#include <string_view>

#include "unilink/diagnostics/error_mapping.hpp"
#include "unilink/interface/channel.hpp"
#include "unilink/wrapper/context.hpp"

// Shared replacement for the per-wrapper dynamic_pointer_cast<TcpClient>/
// <UdsClient> special-casing (previously the only two transports whose
// wrapper could report a detailed ErrorContext) and every other wrapper's
// independent hardcoded generic-string fallback (jwsung91/unilink#445).
// Works uniformly now that interface::Channel::last_error_info() is
// virtual - no cast to a concrete transport type needed.
namespace unilink {
namespace wrapper {
namespace detail {

inline ErrorContext build_error_context(const interface::Channel& channel, std::string_view fallback_message,
                                        std::optional<ClientId> client_id = std::nullopt) {
  if (auto info = channel.last_error_info()) {
    return diagnostics::to_error_context(*info, client_id);
  }
  return ErrorContext(ErrorCode::IoError, fallback_message, client_id);
}

}  // namespace detail
}  // namespace wrapper
}  // namespace unilink
