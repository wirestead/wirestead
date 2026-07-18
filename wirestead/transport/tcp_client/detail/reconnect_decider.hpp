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

#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/transport/base/reconnect_decider.hpp"

namespace wirestead {
namespace transport {
namespace detail {

/**
 * @brief Determines whether a reconnection attempt should be made and the base delay to use.
 */
inline ReconnectLogicDecision decide_reconnect(const config::TcpClientConfig& cfg,
                                               const diagnostics::ErrorInfo& error_info, uint32_t attempt_count,
                                               const std::optional<ReconnectPolicy>& policy) {
  return decide_reconnect_common(cfg, error_info, attempt_count, policy);
}

}  // namespace detail
}  // namespace transport
}  // namespace wirestead
