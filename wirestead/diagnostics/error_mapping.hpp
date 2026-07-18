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

#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#include <optional>
#include <string>

#include "wirestead/base/error_codes.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/wrapper/context.hpp"

namespace wirestead {
namespace diagnostics {

/**
 * @brief Maps boost::system::error_code to Wirestead ErrorCode
 */
inline ErrorCode to_wirestead_error_code(const boost::system::error_code& ec) {
  if (!ec) [[likely]] {
    return ErrorCode::Success;
  }

  // Check specific boost error codes
  if (ec == boost::asio::error::connection_refused) [[unlikely]] {
    return ErrorCode::ConnectionRefused;
  }
  if (ec == boost::asio::error::timed_out) [[unlikely]] {
    return ErrorCode::TimedOut;
  }
  if (ec == boost::asio::error::connection_reset) [[unlikely]] {
    return ErrorCode::ConnectionReset;
  }
  if (ec == boost::asio::error::connection_aborted) [[unlikely]] {
    return ErrorCode::ConnectionAborted;
  }
  if (ec == boost::asio::error::network_unreachable || ec == boost::asio::error::host_unreachable) [[unlikely]] {
    return ErrorCode::NotConnected;
  }
  if (ec == boost::asio::error::already_connected) [[unlikely]] {
    return ErrorCode::AlreadyConnected;
  }
  if (ec == boost::asio::error::address_in_use) [[unlikely]] {
    return ErrorCode::PortInUse;
  }
  if (ec == boost::asio::error::access_denied) [[unlikely]] {
    return ErrorCode::AccessDenied;
  }

  // Fallback to generic IoError for other network errors
  return ErrorCode::IoError;
}

/**
 * @brief Determines if a TCP connection error is retryable
 */
inline bool is_retryable_tcp_connect_error(const boost::system::error_code& ec) {
  if (!ec) [[likely]]
    return false;

  // Connection refused is temporary (server might be starting up)
  if (ec == boost::asio::error::connection_refused) return true;
  // Timeouts are temporary network glitches
  if (ec == boost::asio::error::timed_out) return true;
  // Reset connection might be temporary
  if (ec == boost::asio::error::connection_reset) return true;
  // Unreachable might be temporary routing issue
  if (ec == boost::asio::error::network_unreachable || ec == boost::asio::error::host_unreachable) return true;
  // Resource temporarily unavailable
  if (ec == boost::asio::error::try_again) return true;

  // Aborted usually means stopped by user, not retryable automatically
  if (ec == boost::asio::error::operation_aborted) return false;

  // Unknown network errors are treated as transient to err on the side of resilience.
  return true;
}

/**
 * @brief Determines if a UDS connection error is retryable
 */
inline bool is_retryable_uds_connect_error(const boost::system::error_code& ec) {
  if (!ec) return false;

  // For UDS, "connection refused" often means the socket file exists but no one is listening.
  // "no such file or directory" means the server hasn't created the socket yet.
  // Both are retryable.
  if (ec == boost::asio::error::connection_refused) return true;
  if (ec == boost::system::errc::no_such_file_or_directory) return true;
  if (ec == boost::asio::error::timed_out) return true;
  if (ec == boost::asio::error::try_again) return true;

  if (ec == boost::asio::error::operation_aborted) return false;

  return true;
}

/**
 * @brief Converts ErrorInfo to wrapper::ErrorContext
 */
inline wrapper::ErrorContext to_error_context(const diagnostics::ErrorInfo& info,
                                              std::optional<size_t> client_id = std::nullopt) {
  ErrorCode code = ErrorCode::IoError;

  if (info.boost_error) {
    code = to_wirestead_error_code(info.boost_error);
  } else {
    // Map category if boost error is not present
    switch (info.category) {
      case ErrorCategory::CONNECTION:
        code = ErrorCode::NotConnected;
        break;
      case ErrorCategory::CONFIGURATION:
        code = ErrorCode::InvalidConfiguration;
        break;
      case ErrorCategory::SYSTEM:
        code = ErrorCode::InternalError;
        break;
      default:
        code = ErrorCode::IoError;
        break;
    }
  }

  // Use the message from ErrorInfo
  return wrapper::ErrorContext(code, info.message, client_id);
}

}  // namespace diagnostics
}  // namespace wirestead
