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

#include <string>

namespace wirestead {

/**
 * @brief Structured error codes for Wirestead
 */
enum class ErrorCode {
  Success = 0,
  Unknown,
  InvalidConfiguration,
  InternalError,
  IoError,

  // Connection related
  ConnectionRefused,
  ConnectionReset,
  ConnectionAborted,
  TimedOut,
  NotConnected,
  AlreadyConnected,

  // Server related
  PortInUse,
  AccessDenied,

  // Lifecycle
  Stopped,
  StartFailed
};

/**
 * @brief Convert ErrorCode to human-readable string
 */
inline std::string to_string(ErrorCode code) {
  switch (code) {
    case ErrorCode::Success:
      return "Success";
    case ErrorCode::Unknown:
      return "Unknown Error";
    case ErrorCode::InvalidConfiguration:
      return "Invalid Configuration";
    case ErrorCode::InternalError:
      return "Internal Error";
    case ErrorCode::IoError:
      return "I/O Error";
    case ErrorCode::ConnectionRefused:
      return "Connection Refused";
    case ErrorCode::ConnectionReset:
      return "Connection Reset";
    case ErrorCode::ConnectionAborted:
      return "Connection Aborted";
    case ErrorCode::TimedOut:
      return "Operation Timed Out";
    case ErrorCode::NotConnected:
      return "Not Connected";
    case ErrorCode::AlreadyConnected:
      return "Already Connected";
    case ErrorCode::PortInUse:
      return "Port Already In Use";
    case ErrorCode::AccessDenied:
      return "Access Denied";
    case ErrorCode::Stopped:
      return "Stopped";
    case ErrorCode::StartFailed:
      return "Failed to Start";
    default:
      return "Unknown Error Code";
  }
}

}  // namespace wirestead
