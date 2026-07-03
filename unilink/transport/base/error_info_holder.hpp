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

#include <boost/system/error_code.hpp>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "unilink/diagnostics/error_types.hpp"

// Extracted from what used to be identical, independently-maintained
// record_error()/last_error_info_ logic in TcpClient::Impl and
// UdsClient::Impl (jwsung91/unilink#445), now shared so every transport can
// implement interface::Channel::last_error_info() the same way instead of
// only two of seven having any detailed-error mechanism at all.
namespace unilink {
namespace transport {

class ErrorInfoHolder {
 public:
  explicit ErrorInfoHolder(std::string component) : component_(std::move(component)) {}

  void record_error(diagnostics::ErrorLevel lvl, diagnostics::ErrorCategory cat, std::string_view operation,
                    const boost::system::error_code& ec, std::string_view msg, bool retryable, uint32_t retry_count) {
    diagnostics::ErrorInfo info(lvl, cat, component_, operation, msg, ec, retryable);
    info.retry_count = retry_count;
    std::lock_guard<std::mutex> lock(mtx_);
    last_error_info_ = std::move(info);
  }

  std::optional<diagnostics::ErrorInfo> last_error_info() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return last_error_info_;
  }

 private:
  std::string component_;
  mutable std::mutex mtx_;
  std::optional<diagnostics::ErrorInfo> last_error_info_;
};

}  // namespace transport
}  // namespace unilink
