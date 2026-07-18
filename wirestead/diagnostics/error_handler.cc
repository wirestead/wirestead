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

#include "wirestead/diagnostics/error_handler.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>

#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {
namespace diagnostics {

ErrorHandler::ErrorHandler() = default;
ErrorHandler::~ErrorHandler() = default;

ErrorHandler& ErrorHandler::instance() {
  static ErrorHandler inst;
  return inst;
}

ErrorHandler& ErrorHandler::default_handler() { return instance(); }

void ErrorHandler::report_error(const ErrorInfo& error) {
  if (!enabled_.load()) {
    return;
  }

  if (error.level < min_level_.load()) {
    return;
  }

  std::vector<ErrorCallback> callbacks_copy;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    update_stats(error);
    add_to_recent_errors(error);
    add_to_component_errors(error);
    callbacks_copy = callbacks_;
  }
  notify_callbacks(callbacks_copy, error);
}

void ErrorHandler::register_callback(ErrorCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callbacks_.push_back(std::move(callback));
}

void ErrorHandler::clear_callbacks() {
  std::lock_guard<std::mutex> lock(mutex_);
  callbacks_.clear();
}

void ErrorHandler::set_min_error_level(ErrorLevel level) { min_level_.store(level); }

ErrorLevel ErrorHandler::min_error_level() const { return min_level_.load(); }

void ErrorHandler::set_enabled(bool enabled) { enabled_.store(enabled); }

bool ErrorHandler::enabled() const { return enabled_.load(); }

ErrorStats ErrorHandler::error_stats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void ErrorHandler::reset_stats() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.reset();
}

std::vector<ErrorInfo> ErrorHandler::errors_by_component(std::string_view component) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = errors_by_component_.find(std::string(component));
  if (it != errors_by_component_.end()) {
    return it->second;
  }
  return {};
}

std::vector<ErrorInfo> ErrorHandler::recent_errors(size_t count) const {
  std::lock_guard<std::mutex> lock(mutex_);

  size_t start_index = 0;
  if (recent_errors_.size() > count) {
    start_index = recent_errors_.size() - count;
  }

  return std::vector<ErrorInfo>(recent_errors_.begin() + static_cast<std::ptrdiff_t>(start_index),
                                recent_errors_.end());
}

bool ErrorHandler::has_errors(std::string_view component) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = errors_by_component_.find(std::string(component));
  return it != errors_by_component_.end() && !it->second.empty();
}

size_t ErrorHandler::error_count(std::string_view component, ErrorLevel level) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = errors_by_component_.find(std::string(component));
  if (it == errors_by_component_.end()) {
    return 0;
  }

  return static_cast<size_t>(std::count_if(it->second.begin(), it->second.end(),
                                           [level](const ErrorInfo& error) { return error.level == level; }));
}

void ErrorHandler::update_stats(const ErrorInfo& error) {
  std::lock_guard<std::mutex> lock(stats_mutex_);

  stats_.total_errors++;
  stats_.errors_by_level[static_cast<int>(error.level)]++;
  stats_.errors_by_category[static_cast<int>(error.category)]++;

  if (error.retryable) {
    stats_.retryable_errors++;
  }

  if (stats_.first_error == std::chrono::system_clock::time_point{}) {
    stats_.first_error = error.timestamp;
  }
  stats_.last_error = error.timestamp;
}

void ErrorHandler::notify_callbacks(const std::vector<ErrorCallback>& callbacks, const ErrorInfo& error) {
  for (const auto& callback : callbacks) {
    try {
      callback(error);
    } catch (const std::exception& e) {
      // Avoid infinite recursion - use Logger directly instead of ErrorHandler
      WIRESTEAD_LOG_ERROR("error_handler", "callback", "Error in error callback: " + std::string(e.what()));
    } catch (...) {
      WIRESTEAD_LOG_ERROR("error_handler", "callback", "Unknown error in error callback");
    }
  }
}

void ErrorHandler::add_to_recent_errors(const ErrorInfo& error) {
  recent_errors_.push_back(error);

  // Keep only the most recent errors
  if (recent_errors_.size() > MAX_RECENT_ERRORS) {
    recent_errors_.erase(
        recent_errors_.begin(),
        recent_errors_.begin() + static_cast<std::ptrdiff_t>(recent_errors_.size() - MAX_RECENT_ERRORS));
  }
}

void ErrorHandler::add_to_component_errors(const ErrorInfo& error) {
  errors_by_component_[error.component].push_back(error);

  // Limit component error history to prevent memory growth
  constexpr size_t MAX_COMPONENT_ERRORS = 100;
  auto& component_errors = errors_by_component_[error.component];
  if (component_errors.size() > MAX_COMPONENT_ERRORS) {
    component_errors.erase(
        component_errors.begin(),
        component_errors.begin() + static_cast<std::ptrdiff_t>(component_errors.size() - MAX_COMPONENT_ERRORS));
  }
}

// Convenience functions implementation
namespace error_reporting {

void report_connection_error(std::string_view component, std::string_view operation,
                             const boost::system::error_code& ec, bool retryable) {
  ErrorInfo error(ErrorLevel::ERROR, ErrorCategory::CONNECTION, component, operation, ec.message(), ec, retryable);
  ErrorHandler::instance().report_error(error);
}

void report_communication_error(std::string_view component, std::string_view operation, std::string_view message,
                                bool retryable) {
  ErrorInfo error(ErrorLevel::ERROR, ErrorCategory::COMMUNICATION, component, operation, message);
  error.retryable = retryable;
  ErrorHandler::instance().report_error(error);
}

void report_configuration_error(std::string_view component, std::string_view operation, std::string_view message) {
  ErrorInfo error(ErrorLevel::ERROR, ErrorCategory::CONFIGURATION, component, operation, message);
  ErrorHandler::instance().report_error(error);
}

void report_memory_error(std::string_view component, std::string_view operation, std::string_view message) {
  ErrorInfo error(ErrorLevel::CRITICAL, ErrorCategory::MEMORY, component, operation, message);
  ErrorHandler::instance().report_error(error);
}

void report_system_error(std::string_view component, std::string_view operation, std::string_view message,
                         const boost::system::error_code& ec) {
  ErrorInfo error(ErrorLevel::ERROR, ErrorCategory::SYSTEM, component, operation, message, ec);
  ErrorHandler::instance().report_error(error);
}

void report_warning(std::string_view component, std::string_view operation, std::string_view message) {
  ErrorInfo error(ErrorLevel::WARNING, ErrorCategory::UNKNOWN, component, operation, message);
  ErrorHandler::instance().report_error(error);
}

void report_info(std::string_view component, std::string_view operation, std::string_view message) {
  ErrorInfo error(ErrorLevel::INFO, ErrorCategory::UNKNOWN, component, operation, message);
  ErrorHandler::instance().report_error(error);
}

}  // namespace error_reporting

}  // namespace diagnostics
}  // namespace wirestead
