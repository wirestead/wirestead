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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <variant>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/memory/safe_span.hpp"

namespace wirestead {
namespace test {

// Event types for recording
enum class EventType { StateChange, DataReceived, Backpressure };

struct RecordedEvent {
  EventType type;
  std::chrono::steady_clock::time_point timestamp;

  // Data variants
  std::variant<base::LinkState, std::string, size_t> data;
};

/**
 * @brief Records callbacks from a Channel to verify compliance with the Channel Contract.
 */
class CallbackRecorder {
 public:
  CallbackRecorder() = default;

  // Callback generators
  interface::Channel::OnState get_state_callback() {
    return [this](base::LinkState state) {
      std::lock_guard<std::mutex> lock(mutex_);
      events_.push_back({EventType::StateChange, std::chrono::steady_clock::now(), state});
      last_state_ = state;
      cv_.notify_all();
    };
  }

  interface::Channel::OnBytes get_bytes_callback() {
    return [this](memory::ConstByteSpan span) {
      std::lock_guard<std::mutex> lock(mutex_);
      // Convert to string for storage (be careful with large data in tests)
      std::string s(reinterpret_cast<const char*>(span.data()), span.size());
      events_.push_back({EventType::DataReceived, std::chrono::steady_clock::now(), s});
      total_bytes_ += span.size();
      cv_.notify_all();
    };
  }

  interface::Channel::OnBackpressure get_backpressure_callback() {
    return [this](size_t queued_bytes) {
      std::lock_guard<std::mutex> lock(mutex_);
      events_.push_back({EventType::Backpressure, std::chrono::steady_clock::now(), queued_bytes});
      cv_.notify_all();
    };
  }

  // Verification helpers

  // Verify that no callbacks occurred after a specific time point
  bool verify_no_events_after(std::chrono::steady_clock::time_point point) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& event : events_) {
      if (event.timestamp > point) {
        return false;
      }
    }
    return true;
  }

  // Wait for a specific state
  bool wait_for_state(base::LinkState target, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (last_state_ == target) return true;
    return cv_.wait_for(lock, timeout, [this, target] { return last_state_ == target; });
  }

  // Wait for data reception
  bool wait_for_data(size_t min_bytes, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (total_bytes_ >= min_bytes) return true;
    return cv_.wait_for(lock, timeout, [this, min_bytes] { return total_bytes_ >= min_bytes; });
  }

  // Get current event log
  std::vector<RecordedEvent> get_events() {
    std::lock_guard<std::mutex> lock(mutex_);
    return events_;
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    events_.clear();
    total_bytes_ = 0;
    // last_state_ is preserved to reflect current reality
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<RecordedEvent> events_;
  base::LinkState last_state_{base::LinkState::Idle};
  size_t total_bytes_{0};
};

/**
 * @brief Helper to run io_context in a separate thread for testing
 */
class IoContextRunner {
 public:
  IoContextRunner(boost::asio::io_context& ioc) : ioc_(ioc) {
    work_guard_ =
        std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(ioc_.get_executor());
    thread_ = std::thread([this] { ioc_.run(); });
  }

  ~IoContextRunner() { stop(); }

  void stop() {
    if (work_guard_) {
      work_guard_->reset();
    }
    if (thread_.joinable()) {
      thread_.join();
    }
    // We don't stop ioc_ here because it might be shared or managed externally.
    // We just stop our runner thread.
  }

 private:
  boost::asio::io_context& ioc_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;
  std::thread thread_;
};

}  // namespace test
}  // namespace wirestead
