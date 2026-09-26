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

#include <algorithm>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <utility>

#include "wirestead/wrapper/receive_limits.hpp"

namespace wirestead::wrapper::detail {
class ReceiveScope;
class ReceiveCharge;

class ReceiveBudget : public std::enable_shared_from_this<ReceiveBudget> {
 public:
  explicit ReceiveBudget(ReceiveLimits limits) : limits_(limits) { limits_.validate(); }
  const ReceiveLimits& limits() const { return limits_; }
  std::shared_ptr<ReceiveScope> open_scope();
  ReceiveMemoryStats stats() const {
    const auto used = used_.load();
    return {used, std::max(used, peak_.load()), sessions_.load(), overflows_.load(), overflow_bytes_.load()};
  }
  void reset_stats() {
    peak_ = used_.load();
    overflows_ = 0;
    overflow_bytes_ = 0;
  }

 private:
  friend class ReceiveScope;
  bool acquire(size_t bytes) {
    auto used = used_.load();
    do {
      if (bytes > limits_.max_bytes - used) return false;
    } while (!used_.compare_exchange_weak(used, used + bytes));
    auto peak = peak_.load();
    while (peak < used + bytes && !peak_.compare_exchange_weak(peak, used + bytes)) {
    }
    return true;
  }
  ReceiveLimits limits_;
  std::atomic<size_t> used_{0}, peak_{0}, sessions_{0};
  std::atomic<uint64_t> overflows_{0}, overflow_bytes_{0};
};

class ReceiveScope : public std::enable_shared_from_this<ReceiveScope> {
  friend class ReceiveBudget;
  struct ConstructionKey {};

 public:
  ReceiveScope(std::shared_ptr<ReceiveBudget> budget, ConstructionKey) : budget_(std::move(budget)) {}
  ~ReceiveScope() { --budget_->sessions_; }
  std::shared_ptr<ReceiveCharge> reserve(size_t bytes);
  void overflow(size_t bytes) {
    ++overflows_;
    overflow_bytes_ += bytes;
    ++budget_->overflows_;
    budget_->overflow_bytes_ += bytes;
  }
  ReceiveMemoryStats stats() const {
    const auto used = used_.load();
    return {used, std::max(used, peak_.load()), 1, overflows_.load(), overflow_bytes_.load()};
  }
  const ReceiveLimits& limits() const { return budget_->limits(); }

 private:
  friend class ReceiveCharge;
  void release(size_t bytes) {
    used_ -= bytes;
    budget_->used_ -= bytes;
  }
  std::shared_ptr<ReceiveBudget> budget_;
  std::atomic<size_t> used_{0}, peak_{0};
  std::atomic<uint64_t> overflows_{0}, overflow_bytes_{0};
};

// The charge follows retained library storage through moves, timer delivery and
// callback execution. Removing a session cannot release bytes still in flight.
class ReceiveCharge {
  friend class ReceiveScope;
  struct ConstructionKey {};

 public:
  ReceiveCharge(std::shared_ptr<ReceiveScope> scope, size_t bytes, ConstructionKey)
      : scope_(std::move(scope)), bytes_(bytes) {}
  ~ReceiveCharge() { scope_->release(bytes_); }
  ReceiveCharge(const ReceiveCharge&) = delete;
  ReceiveCharge& operator=(const ReceiveCharge&) = delete;
  size_t size() const { return bytes_; }
  void merge(ReceiveCharge& other) {
    if (this == &other) return;
    if (scope_ != other.scope_) throw std::logic_error("receive charge scopes differ");
    bytes_ += std::exchange(other.bytes_, 0);
  }
  void shrink(size_t bytes) {
    if (bytes > bytes_) throw std::logic_error("receive charge cannot grow without reservation");
    scope_->release(bytes_ - bytes);
    bytes_ = bytes;
  }

 private:
  std::shared_ptr<ReceiveScope> scope_;
  size_t bytes_;
};

inline std::shared_ptr<ReceiveCharge> ReceiveScope::reserve(size_t bytes) {
  if (!budget_->acquire(bytes)) return {};
  const auto used = used_.fetch_add(bytes) + bytes;
  auto peak = peak_.load();
  while (peak < used && !peak_.compare_exchange_weak(peak, used)) {
  }
  try {
    return std::make_shared<ReceiveCharge>(shared_from_this(), bytes, ReceiveCharge::ConstructionKey{});
  } catch (...) {
    release(bytes);
    throw;
  }
}

inline std::shared_ptr<ReceiveScope> ReceiveBudget::open_scope() {
  auto count = sessions_.load();
  do {
    if (count >= limits_.max_sessions) return {};
  } while (!sessions_.compare_exchange_weak(count, count + 1));
  try {
    return std::make_shared<ReceiveScope>(shared_from_this(), ReceiveScope::ConstructionKey{});
  } catch (...) {
    --sessions_;
    throw;
  }
}
}  // namespace wirestead::wrapper::detail
