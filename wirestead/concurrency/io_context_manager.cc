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

#include "wirestead/concurrency/io_context_manager.hpp"

#include <spdlog/fmt/fmt.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stop_token>
#include <thread>

#include "wirestead/diagnostics/logger.hpp"

namespace wirestead {
namespace concurrency {

struct IoContextManager::Impl {
  bool owns_context_{true};
  std::shared_ptr<IoContext> ioc_;
  std::unique_ptr<WorkGuard> work_guard_;
  std::jthread io_thread_;
  std::atomic<bool> running_{false};
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_{false};

  Impl() { diagnostics::Logger::instance(); }

  explicit Impl(std::shared_ptr<IoContext> external_context) : owns_context_(false), ioc_(std::move(external_context)) {
    diagnostics::Logger::instance();
  }

  explicit Impl(IoContext& external_context)
      : owns_context_(false), ioc_(std::shared_ptr<IoContext>(&external_context, [](IoContext*) {})) {
    diagnostics::Logger::instance();
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void stop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !stopping_; });
    if (!owns_context_ && ioc_) return;
    if (!running_.load() && !io_thread_.joinable()) return;

    stopping_ = true;
    if (work_guard_) work_guard_.reset();
    if (ioc_ && owns_context_) ioc_->stop();

    if (io_thread_.joinable()) {
      if (io_thread_.get_id() == std::this_thread::get_id()) {
        WIRESTEAD_LOG_ERROR("io_context_manager", "stop", "Cannot join IoContext thread from within itself.");
        stopping_ = false;
        cv_.notify_all();
        return;
      }
      lock.unlock();
      io_thread_.request_stop();
      io_thread_.join();
      lock.lock();
    }

    stopping_ = false;
    running_.store(false);
    cv_.notify_all();
  }
};

IoContextManager::IoContextManager() : impl_(std::make_unique<Impl>()) {}

IoContextManager::IoContextManager(std::shared_ptr<IoContext> external_context)
    : impl_(std::make_unique<Impl>(std::move(external_context))) {}

IoContextManager::IoContextManager(IoContext& external_context) : impl_(std::make_unique<Impl>(external_context)) {}

IoContextManager::~IoContextManager() = default;

IoContextManager& IoContextManager::instance() {
  static IoContextManager* instance = new IoContextManager();
  return *instance;
}

boost::asio::io_context& IoContextManager::get_context() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (!impl_->ioc_) {
    impl_->ioc_ = std::make_shared<IoContext>();
    impl_->owns_context_ = true;
  }
  return *impl_->ioc_;
}

void IoContextManager::start() {
  std::shared_ptr<IoContext> context;
  {
    std::unique_lock<std::mutex> lock(impl_->mutex_);

    if (!impl_->owns_context_ && impl_->ioc_) {
      if (impl_->ioc_->stopped()) {
        WIRESTEAD_LOG_WARNING("io_context_manager", "start", "External io_context is stopped.");
      }
      return;
    }

    impl_->cv_.wait(lock, [this] { return !impl_->stopping_; });

    if (impl_->running_) return;

    if (impl_->io_thread_.joinable() && impl_->io_thread_.get_id() == std::this_thread::get_id()) {
      WIRESTEAD_LOG_ERROR("io_context_manager", "start", "Cannot restart from within its own thread.");
      return;
    }

    if (!impl_->ioc_) {
      impl_->ioc_ = std::make_shared<IoContext>();
      impl_->owns_context_ = true;
    }

    if (impl_->ioc_->stopped()) {
      impl_->ioc_->restart();
    }
    impl_->work_guard_ = std::make_unique<WorkGuard>(impl_->ioc_->get_executor());
    context = impl_->ioc_;

    if (impl_->io_thread_.joinable()) {
      impl_->io_thread_.join();
    }

    impl_->io_thread_ = std::jthread([this, context](std::stop_token st) {
      try {
        // Register stop callback to gracefully stop io_context when jthread is stopped
        std::stop_callback cb(st, [context] { context->stop(); });
        context->run();
      } catch (const std::exception& e) {
        WIRESTEAD_LOG_ERROR("io_context_manager", "run", fmt::format("Thread error: {}", e.what()));
      } catch (...) {
      }
      impl_->running_.store(false);
    });
    impl_->running_.store(true);
  }
}

void IoContextManager::stop() { impl_->stop(); }

bool IoContextManager::is_running() const { return get_impl()->running_.load(); }

std::unique_ptr<boost::asio::io_context> IoContextManager::create_independent_context() {
  return std::make_unique<IoContext>();
}

IoContextManager::IoContextManager(IoContextManager&& other) noexcept = default;
IoContextManager& IoContextManager::operator=(IoContextManager&& other) noexcept = default;

}  // namespace concurrency
}  // namespace wirestead
