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

#include "unilink/transport/uds/uds_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <future>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "unilink/base/platform.hpp"
#include "unilink/builder/auto_initializer.hpp"
#include "unilink/concurrency/io_context_manager.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/interface/iuds_acceptor.hpp"
#include "unilink/transport/base/error_info_holder.hpp"
#include "unilink/transport/uds/boost_uds_acceptor.hpp"
#include "unilink/transport/uds/uds_server_session.hpp"

#if !defined(UNILINK_PLATFORM_WINDOWS)
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

namespace unilink {
namespace transport {

namespace net = boost::asio;
using uds = net::local::stream_protocol;

namespace {

// #438: an existing path at the configured socket location should only ever
// be silently removed if it's a genuinely stale (no longer listened-on)
// socket file - not a regular file/directory (misconfiguration) and not a
// socket another live process is still listening on (which would otherwise
// be silently hijacked instead of failing with a clear error). Returns a
// non-empty reason string if bind should be refused; empty if it's safe to
// remove the path (or there's nothing there) and proceed.
std::string existing_uds_path_blocks_bind(const std::string& path) {
#if defined(UNILINK_PLATFORM_WINDOWS)
  (void)path;
  return {};
#else
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) {
    return {};  // nothing exists at this path - nothing to check
  }
  if (!S_ISSOCK(st.st_mode)) {
    return "existing path is not a socket file";
  }

  int probe_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (probe_fd < 0) {
    return {};  // can't probe - fall back to prior (remove-and-proceed) behavior
  }
  struct sockaddr_un addr {};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  int rc = ::connect(probe_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  ::close(probe_fd);
  if (rc == 0) {
    return "another process is already listening on this socket path";
  }
  return {};  // stale socket (nothing accepted the probe connect) - safe to remove
#endif
}

}  // namespace

struct UdsServer::Impl {
  std::unique_ptr<net::io_context> owned_ioc_;
  net::io_context* ioc_ = nullptr;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;
  bool owns_ioc_ = true;

  std::atomic<bool> stopping_{false};
  std::atomic<ClientId> next_client_id_{0};
  // #438: only this instance's own successful bind() may remove the socket
  // file on cleanup. Without this, a second UdsServer pointed at the same
  // path whose start() failed before ever binding (e.g. because a live
  // listener already owns that path) would still delete the first server's
  // socket file the moment stop()/the destructor ran - the exact hijack
  // #438 is about, just reached via stop() instead of start().
  std::atomic<bool> bound_{false};

  std::unique_ptr<interface::UdsAcceptorInterface> acceptor_;
  config::UdsServerConfig cfg_;

  concurrency::AtomicLinkState state_{base::LinkState::Idle};
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_bp_;
  MultiClientConnectHandler on_multi_connect_;
  MultiClientDataHandler on_multi_data_;
  MultiClientDisconnectHandler on_multi_disconnect_;
  diagnostics::RuntimeStatsCounters stats_;

  mutable std::mutex sessions_mutex_;
  std::unordered_map<ClientId, std::shared_ptr<UdsServerSession>> sessions_;

  ErrorInfoHolder error_info_holder_{"uds_server"};

  Impl(const config::UdsServerConfig& cfg, net::io_context* ioc_ptr)
      : owned_ioc_(ioc_ptr ? nullptr : std::make_unique<net::io_context>()),
        ioc_(ioc_ptr ? ioc_ptr : owned_ioc_.get()),
        owns_ioc_(!ioc_ptr),
        cfg_(cfg) {
    cfg_.validate_and_clamp();
    acceptor_ = std::make_unique<BoostUdsAcceptor>(*ioc_);
  }
  ~Impl() {
    stopping_ = true;
    if (work_guard_) {
      work_guard_.reset();
    }
    if (ioc_ && owns_ioc_) {
      if (ioc_thread_.joinable()) {
        if (std::this_thread::get_id() == ioc_thread_.get_id()) {
          ioc_thread_.detach();
        } else {
          ioc_thread_.request_stop();
          ioc_thread_.join();
        }
      }
    }
    // UDS Cleanup: socket file should be removed, but only if this instance
    // actually bound it (#438).
    if (bound_.load()) {
      std::remove(cfg_.socket_path.c_str());
    }
  }
  void do_accept(std::shared_ptr<UdsServer> self);
  void notify_state();

  void perform_cleanup() {
    try {
      boost::system::error_code ec;
      if (acceptor_) {
        acceptor_->close(ec);
      }

      std::vector<std::shared_ptr<UdsServerSession>> sessions_to_stop;
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (auto& pair : sessions_) {
          sessions_to_stop.push_back(pair.second);
        }
        sessions_.clear();
      }

      for (auto& session : sessions_to_stop) {
        if (session) {
          session->stop();
        }
      }

      if (bound_.exchange(false)) {
        std::remove(cfg_.socket_path.c_str());
      }

      state_.set(base::LinkState::Idle);
      notify_state();
    } catch (...) {
    }
  }

  void stop(std::shared_ptr<UdsServer> self) {
    if (stopping_.exchange(true)) {
      return;
    }

    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      on_bytes_ = nullptr;
      on_state_ = nullptr;
      on_bp_ = nullptr;
      on_multi_connect_ = nullptr;
      on_multi_data_ = nullptr;
      on_multi_disconnect_ = nullptr;
    }

    if (ioc_->get_executor().running_in_this_thread()) {
      perform_cleanup();
      if (owns_ioc_) {
        work_guard_.reset();
        ioc_->stop();
      }
      return;
    }

    bool has_active_ioc = owns_ioc_ || !ioc_->stopped();

    if (has_active_ioc && self) {
      auto cleanup_promise = std::make_shared<std::promise<void>>();
      auto cleanup_future = cleanup_promise->get_future();

      std::weak_ptr<UdsServer> weak_self = self;
      net::dispatch(*ioc_, [weak_self, cleanup_promise]() {
        if (auto shared_self = weak_self.lock()) {
          auto* cleanup_impl = shared_self->get_impl();
          cleanup_impl->perform_cleanup();
        }
        cleanup_promise->set_value();
      });

      if (cleanup_future.wait_for(std::chrono::seconds(2)) == std::future_status::timeout) {
        perform_cleanup();
      }
    } else {
      perform_cleanup();
    }

    if (owns_ioc_) {
      work_guard_.reset();
      ioc_->stop();
    }

    if (owns_ioc_ && ioc_thread_.joinable()) {
      if (std::this_thread::get_id() != ioc_thread_.get_id()) {
        ioc_thread_.join();
      } else {
        ioc_thread_.detach();
      }
      ioc_->restart();
    }
  }
};

std::shared_ptr<UdsServer> UdsServer::create(const config::UdsServerConfig& cfg) {
  return std::shared_ptr<UdsServer>(new UdsServer(cfg));
}

std::shared_ptr<UdsServer> UdsServer::create(const config::UdsServerConfig& cfg,
                                             std::unique_ptr<interface::UdsAcceptorInterface> acceptor,
                                             net::io_context& ioc) {
  return std::shared_ptr<UdsServer>(new UdsServer(cfg, std::move(acceptor), ioc));
}

UdsServer::UdsServer(const config::UdsServerConfig& cfg) : impl_(std::make_unique<Impl>(cfg, nullptr)) {}
UdsServer::UdsServer(const config::UdsServerConfig& cfg, std::unique_ptr<interface::UdsAcceptorInterface> acceptor,
                     net::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, &ioc)) {
  impl_->acceptor_ = std::move(acceptor);
}

UdsServer::~UdsServer() {
  if (impl_ && impl_->state_.get() != base::LinkState::Idle) {
    impl_->stop(nullptr);
  }
}

UdsServer::UdsServer(UdsServer&&) noexcept = default;
UdsServer& UdsServer::operator=(UdsServer&&) noexcept = default;

void UdsServer::start() {
  if (impl_->state_.get() == base::LinkState::Listening) return;

  impl_->stopping_ = false;

  if (!impl_->cfg_.is_valid()) {
    UNILINK_LOG_ERROR("uds_server", "start", "Invalid UDS server configuration or socket path");
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION,
                                           "start", {}, "Invalid UDS server configuration or socket path", false, 0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }

  // #438: only remove an existing path at this location if it's actually a
  // stale socket - never a regular file/directory (misconfiguration), and
  // never a socket another live process is still listening on (that would
  // otherwise be silently hijacked instead of failing loudly).
  std::string block_reason = existing_uds_path_blocks_bind(impl_->cfg_.socket_path);
  if (!block_reason.empty()) {
    std::string msg = fmt::format("Refusing to bind {}: {}", impl_->cfg_.socket_path, block_reason);
    UNILINK_LOG_ERROR("uds_server", "start", msg);
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                           "start", make_error_code(boost::system::errc::address_in_use), msg, false,
                                           0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }
  std::remove(impl_->cfg_.socket_path.c_str());

  boost::system::error_code ec;
  impl_->acceptor_->open(uds(), ec);
  if (ec) {
    std::string msg = fmt::format("Failed to open acceptor: {}", ec.message());
    UNILINK_LOG_ERROR("uds_server", "start", msg);
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "open",
                                           ec, msg, false, 0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }

  uds::endpoint endpoint;
  try {
    endpoint = uds::endpoint(impl_->cfg_.socket_path);
  } catch (const std::exception& e) {
    std::string msg = fmt::format("Invalid UDS endpoint: {}", e.what());
    UNILINK_LOG_ERROR("uds_server", "start", msg);
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION,
                                           "start", make_error_code(boost::system::errc::filename_too_long), msg, false,
                                           0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }

  impl_->acceptor_->bind(endpoint, ec);
  if (ec) {
    std::string msg = fmt::format("Failed to bind to {}: {}", impl_->cfg_.socket_path, ec.message());
    UNILINK_LOG_ERROR("uds_server", "start", msg);
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                           "bind", ec, msg, false, 0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }
  impl_->bound_.store(true);

  impl_->acceptor_->listen(net::socket_base::max_listen_connections, ec);
  if (ec) {
    std::string msg = fmt::format("Failed to listen: {}", ec.message());
    UNILINK_LOG_ERROR("uds_server", "start", msg);
    impl_->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                           "listen", ec, msg, false, 0);
    impl_->state_.set(base::LinkState::Error);
    impl_->notify_state();
    return;
  }

  // #438: restrict local access to the socket file if requested. Best-effort
  // - a chmod failure is logged but does not fail startup, since the socket
  // is already bound and listening at this point.
  if (impl_->cfg_.socket_permissions != -1) {
#if !defined(UNILINK_PLATFORM_WINDOWS)
    if (::chmod(impl_->cfg_.socket_path.c_str(), static_cast<mode_t>(impl_->cfg_.socket_permissions)) != 0) {
      UNILINK_LOG_WARNING("uds_server", "start",
                          fmt::format("Failed to chmod socket {}: {}", impl_->cfg_.socket_path, strerror(errno)));
    }
#else
    UNILINK_LOG_WARNING("uds_server", "start", "socket_permissions is ignored on Windows");
#endif
  }

  impl_->state_.set(base::LinkState::Listening);
  impl_->notify_state();

  if (impl_->owns_ioc_ && !impl_->ioc_thread_.joinable()) {
    if (impl_->ioc_->stopped()) {
      impl_->ioc_->restart();
    }
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(net::make_work_guard(*impl_->ioc_));
    impl_->ioc_thread_ = std::jthread([impl = impl_.get()](std::stop_token st) {
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_->stop(); });
        impl->ioc_->run();
      } catch (...) {
      }
    });
  }

  net::post(impl_->ioc_->get_executor(), [self = shared_from_this()]() { self->impl_->do_accept(self); });
}

void UdsServer::stop() { impl_->stop(shared_from_this()); }

bool UdsServer::is_connected() const { return impl_->state_.get() == base::LinkState::Listening; }
bool UdsServer::is_backpressure_active() const { return false; }

bool UdsServer::is_backpressure_active(ClientId client_id) const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it != impl_->sessions_.end() && it->second) {
    return it->second->is_backpressure_active();
  }
  return false;
}

boost::asio::any_io_executor UdsServer::get_executor() { return impl_->ioc_->get_executor(); }

wrapper::RuntimeStats UdsServer::stats() const {
  auto aggregate = impl_->stats_.snapshot(0, 0, false);
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  for (const auto& pair : impl_->sessions_) {
    if (!pair.second) continue;
    const auto session_stats = pair.second->stats();
    aggregate.bytes_accepted += session_stats.bytes_accepted;
    aggregate.messages_accepted += session_stats.messages_accepted;
    aggregate.bytes_sent += session_stats.bytes_sent;
    aggregate.messages_sent += session_stats.messages_sent;
    aggregate.bytes_received += session_stats.bytes_received;
    aggregate.messages_received += session_stats.messages_received;
    aggregate.failed_sends += session_stats.failed_sends;
    aggregate.dropped_messages += session_stats.dropped_messages;
    aggregate.dropped_bytes += session_stats.dropped_bytes;
    aggregate.backpressure_events += session_stats.backpressure_events;
    aggregate.queued_bytes += session_stats.queued_bytes;
    aggregate.pending_bytes += session_stats.pending_bytes;
    aggregate.max_queued_bytes += session_stats.max_queued_bytes;
    aggregate.backpressure_active = aggregate.backpressure_active || session_stats.backpressure_active;
  }
  return aggregate;
}

void UdsServer::reset_stats() {
  impl_->stats_.reset(0);
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  for (const auto& pair : impl_->sessions_) {
    if (pair.second) pair.second->reset_stats();
  }
}

std::optional<diagnostics::ErrorInfo> UdsServer::last_error_info() const {
  return impl_->error_info_holder_.last_error_info();
}

bool UdsServer::async_write_copy(memory::ConstByteSpan data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(data.begin(), data.end());
  return async_write_shared(shared_data);
}

bool UdsServer::async_write_move(std::vector<uint8_t>&& data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(std::move(data));
  return async_write_shared(shared_data);
}

bool UdsServer::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (impl_->stopping_.load() || !data || data->empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  bool sent = false;
  bool attempted = false;
  for (auto& pair : impl_->sessions_) {
    if (pair.second && pair.second->alive() && pair.second->async_write_shared(data)) {
      attempted = true;
      sent = true;
    } else if (pair.second && pair.second->alive()) {
      attempted = true;
    }
  }
  if (!attempted) impl_->stats_.record_failed_send();
  return sent;
}

bool UdsServer::async_try_write_copy(memory::ConstByteSpan data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(data.begin(), data.end());
  return async_try_write_shared(shared_data);
}

bool UdsServer::async_try_write_move(std::vector<uint8_t>&& data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(std::move(data));
  return async_try_write_shared(shared_data);
}

bool UdsServer::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  if (impl_->stopping_.load() || !data || data->empty()) {
    impl_->stats_.record_failed_send();
    return false;
  }
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  bool sent = false;
  bool attempted = false;
  for (auto& pair : impl_->sessions_) {
    if (pair.second && pair.second->alive()) {
      attempted = true;
      if (pair.second->async_try_write_shared(data)) sent = true;
    }
  }
  if (!attempted) impl_->stats_.record_failed_send();
  return sent;
}

void UdsServer::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bytes_ = std::move(cb);
}

void UdsServer::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_state_ = std::move(cb);
}

void UdsServer::on_backpressure(OnBackpressure cb) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bp_ = std::move(cb);
}

bool UdsServer::broadcast(std::string_view message) {
  auto data =
      std::make_shared<const std::vector<uint8_t>>(reinterpret_cast<const uint8_t*>(message.data()),
                                                   reinterpret_cast<const uint8_t*>(message.data()) + message.size());
  return async_try_write_shared(data);
}

bool UdsServer::broadcast(memory::ConstByteSpan data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(data.begin(), data.end());
  return async_try_write_shared(shared_data);
}

bool UdsServer::send_to_client(ClientId client_id, std::string_view message) {
  return send_to_client(client_id,
                        memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool UdsServer::send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  std::shared_ptr<UdsServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
    auto it = impl_->sessions_.find(client_id);
    if (it != impl_->sessions_.end()) session = it->second;
  }
  if (session) {
    return session->async_write_copy(data);
  }
  impl_->stats_.record_failed_send();
  return false;
}

bool UdsServer::try_send_to_client(ClientId client_id, std::string_view message) {
  return try_send_to_client(client_id,
                            memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool UdsServer::try_send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  std::shared_ptr<UdsServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
    auto it = impl_->sessions_.find(client_id);
    if (it != impl_->sessions_.end()) session = it->second;
  }
  if (session) {
    return session->async_try_write_copy(data);
  }
  impl_->stats_.record_failed_send();
  return false;
}

size_t UdsServer::client_count() const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  return impl_->sessions_.size();
}

std::vector<ClientId> UdsServer::connected_clients() const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  std::vector<ClientId> ids;
  for (const auto& pair : impl_->sessions_) ids.push_back(pair.first);
  return ids;
}

void UdsServer::set_client_limit(size_t max_clients) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->cfg_.max_connections =
      static_cast<int>(std::min(max_clients, static_cast<size_t>(base::constants::MAX_MAX_CONNECTIONS)));
}

void UdsServer::on_multi_connect(MultiClientConnectHandler handler) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_multi_connect_ = std::move(handler);
}

void UdsServer::on_multi_data(MultiClientDataHandler handler) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_multi_data_ = std::move(handler);
}

void UdsServer::on_multi_disconnect(MultiClientDisconnectHandler handler) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_multi_disconnect_ = std::move(handler);
}

base::LinkState UdsServer::state() const { return impl_->state_.get(); }

void UdsServer::Impl::do_accept(std::shared_ptr<UdsServer> self) {
  acceptor_->async_accept([self](const boost::system::error_code& ec, uds::socket socket) {
    if (!self || self->impl_->stopping_) return;

    if (!ec) {
      ClientId client_id;
      {
        std::lock_guard<std::mutex> lock(self->impl_->sessions_mutex_);
        if (self->impl_->cfg_.max_connections > 0 &&
            self->impl_->sessions_.size() >= static_cast<size_t>(self->impl_->cfg_.max_connections)) {
          boost::system::error_code ignored;
          socket.close(ignored);
          auto* impl = self->impl_.get();
          impl->do_accept(self);
          return;
        }
        client_id = self->impl_->next_client_id_++;
      }

      auto session = std::make_shared<UdsServerSession>(
          *self->impl_->ioc_, std::move(socket), self->impl_->cfg_.backpressure_threshold,
          self->impl_->cfg_.idle_timeout_ms, self->impl_->cfg_.backpressure_strategy,
          self->impl_->cfg_.enable_memory_pool);

      std::weak_ptr<UdsServer> weak_self = self;
      session->on_bytes([weak_self, client_id](memory::ConstByteSpan data) {
        auto s = weak_self.lock();
        if (!s) return;
        MultiClientDataHandler data_handler;
        OnBytes bytes_handler;
        {
          std::lock_guard<std::mutex> lock(s->impl_->sessions_mutex_);
          data_handler = s->impl_->on_multi_data_;
          bytes_handler = s->impl_->on_bytes_;
        }
        if (data_handler) data_handler(client_id, data);
        if (bytes_handler) bytes_handler(data);
      });

      session->on_close([weak_self, client_id]() {
        auto s = weak_self.lock();
        if (!s || s->impl_->stopping_) return;

        MultiClientDisconnectHandler disconnect_handler;
        {
          std::lock_guard<std::mutex> lock(s->impl_->sessions_mutex_);
          if (s->impl_->stopping_) return;  // Double check inside lock
          s->impl_->sessions_.erase(client_id);
          disconnect_handler = s->impl_->on_multi_disconnect_;
        }
        if (disconnect_handler) disconnect_handler(client_id);
      });

      // alive_ must be true before the session enters sessions_, so that
      // broadcast() callers who observe client_count() >= 1 are guaranteed
      // to pass the alive() check inside async_try_write_shared().
      session->start();

      {
        std::lock_guard<std::mutex> lock(self->impl_->sessions_mutex_);
        self->impl_->sessions_[client_id] = session;
      }

      MultiClientConnectHandler connect_handler;
      {
        std::lock_guard<std::mutex> lock(self->impl_->sessions_mutex_);
        connect_handler = self->impl_->on_multi_connect_;
      }
      if (connect_handler) connect_handler(client_id, "UDS Client");

      // Continue accepting
      auto* impl = self->impl_.get();
      impl->do_accept(self);
    } else {
      auto* impl = self->impl_.get();
      if (impl->stopping_.load()) return;

      // Log only real errors, not operation_aborted
      if (ec != boost::asio::error::operation_aborted) {
        std::string msg = fmt::format("Accept failed: {}", ec.message());
        UNILINK_LOG_ERROR("uds_server", "accept", msg);
        impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                              "accept", ec, msg, true, 0);
        impl->state_.set(base::LinkState::Error);
        impl->notify_state();
      }

      if (!impl->stopping_.load()) {
        auto timer = std::make_shared<net::steady_timer>(*impl->ioc_);
        timer->expires_after(std::chrono::milliseconds(100));
        timer->async_wait([self, timer](const boost::system::error_code&) {
          auto* retry_impl = self->impl_.get();
          if (!retry_impl->stopping_.load()) {
            retry_impl->do_accept(self);
          }
        });
      }
    }
  });
}

void UdsServer::Impl::notify_state() {
  OnState cb;
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    cb = on_state_;
  }
  if (cb) cb(state_.get());
}

}  // namespace transport
}  // namespace unilink
