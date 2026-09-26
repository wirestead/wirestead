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

#include "wirestead/transport/uds/uds_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "wirestead/base/platform.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/interface/iuds_acceptor.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/uds/boost_uds_acceptor.hpp"
#include "wirestead/transport/uds/uds_server_session.hpp"
#include "wirestead/wrapper/send_validation.hpp"

#if !defined(WIRESTEAD_PLATFORM_WINDOWS)
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

namespace wirestead {
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
#if defined(WIRESTEAD_PLATFORM_WINDOWS)
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
  net::strand<net::io_context::executor_type> strand_;
  std::atomic<uint64_t> generation_{0};
  bool run_dispatched_ = false;
  std::atomic<bool> cleanup_started_{false};
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;
  bool owns_ioc_ = true;

  std::atomic<bool> stopping_{false};
  std::atomic<ClientId> next_client_id_{0};
  std::mutex target_admission_mtx_;
  std::mutex stop_mtx_;
  std::condition_variable stop_cv_;
  bool cleanup_done_ = false;
  std::mutex join_mtx_;

  void mark_cleanup_done() {
    detail::stop_test_hook(this, true);
    {
      std::lock_guard<std::mutex> lock(stop_mtx_);
      cleanup_done_ = true;
    }
    stop_cv_.notify_all();
  }

  // Waits for the teardown that was dispatched onto the context, and nothing
  // more: the contract's precondition is that the context's owner keeps it
  // running, and running it here would execute unrelated handlers - another
  // channel's user callbacks included - on the stopping thread.
  void wait_for_cleanup() {
    detail::stop_test_hook(this, false);
    std::unique_lock<std::mutex> lock(stop_mtx_);
    stop_cv_.wait(lock, [this] { return cleanup_done_; });
  }

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
  // Shared snapshots for the handlers the io thread copies out per received
  // chunk - a std::function copy allocates whenever the target outgrows its
  // small-object buffer. See interface::SharedCallback. The connect/disconnect
  // handlers stay plain: they fire once per connection, not per chunk.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;
  MultiClientConnectHandler on_multi_connect_;
  interface::SharedCallback<MultiClientDataHandler> on_multi_data_;
  MultiClientDisconnectHandler on_multi_disconnect_;
  diagnostics::RuntimeStatsCounters stats_;

  mutable std::mutex sessions_mutex_;
  std::unordered_map<ClientId, std::shared_ptr<UdsServerSession>> sessions_;
  // Stop removes targets immediately, but their counters remain visible until
  // asynchronous cleanup transfers the final snapshot exactly once.
  std::unordered_map<ClientId, std::shared_ptr<UdsServerSession>> retiring_sessions_;
  wrapper::SendAccounting closed_send_accounting_;

  void absorb_session(const std::shared_ptr<UdsServerSession>& session) {
    const auto snapshot = session->stats();
    stats_.absorb(snapshot);
    diagnostics::accumulate_send_accounting(closed_send_accounting_, *snapshot.send_accounting);
  }

  ErrorInfoHolder error_info_holder_{"uds_server"};

  Impl(const config::UdsServerConfig& cfg, net::io_context* ioc_ptr)
      : owned_ioc_(ioc_ptr ? nullptr : std::make_unique<net::io_context>()),
        ioc_(ioc_ptr ? ioc_ptr : owned_ioc_.get()),
        strand_(net::make_strand(*ioc_)),
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
  void do_accept(std::shared_ptr<UdsServer> self, uint64_t generation);
  void notify_state();

  void finish_cleanup() {
    if (bound_.exchange(false)) std::remove(cfg_.socket_path.c_str());
    state_.set(base::LinkState::Idle);
    if (owns_ioc_) work_guard_.reset();
    mark_cleanup_done();
  }

  void perform_cleanup(std::shared_ptr<UdsServer> self = {}) {
    if (cleanup_started_.exchange(true)) return;
    boost::system::error_code ec;
    if (acceptor_) acceptor_->close(ec);
    std::vector<std::pair<ClientId, std::shared_ptr<UdsServerSession>>> sessions;
    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      retiring_sessions_.swap(sessions_);
      for (const auto& entry : retiring_sessions_) sessions.push_back(entry);
    }
    if (auto hook = detail::g_server_sessions_retiring_hook.load()) hook();
    if (sessions.empty()) {
      finish_cleanup();
      return;
    }
    // Completion includes every session's callback body and cleanup. Each
    // session owns its outstanding I/O; final state changes are serialized
    // with accept/retry handlers on the server's management strand.
    auto remaining = std::make_shared<size_t>(sessions.size());
    for (auto& [client_id, session] : sessions) {
      session->async_stop([this, self, remaining, client_id] {
        net::post(strand_, [this, self, remaining, client_id] {
          {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            const auto it = retiring_sessions_.find(client_id);
            if (it != retiring_sessions_.end()) {
              absorb_session(it->second);
              retiring_sessions_.erase(it);
            }
          }
          if (--*remaining == 0) finish_cleanup();
        });
      });
    }
  }

  void stop(std::shared_ptr<UdsServer> self) {
    const bool on_executor = ioc_->get_executor().running_in_this_thread();
    bool first;
    {
      std::lock_guard<std::mutex> lock(target_admission_mtx_);
      first = !stopping_.exchange(true);
      if (first) {
        std::lock_guard<std::mutex> sessions_lock(sessions_mutex_);
        for (auto& entry : sessions_)
          if (entry.second) entry.second->request_stop();
      }
    }
    if (first) {
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        on_bytes_ = nullptr;
        on_state_ = nullptr;
        on_bp_ = nullptr;
        on_multi_connect_ = nullptr;
        on_multi_data_ = nullptr;
        on_multi_disconnect_ = nullptr;
      }
      if (!run_dispatched_) {
        perform_cleanup(self);
      } else {
        net::post(strand_, [this, self] { perform_cleanup(self); });
      }
    }
    if (on_executor) return;
    wait_for_cleanup();
    std::lock_guard<std::mutex> join_lock(join_mtx_);
    join_owned_thread();
  }

  void join_owned_thread() {
    if (owns_ioc_ && ioc_thread_.joinable()) ioc_thread_.join();
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
  if (impl_) {
    impl_->stop(nullptr);
  }
}

UdsServer::UdsServer(UdsServer&&) noexcept = default;
UdsServer& UdsServer::operator=(UdsServer&&) noexcept = default;

void UdsServer::start() {
  if (impl_->state_.get() == base::LinkState::Listening) return;

  const auto generation = impl_->generation_.fetch_add(1) + 1;
  impl_->stopping_ = false;
  impl_->run_dispatched_ = false;
  impl_->cleanup_started_ = false;
  {
    std::lock_guard<std::mutex> lock(impl_->stop_mtx_);
    impl_->cleanup_done_ = false;
  }
  // Restart contract (#444): stats() resets on restart. The server-level
  // counters now outlive the sessions that fed them, so clearing them here is
  // what keeps that promise - before absorption they were empty and a restart
  // zeroed the aggregate for free.
  {
    std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
    impl_->stats_.reset(0);
    impl_->closed_send_accounting_ = {};
  }

  if (!impl_->cfg_.is_valid()) {
    WIRESTEAD_LOG_ERROR("uds_server", "start", "Invalid UDS server configuration or socket path");
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
    WIRESTEAD_LOG_ERROR("uds_server", "start", msg);
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
    WIRESTEAD_LOG_ERROR("uds_server", "start", msg);
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
    WIRESTEAD_LOG_ERROR("uds_server", "start", msg);
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
    WIRESTEAD_LOG_ERROR("uds_server", "start", msg);
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
    WIRESTEAD_LOG_ERROR("uds_server", "start", msg);
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
#if !defined(WIRESTEAD_PLATFORM_WINDOWS)
    if (::chmod(impl_->cfg_.socket_path.c_str(), static_cast<mode_t>(impl_->cfg_.socket_permissions)) != 0) {
      WIRESTEAD_LOG_WARNING("uds_server", "start",
                            fmt::format("Failed to chmod socket {}: {}", impl_->cfg_.socket_path, strerror(errno)));
    }
#else
    WIRESTEAD_LOG_WARNING("uds_server", "start", "socket_permissions is ignored on Windows");
#endif
  }

  impl_->state_.set(base::LinkState::Listening);
  impl_->notify_state();
  if (impl_->stopping_) return;

  if (impl_->owns_ioc_ && !impl_->ioc_thread_.joinable()) {
    if (impl_->ioc_->stopped()) {
      impl_->ioc_->restart();
    }
    impl_->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(net::make_work_guard(*impl_->ioc_));
    impl_->ioc_thread_ = std::jthread([impl = impl_.get()](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_->stop(); });
        impl->ioc_->run();
      } catch (...) {
      }
    });
  }

  impl_->run_dispatched_ = true;
  net::post(impl_->strand_, [self = shared_from_this(), generation]() { self->impl_->do_accept(self, generation); });
}

void UdsServer::stop() { impl_->stop(weak_from_this().lock()); }

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

std::optional<size_t> UdsServer::write_queue_limit(ClientId client_id) const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it == impl_->sessions_.end() || !it->second) return std::nullopt;
  return it->second->write_queue_limit();
}

boost::asio::any_io_executor UdsServer::get_executor() { return impl_->strand_; }

wrapper::RuntimeStats UdsServer::stats() const {
  // Snapshot retained totals and live sessions under the same lock as the
  // disconnect transfer; otherwise a retiring session can disappear from both.
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto aggregate = impl_->stats_.snapshot(0, 0, false);
  aggregate.send_accounting = impl_->closed_send_accounting_;
  for (const auto* sessions : {&impl_->sessions_, &impl_->retiring_sessions_}) {
    for (const auto& pair : *sessions) {
      if (!pair.second) continue;
      const auto session_stats = pair.second->stats();
      diagnostics::accumulate_send_accounting(*aggregate.send_accounting, *session_stats.send_accounting);
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
      // Retiring sessions retain cumulative totals, not live queue gauges.
      if (sessions == &impl_->sessions_) {
        aggregate.queued_bytes += session_stats.queued_bytes;
        aggregate.pending_bytes += session_stats.pending_bytes;
        aggregate.backpressure_active = aggregate.backpressure_active || session_stats.backpressure_active;
      }
      // Peak, not a total: summing per-session peaks would report a depth no
      // session ever reached, because the peaks need not have been simultaneous.
      aggregate.max_queued_bytes = std::max(aggregate.max_queued_bytes, session_stats.max_queued_bytes);
    }
  }
  return aggregate;
}

void UdsServer::reset_stats() {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->stats_.reset(0);
  impl_->closed_send_accounting_ = {};
  for (const auto* sessions : {&impl_->sessions_, &impl_->retiring_sessions_}) {
    for (const auto& entry : *sessions) {
      if (entry.second) entry.second->reset_stats();
    }
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
  auto shared_data = std::make_shared<std::vector<uint8_t>>(std::move(data));
  const bool accepted = async_write_shared(shared_data);
  // Rejected session admissions retain no reference. With no accepting target,
  // ownership still belongs to the caller; restore it before returning false.
  if (!accepted) data = std::move(*shared_data);
  return accepted;
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
  auto shared_data = std::make_shared<std::vector<uint8_t>>(std::move(data));
  const bool accepted = async_try_write_shared(shared_data);
  // Rejected session admissions retain no reference. With no accepting target,
  // ownership still belongs to the caller; restore it before returning false.
  if (!accepted) data = std::move(*shared_data);
  return accepted;
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
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bytes_ = std::move(shared);
}

void UdsServer::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_state_ = std::move(shared);
}

void UdsServer::on_backpressure(OnBackpressure cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bp_ = std::move(shared);
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

wrapper::FanoutResult UdsServer::broadcast_result(memory::ConstByteSpan data, wrapper::SendResult wrapper_state,
                                                  bool append_newline) {
  std::vector<std::pair<ClientId, std::shared_ptr<UdsServerSession>>> targets;
  {
    std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
    targets.reserve(impl_->sessions_.size());
    for (const auto& [id, session] : impl_->sessions_) {
      if (session && session->alive()) targets.emplace_back(id, session);
    }
  }
  if (auto hook = detail::g_uds_fanout_snapshot_hook.load()) hook();
  wrapper::FanoutResult result;
  std::shared_ptr<const std::vector<uint8_t>> shared_data;
  for (const auto& [id, session] : targets) {
    const auto size =
        append_newline
            ? (data.size() >= base::constants::MAX_BUFFER_SIZE ? base::constants::MAX_BUFFER_SIZE + 1 : data.size() + 1)
            : data.size();
    const auto validation = wrapper::detail::validate_payload_size(size, session->write_queue_limit());
    if (!validation.accepted()) {
      result.add(validation);
      continue;
    }
    if (!wrapper_state.accepted()) {
      result.add(wrapper_state);
      continue;
    }
    if (!shared_data) {
      auto payload = std::make_shared<std::vector<uint8_t>>();
      if (!data.empty()) payload->assign(data.begin(), data.end());
      if (append_newline) payload->push_back('\n');
      shared_data = std::move(payload);
    }
    std::lock_guard<std::mutex> admission_lock(impl_->target_admission_mtx_);
    const auto state = target_state();
    if (!state.accepted()) {
      result.add(state);
      continue;
    }
    std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
    const auto it = impl_->sessions_.find(id);
    if (it == impl_->sessions_.end() || it->second != session) {
      result.add(wrapper::SendResult::reject(wrapper::SendRejection::NotReady));
      continue;
    }
    result.add(session->try_write_shared(shared_data));
  }
  return result;
}

bool UdsServer::send_to_client(ClientId client_id, std::string_view message) {
  return send_to_client(client_id,
                        memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool UdsServer::send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  const auto result = write_target(client_id, data, false);
  if (auto hook = detail::g_uds_server_write_result_hook.load()) hook(result);
  return result.accepted();
}

bool UdsServer::try_send_to_client(ClientId client_id, std::string_view message) {
  return try_send_to_client(client_id,
                            memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool UdsServer::try_send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  const auto result = write_target(client_id, data, true);
  if (auto hook = detail::g_uds_server_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult UdsServer::target_state() const {
  if (impl_->stopping_.load()) {
    std::lock_guard<std::mutex> lock(impl_->stop_mtx_);
    return wrapper::SendResult::reject(impl_->cleanup_done_ ? wrapper::SendRejection::NotStarted
                                                            : wrapper::SendRejection::Stopping);
  }
  if (impl_->generation_.load() == 0) return wrapper::SendResult::reject(wrapper::SendRejection::NotStarted);
  return wrapper::SendResult::accept();
}

std::shared_ptr<UdsServerSession> UdsServer::capture_target(ClientId client_id) const {
  std::lock_guard<std::mutex> admission_lock(impl_->target_admission_mtx_);
  if (!target_state().accepted()) return {};
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  return it == impl_->sessions_.end() ? nullptr : it->second;
}

std::optional<wrapper::SendResult> UdsServer::poll_target_wait(const std::shared_ptr<UdsServerSession>& session) const {
  if (!session) return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  return session->poll_write_wait();
}

void UdsServer::cancel_target_waits() {
  std::lock_guard<std::mutex> admission_lock(impl_->target_admission_mtx_);
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  for (auto& entry : impl_->sessions_)
    if (entry.second) entry.second->cancel_write_wait();
}

wrapper::SendResult UdsServer::write_target(ClientId client_id, memory::ConstByteSpan data, bool try_only,
                                            const std::shared_ptr<UdsServerSession>& expected) {
  if (expected) {
    if (auto hook = detail::g_uds_server_pinned_write_hook.load()) hook();
  }
  std::lock_guard<std::mutex> admission_lock(impl_->target_admission_mtx_);
  const auto state = target_state();
  if (!state.accepted()) {
    impl_->stats_.record_failed_send();
    return state;
  }
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it == impl_->sessions_.end() || !it->second || (expected && it->second != expected)) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  }
  return try_only ? it->second->try_write_copy(data) : it->second->write_copy(data);
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

std::optional<wrapper::RuntimeStats> UdsServer::client_stats(ClientId client_id) const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it == impl_->sessions_.end() || !it->second) return std::nullopt;
  return it->second->stats();
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
  auto shared = interface::share_callback(std::move(handler));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_multi_data_ = std::move(shared);
}

void UdsServer::on_multi_disconnect(MultiClientDisconnectHandler handler) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_multi_disconnect_ = std::move(handler);
}

base::LinkState UdsServer::state() const { return impl_->state_.get(); }

void UdsServer::Impl::do_accept(std::shared_ptr<UdsServer> self, uint64_t generation) {
  if (stopping_ || generation != generation_) return;
  acceptor_->async_accept([self, generation](const boost::system::error_code& ec, uds::socket socket) {
    net::dispatch(self->impl_->strand_, [self, generation, ec, socket = std::move(socket)]() mutable {
      if (self->impl_->stopping_ || generation != self->impl_->generation_) return;

      if (!ec) {
        ClientId client_id;
        {
          std::lock_guard<std::mutex> lock(self->impl_->sessions_mutex_);
          if (self->impl_->cfg_.max_connections > 0 &&
              self->impl_->sessions_.size() >= static_cast<size_t>(self->impl_->cfg_.max_connections)) {
            boost::system::error_code ignored;
            socket.close(ignored);
            auto* impl = self->impl_.get();
            impl->do_accept(self, generation);
            return;
          }
          client_id = self->impl_->next_client_id_++;
        }

        auto session = std::make_shared<UdsServerSession>(
            *self->impl_->ioc_, std::move(socket), self->impl_->cfg_.backpressure_threshold,
            self->impl_->cfg_.idle_timeout_ms, self->impl_->cfg_.backpressure_strategy,
            self->impl_->cfg_.enable_memory_pool, self->impl_->cfg_.read_buffer_size);

        std::weak_ptr<UdsServer> weak_self = self;
        session->on_bytes([weak_self, client_id, generation](memory::ConstByteSpan data) {
          auto s = weak_self.lock();
          if (!s || s->impl_->stopping_ || generation != s->impl_->generation_) return;
          interface::SharedCallback<MultiClientDataHandler> data_handler;
          interface::SharedCallback<OnBytes> bytes_handler;
          {
            std::lock_guard<std::mutex> lock(s->impl_->sessions_mutex_);
            data_handler = s->impl_->on_multi_data_;
            bytes_handler = s->impl_->on_bytes_;
          }
          if (data_handler) (*data_handler)(client_id, data);
          if (bytes_handler) (*bytes_handler)(data);
        });

        // Forward each session's pressure transitions through the current
        // server handler, just like data. The snapshot permits replacement
        // after connect and invokes user code outside sessions_mutex_.
        session->on_backpressure([weak_self, generation](size_t queued) {
          auto s = weak_self.lock();
          if (!s || s->impl_->stopping_ || generation != s->impl_->generation_) return;
          interface::SharedCallback<OnBackpressure> handler;
          {
            std::lock_guard<std::mutex> lock(s->impl_->sessions_mutex_);
            handler = s->impl_->on_bp_;
          }
          if (handler) (*handler)(queued);
        });

        session->on_close([weak_self, client_id, generation]() {
          auto s = weak_self.lock();
          if (!s) return;
          net::post(s->impl_->strand_, [s, client_id, generation] {
            if (s->impl_->stopping_ || generation != s->impl_->generation_) return;

            MultiClientDisconnectHandler disconnect_handler;
            {
              std::lock_guard<std::mutex> lock(s->impl_->sessions_mutex_);
              if (s->impl_->stopping_) return;  // Double check inside lock
              // Carry the session's totals over to the server before it goes away,
              // so stats() keeps reporting what this connection did. Tied to the
              // erase below, which makes it exactly once even if on_close re-fires.
              auto it = s->impl_->sessions_.find(client_id);
              if (it != s->impl_->sessions_.end() && it->second) {
                s->impl_->absorb_session(it->second);
              }
              s->impl_->sessions_.erase(client_id);
              disconnect_handler = s->impl_->on_multi_disconnect_;
            }
            if (disconnect_handler) disconnect_handler(client_id);
          });
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
        impl->do_accept(self, generation);
      } else {
        auto* impl = self->impl_.get();
        if (impl->stopping_.load()) return;

        // Log only real errors, not operation_aborted
        if (ec != boost::asio::error::operation_aborted) {
          std::string msg = fmt::format("Accept failed: {}", ec.message());
          WIRESTEAD_LOG_ERROR("uds_server", "accept", msg);
          impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION,
                                                "accept", ec, msg, true, 0);
          impl->state_.set(base::LinkState::Error);
          impl->notify_state();
        }

        if (!impl->stopping_.load()) {
          auto timer = std::make_shared<net::steady_timer>(impl->strand_);
          timer->expires_after(std::chrono::milliseconds(100));
          timer->async_wait([self, timer, generation](const boost::system::error_code& ec) {
            auto* retry_impl = self->impl_.get();
            if (!ec && !retry_impl->stopping_.load() && generation == retry_impl->generation_) {
              retry_impl->do_accept(self, generation);
            }
          });
        }
      }
    });
  });
}

void UdsServer::Impl::notify_state() {
  interface::SharedCallback<OnState> cb;
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    cb = on_state_;
  }
  if (cb) (*cb)(state_.get());
}

}  // namespace transport
}  // namespace wirestead
