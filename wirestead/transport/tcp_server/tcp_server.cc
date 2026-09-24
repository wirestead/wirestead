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

#include "wirestead/transport/tcp_server/tcp_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/diagnostics/runtime_stats_counter.hpp"
#include "wirestead/interface/itcp_acceptor.hpp"
#include "wirestead/transport/base/error_info_holder.hpp"
#include "wirestead/transport/base/stop_test_hook.hpp"
#include "wirestead/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "wirestead/transport/tcp_server/ssl_tcp_socket.hpp"
#include "wirestead/transport/tcp_server/tcp_server_session.hpp"

namespace wirestead {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

struct TcpServer::Impl {
  std::atomic<bool> stopping_{false};
  // #503: stop() can call perform_cleanup() from two different paths - the
  // dispatched-onto-the-io_context call, and (if that doesn't complete
  // within the timeout below) a direct fallback call on the stopping
  // thread. Under slow/instrumented builds (TSAN) the dispatched call can
  // still be mid-flight when the timeout fires, so both could run
  // perform_cleanup()'s body concurrently - including acceptor_->close(),
  // racing with the io_context thread's own concurrent async_accept.
  // Guards perform_cleanup() to run its body at most once per stop cycle,
  // reset back to false in start() alongside stopping_.
  std::atomic<bool> cleanup_started_{false};
  std::atomic<ClientId> next_client_id_{0};

  std::unique_ptr<net::io_context> owned_ioc_;
  bool owns_ioc_;
  bool uses_shared_context_{false};
  net::io_context& ioc_;
  net::strand<net::io_context::executor_type> strand_;
  std::atomic<uint64_t> generation_{0};
  // start() may fail validation before dispatching any executor work.
  bool run_dispatched_ = false;
  std::unique_ptr<net::executor_work_guard<net::io_context::executor_type>> work_guard_;
  std::jthread ioc_thread_;

  std::unique_ptr<interface::TcpAcceptorInterface> acceptor_;
  config::TcpServerConfig cfg_;

  concurrency::AtomicLinkState state_{base::LinkState::Idle};
  // Shared snapshots for the handlers the io thread copies out per received
  // chunk - a std::function copy allocates whenever the target outgrows its
  // small-object buffer. See interface::SharedCallback. The connect/disconnect
  // handlers below stay plain: they fire once per connection, not per chunk.
  interface::SharedCallback<OnBytes> on_bytes_;
  interface::SharedCallback<OnState> on_state_;
  interface::SharedCallback<OnBackpressure> on_bp_;
  MultiClientConnectHandler on_multi_connect_;
  interface::SharedCallback<MultiClientDataHandler> on_multi_data_;
  MultiClientDisconnectHandler on_multi_disconnect_;
  diagnostics::RuntimeStatsCounters stats_;

  mutable std::mutex sessions_mutex_;
  // D-1: shutdown requested and shutdown completed are separate states. The
  // cleanup that tears the sessions down is what completes it, so waiting
  // callers wait for its signal rather than for a lock.
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
  std::unordered_map<ClientId, std::shared_ptr<TcpServerSession>> sessions_;

  size_t max_clients_;
  bool client_limit_enabled_;

  std::shared_ptr<TcpServerSession> current_session_;

  ErrorInfoHolder error_info_holder_{"tcp_server"};

  explicit Impl(const config::TcpServerConfig& cfg, bool use_shared_context)
      : owned_ioc_(use_shared_context ? nullptr : std::make_unique<net::io_context>()),
        owns_ioc_(!use_shared_context),
        uses_shared_context_(use_shared_context),
        ioc_(use_shared_context ? concurrency::IoContextManager::instance().get_context() : *owned_ioc_),
        strand_(net::make_strand(ioc_)),
        cfg_(cfg),
        max_clients_(cfg.max_connections > 0 ? static_cast<size_t>(cfg.max_connections) : 0),
        client_limit_enabled_(cfg.max_connections > 0) {
    try {
      acceptor_ = std::make_unique<BoostTcpAcceptor>(ioc_);
    } catch (const std::exception& e) {
      throw diagnostics::BuilderException("Failed to create TCP acceptor: " + std::string(e.what()), "tcp_server");
    }
    cfg_.validate_and_clamp();
    max_clients_ = cfg_.max_connections > 0 ? static_cast<size_t>(cfg_.max_connections) : 0;
    client_limit_enabled_ = cfg_.max_connections > 0;
  }

  Impl(const config::TcpServerConfig& cfg, std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
       net::io_context& ioc)
      : owns_ioc_(false),
        ioc_(ioc),
        strand_(net::make_strand(ioc_)),
        acceptor_(std::move(acceptor)),
        cfg_(cfg),
        max_clients_(cfg.max_connections > 0 ? static_cast<size_t>(cfg.max_connections) : 0),
        client_limit_enabled_(cfg.max_connections > 0) {
    if (!acceptor_) {
      throw diagnostics::BuilderException("Failed to create TCP acceptor", "tcp_server");
    }
    cfg_.validate_and_clamp();
    max_clients_ = cfg_.max_connections > 0 ? static_cast<size_t>(cfg_.max_connections) : 0;
    client_limit_enabled_ = cfg_.max_connections > 0;
  }

  ~Impl() {
    try {
      stopping_.store(true);
      if (ioc_thread_.joinable()) {
        if (std::this_thread::get_id() == ioc_thread_.get_id()) {
          ioc_thread_.detach();
        } else {
          ioc_thread_.request_stop();
          ioc_thread_.join();
        }
      }
      perform_cleanup();
    } catch (...) {
    }
  }

  void notify_state() {
    if (stopping_.load()) return;
    interface::SharedCallback<OnState> cb;
    try {
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        cb = on_state_;
      }
      if (cb) {
        (*cb)(state_.get());
      }
    } catch (...) {
    }
  }

  // One context for the whole server, shared by every accepted connection.
  // Built once in start() so a bad certificate fails there rather than on the
  // first client.
#ifdef WIRESTEAD_TLS_ENABLED
  std::shared_ptr<boost::asio::ssl::context> ssl_context_;
#endif

  // Builds the ssl::context, or reports why it cannot. Called from start(), so
  // a certificate problem surfaces as a start failure with a message rather
  // than as connections that mysteriously drop at handshake.
  std::string init_tls() {
    // Half a TLS config is a request for TLS, not a request for plaintext. An
    // empty environment variable or a typo would otherwise leave tls_enabled()
    // false and bring the server up unencrypted without saying anything, which
    // is the one outcome this whole path exists to prevent.
    if (!cfg_.tls_certificate_file.empty() != !cfg_.tls_private_key_file.empty()) {
      return cfg_.tls_certificate_file.empty() ? "TLS needs a certificate as well as a private key"
                                               : "TLS needs a private key as well as a certificate";
    }
    if (!cfg_.tls_enabled()) return {};
#ifndef WIRESTEAD_TLS_ENABLED
    return "TLS was configured but this build has WIRESTEAD_ENABLE_TLS=OFF";
#else
    namespace ssl = boost::asio::ssl;
    try {
      auto ctx = std::make_shared<ssl::context>(ssl::context::tls_server);
      // Anything below TLS 1.2 is broken in ways nobody should opt into by
      // accident, so the floor is not configurable.
      ctx->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                       ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1 | ssl::context::single_dh_use);
      ctx->use_certificate_chain_file(cfg_.tls_certificate_file);
      ctx->use_private_key_file(cfg_.tls_private_key_file, ssl::context::pem);
      ssl_context_ = std::move(ctx);
      return {};
    } catch (const std::exception& e) {
      return std::string("Failed to load TLS certificate or key: ") + e.what();
    }
#endif
  }

  std::shared_ptr<TcpServerSession> make_session(tcp::socket sock) {
#ifdef WIRESTEAD_TLS_ENABLED
    if (ssl_context_) {
      return std::make_shared<TcpServerSession>(
          ioc_, std::make_unique<SslTcpSocket>(std::move(sock), ssl_context_), cfg_.backpressure_threshold,
          cfg_.idle_timeout_ms, cfg_.backpressure_strategy, cfg_.enable_memory_pool, cfg_.read_buffer_size);
    }
#endif
    return std::make_shared<TcpServerSession>(ioc_, std::move(sock), cfg_.backpressure_threshold, cfg_.idle_timeout_ms,
                                              cfg_.backpressure_strategy, cfg_.enable_memory_pool,
                                              cfg_.read_buffer_size);
  }

  void apply_accepted_socket_options(tcp::socket& sock) {
    boost::system::error_code ec;

    if (cfg_.tcp_no_delay) {
      sock.set_option(tcp::no_delay(true), ec);
      if (ec) {
        WIRESTEAD_LOG_WARNING("tcp_server", "socket_options",
                              fmt::format("Failed to set TCP_NODELAY: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.keep_alive) {
      sock.set_option(net::socket_base::keep_alive(true), ec);
      if (ec) {
        WIRESTEAD_LOG_WARNING("tcp_server", "socket_options",
                              fmt::format("Failed to set keep_alive: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.send_buffer_size > 0) {
      sock.set_option(net::socket_base::send_buffer_size(static_cast<int>(cfg_.send_buffer_size)), ec);
      if (ec) {
        WIRESTEAD_LOG_WARNING("tcp_server", "socket_options",
                              fmt::format("Failed to set send buffer size: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.receive_buffer_size > 0) {
      sock.set_option(net::socket_base::receive_buffer_size(static_cast<int>(cfg_.receive_buffer_size)), ec);
      if (ec) {
        WIRESTEAD_LOG_WARNING("tcp_server", "socket_options",
                              fmt::format("Failed to set receive buffer size: {}", ec.message()));
        ec.clear();
      }
    }
  }

  void attempt_port_binding(std::shared_ptr<TcpServer> self, int retry_count) {
    if (stopping_.load()) return;
    boost::system::error_code ec;

    auto address = net::ip::make_address(cfg_.bind_address, ec);
    if (ec) {
      std::string msg = fmt::format("Invalid bind address: {}, {}", cfg_.bind_address, ec.message());
      WIRESTEAD_LOG_ERROR("tcp_server", "bind", msg);
      error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION, "bind",
                                      ec, msg, false, static_cast<uint32_t>(retry_count));
      state_.set(base::LinkState::Error);
      notify_state();
      return;
    }

    if (!acceptor_->is_open()) {
      acceptor_->open(address.is_v6() ? tcp::v6() : tcp::v4(), ec);
      if (ec) {
        std::string msg = fmt::format("Failed to open acceptor: {}", ec.message());
        WIRESTEAD_LOG_ERROR("tcp_server", "open", msg);
        error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "open", ec,
                                        msg, false, static_cast<uint32_t>(retry_count));
        state_.set(base::LinkState::Error);
        notify_state();
        return;
      }
    }

    acceptor_->bind(tcp::endpoint(address, cfg_.port), ec);
    if (ec) {
      if (cfg_.enable_port_retry && retry_count < cfg_.max_port_retries) {
        auto timer = std::make_shared<net::steady_timer>(strand_);
        timer->expires_after(std::chrono::milliseconds(cfg_.port_retry_interval_ms));
        timer->async_wait(
            [self, retry_count, timer, generation = generation_.load()](const boost::system::error_code& timer_ec) {
              if (!timer_ec) {
                auto* timer_impl = self->get_impl();
                if (!timer_impl->stopping_.load() && timer_impl->generation_.load() == generation) {
                  timer_impl->attempt_port_binding(self, retry_count + 1);
                }
              }
            });
        return;
      } else {
        std::string msg = fmt::format("Failed to bind to port {}: {}", cfg_.port, ec.message());
        WIRESTEAD_LOG_ERROR("tcp_server", "bind", msg);
        error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "bind",
                                        ec, msg, false, static_cast<uint32_t>(retry_count));
        state_.set(base::LinkState::Error);
        notify_state();
        return;
      }
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      std::string msg = fmt::format("Failed to listen on port {}: {}", cfg_.port, ec.message());
      WIRESTEAD_LOG_ERROR("tcp_server", "listen", msg);
      error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "listen",
                                      ec, msg, false, static_cast<uint32_t>(retry_count));
      state_.set(base::LinkState::Error);
      notify_state();
      return;
    }

    state_.set(base::LinkState::Listening);
    notify_state();
    do_accept(self);
  }

  void do_accept(std::shared_ptr<TcpServer> self) {
    if (stopping_.load() || !acceptor_ || !acceptor_->is_open()) return;

    acceptor_->async_accept([self, generation = generation_.load()](auto ec, tcp::socket sock) {
      net::dispatch(self->get_impl()->strand_, [self, generation, ec, sock = std::move(sock)]() mutable {
        auto* accept_impl = self->get_impl();
        if (accept_impl->stopping_.load() || accept_impl->generation_.load() != generation) {
          return;
        }
        if (ec) {
          if (ec != boost::asio::error::operation_aborted) {
            accept_impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR,
                                                         diagnostics::ErrorCategory::CONNECTION, "accept", ec,
                                                         fmt::format("Accept failed: {}", ec.message()), true, 0);
            accept_impl->state_.set(base::LinkState::Error);
            accept_impl->notify_state();
          }
          if (!accept_impl->state_.is_state(base::LinkState::Closed) && !accept_impl->stopping_.load()) {
            auto timer = std::make_shared<net::steady_timer>(accept_impl->strand_);
            timer->expires_after(std::chrono::milliseconds(100));
            timer->async_wait([self, timer, generation](const boost::system::error_code&) {
              auto* retry_impl = self->get_impl();
              if (!retry_impl->stopping_.load() && retry_impl->generation_.load() == generation) {
                retry_impl->do_accept(self);
              }
            });
          }
          return;
        }

        boost::system::error_code ep_ec;
        auto rep = sock.remote_endpoint(ep_ec);
        std::string client_info = "unknown";
        if (!ep_ec) {
          client_info = fmt::format("{}:{}", rep.address().to_string(), rep.port());
        }

        if (accept_impl->client_limit_enabled_) {
          bool over_limit;
          {
            std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
            over_limit = accept_impl->sessions_.size() >= accept_impl->max_clients_;
          }
          if (over_limit) {
            // #437: accept and immediately close over-limit connections
            // instead of pausing the accept loop entirely - a client whose
            // TCP handshake already completed while paused would otherwise
            // sit connected but silent until a slot frees up.
            boost::system::error_code close_ec;
            sock.close(close_ec);
            accept_impl->do_accept(self);
            return;
          }
        }

        accept_impl->apply_accepted_socket_options(sock);

        // The session only talks to TcpSocketInterface, so TLS is a matter of
        // which implementation it gets wrapped in here. Everything downstream -
        // reads, writes, backpressure, stats - is identical either way.
        auto new_session = accept_impl->make_session(std::move(sock));

        ClientId client_id = accept_impl->next_client_id_.fetch_add(1);

        std::weak_ptr<TcpServer> weak_self = self;

        new_session->on_bytes([weak_self, client_id](memory::ConstByteSpan data) {
          auto shared_self = weak_self.lock();
          if (!shared_self) return;
          auto* bytes_impl = shared_self->get_impl();

          interface::SharedCallback<OnBytes> cb;
          interface::SharedCallback<MultiClientDataHandler> multi_cb;
          {
            std::lock_guard<std::mutex> lock(bytes_impl->sessions_mutex_);
            cb = bytes_impl->on_bytes_;
            multi_cb = bytes_impl->on_multi_data_;
          }
          if (cb) (*cb)(data);
          if (multi_cb) {
            (*multi_cb)(client_id, data);
          }
        });

        interface::SharedCallback<OnBackpressure> bp_cb;
        {
          std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
          bp_cb = accept_impl->on_bp_;
        }
        if (bp_cb) new_session->on_backpressure(*bp_cb);

        new_session->on_close([weak_self, client_id, new_session, generation] {
          auto shared_self = weak_self.lock();
          if (!shared_self) return;
          net::dispatch(shared_self->get_impl()->strand_, [shared_self, client_id, new_session, generation] {
            auto* close_impl = shared_self->get_impl();
            if (close_impl->stopping_.load() || close_impl->generation_.load() != generation) return;

            MultiClientDisconnectHandler disconnect_cb;
            {
              std::lock_guard<std::mutex> lock(close_impl->sessions_mutex_);
              disconnect_cb = close_impl->on_multi_disconnect_;
            }
            if (disconnect_cb) disconnect_cb(client_id);

            bool was_current = false;
            {
              std::lock_guard<std::mutex> lock(close_impl->sessions_mutex_);
              // Carry the session's totals over to the server before it goes away,
              // so stats() keeps reporting what this connection did. Tied to the
              // erase below, which makes it exactly once even if on_close re-fires.
              auto it = close_impl->sessions_.find(client_id);
              if (it != close_impl->sessions_.end() && it->second) {
                close_impl->stats_.absorb(it->second->stats());
              }
              close_impl->sessions_.erase(client_id);
              was_current = (close_impl->current_session_ == new_session);
              if (was_current) {
                if (!close_impl->sessions_.empty())
                  close_impl->current_session_ = close_impl->sessions_.begin()->second;
                else
                  close_impl->current_session_.reset();
              }
            }
            if (was_current) {
              close_impl->state_.set(base::LinkState::Listening);
              close_impl->notify_state();
            }
          });
        });

        // alive_ must be true before the session enters sessions_, so that
        // broadcast() callers who observe client_count() >= 1 are guaranteed
        // to pass the alive() check inside async_try_write_shared().
        new_session->start();

        {
          std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
          accept_impl->sessions_.emplace(client_id, new_session);
          accept_impl->current_session_ = new_session;
        }

        MultiClientConnectHandler connect_cb;
        {
          std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
          connect_cb = accept_impl->on_multi_connect_;
        }
        if (connect_cb) connect_cb(client_id, client_info);

        accept_impl->state_.set(base::LinkState::Connected);
        accept_impl->notify_state();
        accept_impl->do_accept(self);
      });
    });
  }

  void finish_cleanup() {
    state_.set(base::LinkState::Closed);
    if (work_guard_) work_guard_->reset();
    mark_cleanup_done();
  }

  void perform_cleanup(std::shared_ptr<TcpServer> self = {}) {
    if (cleanup_started_.exchange(true)) return;
    boost::system::error_code ec;
    if (acceptor_) acceptor_->close(ec);
    std::vector<std::shared_ptr<TcpServerSession>> sessions;
    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      for (auto& entry : sessions_) sessions.push_back(entry.second);
      sessions_.clear();
      current_session_.reset();
    }
    if (sessions.empty()) {
      finish_cleanup();
      return;
    }
    // Completion includes every session's callback body and cleanup. Each
    // session owns its outstanding I/O; final state changes are serialized
    // with accept/retry handlers on the server's management strand.
    auto remaining = std::make_shared<size_t>(sessions.size());
    for (auto& session : sessions) {
      session->async_stop([this, self, remaining] {
        net::post(strand_, [this, self, remaining] {
          if (--*remaining == 0) finish_cleanup();
        });
      });
    }
  }

  void stop(std::shared_ptr<TcpServer> self) {
    const bool on_executor = ioc_.get_executor().running_in_this_thread();
    bool first;
    {
      std::lock_guard<std::mutex> lock(target_admission_mtx_);
      first = !stopping_.exchange(true);
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
    if (!owns_ioc_) return;
    if (work_guard_) work_guard_->reset();
    if (!ioc_thread_.joinable()) return;
    if (std::this_thread::get_id() == ioc_thread_.get_id()) {
      ioc_thread_.detach();
    } else {
      ioc_thread_.join();
    }
  }
};

std::shared_ptr<TcpServer> TcpServer::create(const config::TcpServerConfig& cfg, bool use_shared_context) {
  return std::shared_ptr<TcpServer>(new TcpServer(cfg, use_shared_context));
}

std::shared_ptr<TcpServer> TcpServer::create(const config::TcpServerConfig& cfg,
                                             std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
                                             net::io_context& ioc) {
  return std::shared_ptr<TcpServer>(new TcpServer(cfg, std::move(acceptor), ioc));
}

TcpServer::TcpServer(const config::TcpServerConfig& cfg, bool use_shared_context)
    : impl_(std::make_unique<Impl>(cfg, use_shared_context)) {}

TcpServer::TcpServer(const config::TcpServerConfig& cfg, std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
                     net::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, std::move(acceptor), ioc)) {}

TcpServer::~TcpServer() {
  if (impl_) {
    // Pass nullptr to stop() to indicate we are in destructor and cannot use shared_from_this
    impl_->stop(nullptr);
  }
}

TcpServer::TcpServer(TcpServer&&) noexcept = default;
TcpServer& TcpServer::operator=(TcpServer&&) noexcept = default;

void TcpServer::start() {
  auto impl = get_impl();
  auto current = impl->state_.get();
  if (current == base::LinkState::Listening || current == base::LinkState::Connected ||
      current == base::LinkState::Connecting) {
    return;
  }
  const auto generation = impl->generation_.fetch_add(1) + 1;
  impl->run_dispatched_ = false;
  impl->stopping_.store(false);
  impl->cleanup_started_.store(false);
  {
    std::lock_guard<std::mutex> lock(impl->stop_mtx_);
    impl->cleanup_done_ = false;
  }
  // Restart contract (#444): stats() resets on restart. The server-level
  // counters now outlive the sessions that fed them, so clearing them here is
  // what keeps that promise - before absorption they were empty and a restart
  // zeroed the aggregate for free.
  impl->stats_.reset(0);

  // Load the certificate before binding. A server asked for TLS that cannot
  // provide it must not come up in plaintext instead - that is the failure mode
  // where everything looks healthy and nothing is encrypted.
  if (const auto tls_error = impl->init_tls(); !tls_error.empty()) {
    WIRESTEAD_LOG_ERROR("tcp_server", "start", tls_error);
    impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONFIGURATION,
                                          "start", {}, tls_error, false, 0);
    impl->state_.set(base::LinkState::Error);
    impl->notify_state();
    return;
  }

  if (impl->uses_shared_context_) {
    auto& manager = concurrency::IoContextManager::instance();
    if (!manager.is_running()) {
      manager.start();
    }
  }

  if (!impl->acceptor_) {
    impl->error_info_holder_.record_error(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::SYSTEM, "start",
                                          {}, "No acceptor available", false, 0);
    impl->state_.set(base::LinkState::Error);
    impl->notify_state();
    return;
  }

  if (impl->owns_ioc_ && !impl->ioc_thread_.joinable()) {
    if (impl->ioc_.stopped()) {
      impl->ioc_.restart();
    }
    impl->work_guard_ =
        std::make_unique<net::executor_work_guard<net::io_context::executor_type>>(impl->ioc_.get_executor());
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      wirestead::concurrency::run_io_thread_init();
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_.stop(); });
        impl->ioc_.run();
      } catch (...) {
      }
    });
  }
  impl->run_dispatched_ = true;
  auto self = shared_from_this();
  net::dispatch(impl->strand_, [self, generation] {
    auto* impl = self->get_impl();
    if (impl->stopping_.load() || impl->generation_.load() != generation) return;
    impl->attempt_port_binding(self, 0);
  });
}

void TcpServer::stop() { impl_->stop(shared_from_this()); }

void TcpServer::request_stop() {
  auto impl = get_impl();
  if (impl->stopping_.load()) return;
  auto self = shared_from_this();
  net::post(impl->strand_, [self, generation = impl->generation_.load()] {
    if (self->get_impl()->generation_.load() == generation) self->stop();
  });
}

bool TcpServer::is_connected() const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  return impl->current_session_ && impl->current_session_->alive();
}

bool TcpServer::is_backpressure_active() const { return false; }

bool TcpServer::is_backpressure_active(ClientId client_id) const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  auto it = impl->sessions_.find(client_id);
  if (it != impl->sessions_.end() && it->second) {
    return it->second->is_backpressure_active();
  }
  return false;
}

std::optional<size_t> TcpServer::write_queue_limit(ClientId client_id) const {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it == impl_->sessions_.end() || !it->second) return std::nullopt;
  return it->second->write_queue_limit();
}

boost::asio::any_io_executor TcpServer::get_executor() { return impl_->strand_; }

wrapper::RuntimeStats TcpServer::stats() const {
  auto impl = get_impl();
  // Snapshot retained totals and live sessions under the same lock as the
  // disconnect transfer; otherwise a retiring session can disappear from both.
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  auto aggregate = impl->stats_.snapshot(0, 0, false);
  for (const auto& entry : impl->sessions_) {
    if (!entry.second) continue;
    const auto session_stats = entry.second->stats();
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
    // Peak, not a total: summing per-session peaks would report a depth no
    // session ever reached, because the peaks need not have been simultaneous.
    aggregate.max_queued_bytes = std::max(aggregate.max_queued_bytes, session_stats.max_queued_bytes);
    aggregate.backpressure_active = aggregate.backpressure_active || session_stats.backpressure_active;
  }
  return aggregate;
}

void TcpServer::reset_stats() {
  auto impl = get_impl();
  impl->stats_.reset(0);
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  for (const auto& entry : impl->sessions_) {
    if (entry.second) entry.second->reset_stats();
  }
}

std::optional<diagnostics::ErrorInfo> TcpServer::last_error_info() const {
  return get_impl()->error_info_holder_.last_error_info();
}

bool TcpServer::async_write_copy(memory::ConstByteSpan data) {
  auto impl = get_impl();
  if (impl->stopping_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_write_copy(data);
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::async_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  if (impl->stopping_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_write_move(std::move(data));
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || !data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_write_shared(std::move(data));
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::async_try_write_copy(memory::ConstByteSpan data) {
  auto impl = get_impl();
  if (impl->stopping_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_try_write_copy(data);
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::async_try_write_move(std::vector<uint8_t>&& data) {
  auto impl = get_impl();
  if (impl->stopping_.load()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_try_write_move(std::move(data));
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) {
  auto impl = get_impl();
  if (impl->stopping_.load() || !data || data->empty()) {
    impl->stats_.record_failed_send();
    return false;
  }
  std::shared_ptr<TcpServerSession> session;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    session = impl->current_session_;
  }

  if (session && session->alive()) {
    return session->async_try_write_shared(std::move(data));
  }
  impl->stats_.record_failed_send();
  return false;
}

// Each setter builds the shared snapshot before taking the lock, so the
// allocation stays outside the critical section the io thread contends on.
void TcpServer::on_bytes(OnBytes cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bytes_ = std::move(shared);
}
void TcpServer::on_state(OnState cb) {
  auto shared = interface::share_callback(std::move(cb));
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_state_ = std::move(shared);
}
void TcpServer::on_backpressure(OnBackpressure cb) {
  auto impl = get_impl();
  auto shared = interface::share_callback(std::move(cb));
  std::shared_ptr<TcpServerSession> session;
  interface::SharedCallback<OnBackpressure> bp_cb;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    impl->on_bp_ = std::move(shared);
    bp_cb = impl->on_bp_;
    session = impl->current_session_;
  }

  if (session) session->on_backpressure(bp_cb ? *bp_cb : OnBackpressure{});
}

bool TcpServer::broadcast(std::string_view message) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(message.begin(), message.end());
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  bool sent = false;
  bool attempted = false;
  for (auto& entry : impl->sessions_) {
    auto& session = entry.second;
    if (session && session->alive()) {
      attempted = true;
      if (session->async_try_write_shared(shared_data)) sent = true;
    }
  }
  if (!attempted) impl->stats_.record_failed_send();
  return sent;
}

bool TcpServer::broadcast(memory::ConstByteSpan data) {
  auto shared_data = std::make_shared<const std::vector<uint8_t>>(data.begin(), data.end());
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  bool sent = false;
  bool attempted = false;
  for (auto& entry : impl->sessions_) {
    auto& session = entry.second;
    if (session && session->alive()) {
      attempted = true;
      if (session->async_try_write_shared(shared_data)) sent = true;
    }
  }
  if (!attempted) impl->stats_.record_failed_send();
  return sent;
}

bool TcpServer::send_to_client(ClientId client_id, std::string_view message) {
  return send_to_client(client_id,
                        memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool TcpServer::send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  const auto result = write_target(client_id, data, false);
  if (auto hook = detail::g_tcp_server_write_result_hook.load()) hook(result);
  return result.accepted();
}

bool TcpServer::try_send_to_client(ClientId client_id, std::string_view message) {
  return try_send_to_client(client_id,
                            memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool TcpServer::try_send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  const auto result = write_target(client_id, data, true);
  if (auto hook = detail::g_tcp_server_write_result_hook.load()) hook(result);
  return result.accepted();
}

wrapper::SendResult TcpServer::target_state() const {
  if (impl_->stopping_.load()) {
    std::lock_guard<std::mutex> lock(impl_->stop_mtx_);
    return wrapper::SendResult::reject(impl_->cleanup_done_ ? wrapper::SendRejection::NotStarted
                                                            : wrapper::SendRejection::Stopping);
  }
  if (impl_->generation_.load() == 0) return wrapper::SendResult::reject(wrapper::SendRejection::NotStarted);
  return wrapper::SendResult::accept();
}

wrapper::SendResult TcpServer::write_target(ClientId client_id, memory::ConstByteSpan data, bool try_only) {
  std::lock_guard<std::mutex> admission_lock(impl_->target_admission_mtx_);
  const auto state = target_state();
  if (!state.accepted()) {
    impl_->stats_.record_failed_send();
    return state;
  }
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  auto it = impl_->sessions_.find(client_id);
  if (it == impl_->sessions_.end() || !it->second) {
    impl_->stats_.record_failed_send();
    return wrapper::SendResult::reject(wrapper::SendRejection::NotReady);
  }
  return try_only ? it->second->try_write_copy(data) : it->second->write_copy(data);
}

size_t TcpServer::client_count() const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  size_t alive = 0;
  for (const auto& entry : impl->sessions_)
    if (entry.second && entry.second->alive()) ++alive;
  return alive;
}

std::optional<wrapper::RuntimeStats> TcpServer::client_stats(ClientId client_id) const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  auto it = impl->sessions_.find(client_id);
  if (it == impl->sessions_.end() || !it->second) return std::nullopt;
  return it->second->stats();
}

std::vector<ClientId> TcpServer::connected_clients() const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  std::vector<ClientId> connected_clients;
  connected_clients.reserve(impl->sessions_.size());
  for (const auto& entry : impl->sessions_)
    if (entry.second && entry.second->alive()) connected_clients.push_back(entry.first);
  return connected_clients;
}

void TcpServer::on_multi_connect(MultiClientConnectHandler h) {
  std::lock_guard<std::mutex> l(impl_->sessions_mutex_);
  impl_->on_multi_connect_ = std::move(h);
}
void TcpServer::on_multi_data(MultiClientDataHandler h) {
  auto shared = interface::share_callback(std::move(h));
  std::lock_guard<std::mutex> l(impl_->sessions_mutex_);
  impl_->on_multi_data_ = std::move(shared);
}
void TcpServer::on_multi_disconnect(MultiClientDisconnectHandler h) {
  std::lock_guard<std::mutex> l(impl_->sessions_mutex_);
  impl_->on_multi_disconnect_ = std::move(h);
}

void TcpServer::set_client_limit(size_t max) {
  auto impl = get_impl();
  if (max > base::constants::MAX_MAX_CONNECTIONS) {
    max = base::constants::MAX_MAX_CONNECTIONS;
  }
  std::lock_guard<std::mutex> l(impl->sessions_mutex_);
  impl->max_clients_ = max;
  if (max == 0) {
    impl->client_limit_enabled_ = false;
  } else {
    impl->client_limit_enabled_ = true;
  }
}

base::LinkState TcpServer::state() const { return get_impl()->state_.get(); }

}  // namespace transport
}  // namespace wirestead
