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

#include "unilink/transport/tcp_server/tcp_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <atomic>
#include <boost/asio.hpp>
#include <future>
#include <iostream>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "unilink/concurrency/io_context_manager.hpp"
#include "unilink/concurrency/thread_safe_state.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/diagnostics/runtime_stats_counter.hpp"
#include "unilink/interface/itcp_acceptor.hpp"
#include "unilink/transport/tcp_server/boost_tcp_acceptor.hpp"
#include "unilink/transport/tcp_server/tcp_server_session.hpp"

namespace unilink {
namespace transport {

namespace net = boost::asio;
using tcp = net::ip::tcp;

struct TcpServer::Impl {
  std::atomic<bool> stopping_{false};
  std::atomic<ClientId> next_client_id_{0};

  std::unique_ptr<net::io_context> owned_ioc_;
  bool owns_ioc_;
  bool uses_global_ioc_;
  net::io_context& ioc_;
  std::jthread ioc_thread_;

  std::unique_ptr<interface::TcpAcceptorInterface> acceptor_;
  config::TcpServerConfig cfg_;

  concurrency::ThreadSafeLinkState state_{base::LinkState::Idle};
  OnBytes on_bytes_;
  OnState on_state_;
  OnBackpressure on_bp_;
  MultiClientConnectHandler on_multi_connect_;
  MultiClientDataHandler on_multi_data_;
  MultiClientDisconnectHandler on_multi_disconnect_;
  diagnostics::RuntimeStatsCounters stats_;

  mutable std::mutex sessions_mutex_;
  std::unordered_map<ClientId, std::shared_ptr<TcpServerSession>> sessions_;

  size_t max_clients_;
  bool client_limit_enabled_;
  bool paused_accept_ = false;

  std::shared_ptr<TcpServerSession> current_session_;

  explicit Impl(const config::TcpServerConfig& cfg)
      : owns_ioc_(false),
        uses_global_ioc_(true),
        ioc_(concurrency::IoContextManager::instance().get_context()),
        cfg_(cfg),
        max_clients_(cfg.max_connections > 0 ? static_cast<size_t>(cfg.max_connections) : 0),
        client_limit_enabled_(cfg.max_connections > 0) {
    try {
      acceptor_ = std::make_unique<BoostTcpAcceptor>(ioc_);
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to create TCP acceptor: " + std::string(e.what()));
    }
    cfg_.validate_and_clamp();
    max_clients_ = cfg_.max_connections > 0 ? static_cast<size_t>(cfg_.max_connections) : 0;
    client_limit_enabled_ = cfg_.max_connections > 0;
  }

  Impl(const config::TcpServerConfig& cfg, std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
       net::io_context& ioc)
      : owns_ioc_(false),
        uses_global_ioc_(false),
        ioc_(ioc),
        acceptor_(std::move(acceptor)),
        cfg_(cfg),
        max_clients_(cfg.max_connections > 0 ? static_cast<size_t>(cfg.max_connections) : 0),
        client_limit_enabled_(cfg.max_connections > 0) {
    if (!acceptor_) {
      throw std::runtime_error("Failed to create TCP acceptor");
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
    OnState cb;
    try {
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        cb = on_state_;
      }
      if (cb) {
        cb(state_.state());
      }
    } catch (...) {
    }
  }

  void apply_accepted_socket_options(tcp::socket& sock) {
    boost::system::error_code ec;

    if (cfg_.tcp_no_delay) {
      sock.set_option(tcp::no_delay(true), ec);
      if (ec) {
        UNILINK_LOG_WARNING("tcp_server", "socket_options", fmt::format("Failed to set TCP_NODELAY: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.keep_alive) {
      sock.set_option(net::socket_base::keep_alive(true), ec);
      if (ec) {
        UNILINK_LOG_WARNING("tcp_server", "socket_options", fmt::format("Failed to set keep_alive: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.send_buffer_size > 0) {
      sock.set_option(net::socket_base::send_buffer_size(static_cast<int>(cfg_.send_buffer_size)), ec);
      if (ec) {
        UNILINK_LOG_WARNING("tcp_server", "socket_options",
                            fmt::format("Failed to set send buffer size: {}", ec.message()));
        ec.clear();
      }
    }

    if (cfg_.receive_buffer_size > 0) {
      sock.set_option(net::socket_base::receive_buffer_size(static_cast<int>(cfg_.receive_buffer_size)), ec);
      if (ec) {
        UNILINK_LOG_WARNING("tcp_server", "socket_options",
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
      UNILINK_LOG_ERROR("tcp_server", "bind",
                        fmt::format("Invalid bind address: {}, {}", cfg_.bind_address, ec.message()));
      state_.set_state(base::LinkState::Error);
      notify_state();
      return;
    }

    if (!acceptor_->is_open()) {
      acceptor_->open(address.is_v6() ? tcp::v6() : tcp::v4(), ec);
      if (ec) {
        UNILINK_LOG_ERROR("tcp_server", "open", fmt::format("Failed to open acceptor: {}", ec.message()));
        state_.set_state(base::LinkState::Error);
        notify_state();
        return;
      }
    }

    acceptor_->bind(tcp::endpoint(address, cfg_.port), ec);
    if (ec) {
      if (cfg_.enable_port_retry && retry_count < cfg_.max_port_retries) {
        auto timer = std::make_shared<net::steady_timer>(ioc_);
        timer->expires_after(std::chrono::milliseconds(cfg_.port_retry_interval_ms));
        timer->async_wait([self, retry_count, timer](const boost::system::error_code& timer_ec) {
          if (!timer_ec) {
            auto* timer_impl = self->get_impl();
            if (!timer_impl->stopping_.load()) {
              timer_impl->attempt_port_binding(self, retry_count + 1);
            }
          }
        });
        return;
      } else {
        UNILINK_LOG_ERROR("tcp_server", "bind", fmt::format("Failed to bind to port {}: {}", cfg_.port, ec.message()));
        state_.set_state(base::LinkState::Error);
        notify_state();
        return;
      }
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      UNILINK_LOG_ERROR("tcp_server", "listen",
                        fmt::format("Failed to listen on port {}: {}", cfg_.port, ec.message()));
      state_.set_state(base::LinkState::Error);
      notify_state();
      return;
    }

    state_.set_state(base::LinkState::Listening);
    notify_state();
    do_accept(self);
  }

  void do_accept(std::shared_ptr<TcpServer> self) {
    if (stopping_.load() || !acceptor_ || !acceptor_->is_open()) return;

    acceptor_->async_accept([self](auto ec, tcp::socket sock) {
      auto* accept_impl = self->get_impl();
      if (accept_impl->stopping_.load()) {
        return;
      }
      if (ec) {
        if (ec != boost::asio::error::operation_aborted) {
          accept_impl->state_.set_state(base::LinkState::Error);
          accept_impl->notify_state();
        }
        if (!accept_impl->state_.is_state(base::LinkState::Closed) && !accept_impl->stopping_.load()) {
          auto timer = std::make_shared<net::steady_timer>(accept_impl->ioc_);
          timer->expires_after(std::chrono::milliseconds(100));
          timer->async_wait([self, timer](const boost::system::error_code&) {
            auto* retry_impl = self->get_impl();
            if (!retry_impl->stopping_.load()) {
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
        std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
        if (accept_impl->sessions_.size() >= accept_impl->max_clients_) {
          boost::system::error_code close_ec;
          sock.close(close_ec);
          accept_impl->paused_accept_ = true;
          return;
        }
      }

      accept_impl->apply_accepted_socket_options(sock);

      auto new_session = std::make_shared<TcpServerSession>(
          accept_impl->ioc_, std::move(sock), accept_impl->cfg_.backpressure_threshold,
          accept_impl->cfg_.idle_timeout_ms, accept_impl->cfg_.backpressure_strategy);

      ClientId client_id = accept_impl->next_client_id_.fetch_add(1);

      std::weak_ptr<TcpServer> weak_self = self;

      new_session->on_bytes([weak_self, client_id](memory::ConstByteSpan data) {
        auto shared_self = weak_self.lock();
        if (!shared_self) return;
        auto* bytes_impl = shared_self->get_impl();

        OnBytes cb;
        MultiClientDataHandler multi_cb;
        {
          std::lock_guard<std::mutex> lock(bytes_impl->sessions_mutex_);
          cb = bytes_impl->on_bytes_;
          multi_cb = bytes_impl->on_multi_data_;
        }
        if (cb) cb(data);
        if (multi_cb) {
          multi_cb(client_id, data);
        }
      });

      OnBackpressure bp_cb;
      {
        std::lock_guard<std::mutex> lock(accept_impl->sessions_mutex_);
        bp_cb = accept_impl->on_bp_;
      }
      if (bp_cb) new_session->on_backpressure(bp_cb);

      new_session->on_close([weak_self, client_id, new_session] {
        auto shared_self = weak_self.lock();
        if (!shared_self) return;
        auto* close_impl = shared_self->get_impl();
        if (close_impl->stopping_.load()) return;

        MultiClientDisconnectHandler disconnect_cb;
        {
          std::lock_guard<std::mutex> lock(close_impl->sessions_mutex_);
          disconnect_cb = close_impl->on_multi_disconnect_;
        }
        if (disconnect_cb) disconnect_cb(client_id);

        bool was_current = false;
        {
          std::lock_guard<std::mutex> lock(close_impl->sessions_mutex_);
          close_impl->sessions_.erase(client_id);
          if (close_impl->paused_accept_ &&
              (!close_impl->client_limit_enabled_ || close_impl->sessions_.size() < close_impl->max_clients_)) {
            close_impl->paused_accept_ = false;
            net::post(close_impl->ioc_, [shared_self] { shared_self->get_impl()->do_accept(shared_self); });
          }
          was_current = (close_impl->current_session_ == new_session);
          if (was_current) {
            if (!close_impl->sessions_.empty())
              close_impl->current_session_ = close_impl->sessions_.begin()->second;
            else
              close_impl->current_session_.reset();
          }
        }
        if (was_current) {
          close_impl->state_.set_state(base::LinkState::Listening);
          close_impl->notify_state();
        }
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

      accept_impl->state_.set_state(base::LinkState::Connected);
      accept_impl->notify_state();
      accept_impl->do_accept(self);
    });
  }

  void perform_cleanup() {
    try {
      boost::system::error_code ec;
      if (acceptor_ && acceptor_->is_open()) {
        acceptor_->close(ec);
      }

      std::vector<std::shared_ptr<TcpServerSession>> sessions_copy;
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        sessions_copy.reserve(sessions_.size());
        for (auto& kv : sessions_) {
          sessions_copy.push_back(kv.second);
        }
        sessions_.clear();
        current_session_.reset();
      }

      for (auto& session : sessions_copy) {
        if (session) {
          session->stop();
        }
      }

      state_.set_state(base::LinkState::Closed);
      notify_state();
    } catch (...) {
    }
  }

  void stop(std::shared_ptr<TcpServer> self) {
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

    if (ioc_.get_executor().running_in_this_thread()) {
      perform_cleanup();
      if (owns_ioc_) ioc_.stop();
      return;
    }

    bool has_active_ioc = owns_ioc_ || (uses_global_ioc_ && concurrency::IoContextManager::instance().is_running());

    if (has_active_ioc && self) {
      auto cleanup_promise = std::make_shared<std::promise<void>>();
      auto cleanup_future = cleanup_promise->get_future();

      std::weak_ptr<TcpServer> weak_self = self;
      net::dispatch(ioc_, [weak_self, cleanup_promise]() {
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

    if (owns_ioc_ && ioc_thread_.joinable()) {
      if (std::this_thread::get_id() == ioc_thread_.get_id()) {
        ioc_thread_.detach();
      } else {
        ioc_thread_.request_stop();
        ioc_thread_.join();
      }
      ioc_.restart();
    }
  }
};

std::shared_ptr<TcpServer> TcpServer::create(const config::TcpServerConfig& cfg) {
  return std::shared_ptr<TcpServer>(new TcpServer(cfg));
}

std::shared_ptr<TcpServer> TcpServer::create(const config::TcpServerConfig& cfg,
                                             std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
                                             net::io_context& ioc) {
  return std::shared_ptr<TcpServer>(new TcpServer(cfg, std::move(acceptor), ioc));
}

TcpServer::TcpServer(const config::TcpServerConfig& cfg) : impl_(std::make_unique<Impl>(cfg)) {}

TcpServer::TcpServer(const config::TcpServerConfig& cfg, std::unique_ptr<interface::TcpAcceptorInterface> acceptor,
                     net::io_context& ioc)
    : impl_(std::make_unique<Impl>(cfg, std::move(acceptor), ioc)) {}

TcpServer::~TcpServer() {
  if (impl_ && !impl_->state_.is_state(base::LinkState::Closed)) {
    // Pass nullptr to stop() to indicate we are in destructor and cannot use shared_from_this
    impl_->stop(nullptr);
  }
}

TcpServer::TcpServer(TcpServer&&) noexcept = default;
TcpServer& TcpServer::operator=(TcpServer&&) noexcept = default;

void TcpServer::start() {
  auto impl = get_impl();
  auto current = impl->state_.state();
  if (current == base::LinkState::Listening || current == base::LinkState::Connected ||
      current == base::LinkState::Connecting) {
    return;
  }
  impl->stopping_.store(false);

  if (impl->uses_global_ioc_) {
    auto& manager = concurrency::IoContextManager::instance();
    if (!manager.is_running()) {
      manager.start();
    }
  }

  if (!impl->acceptor_) {
    impl->state_.set_state(base::LinkState::Error);
    impl->notify_state();
    return;
  }

  if (impl->owns_ioc_) {
    impl->ioc_thread_ = std::jthread([impl](std::stop_token st) {
      try {
        std::stop_callback cb(st, [impl] { impl->ioc_.stop(); });
        impl->ioc_.run();
      } catch (...) {
      }
    });
  }
  auto self = shared_from_this();
  if (impl->ioc_.get_executor().running_in_this_thread()) {
    if (!impl->stopping_.load()) {
      impl->attempt_port_binding(self, 0);
    }
  } else {
    net::dispatch(impl->ioc_, [self] {
      auto impl = self->get_impl();
      if (impl->stopping_.load()) return;
      impl->attempt_port_binding(self, 0);
    });
  }
}

void TcpServer::stop() { impl_->stop(shared_from_this()); }

void TcpServer::request_stop() {
  auto impl = get_impl();
  if (impl->stopping_.load()) return;
  auto self = shared_from_this();
  net::post(impl->ioc_, [self] { self->stop(); });
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

boost::asio::any_io_executor TcpServer::get_executor() { return impl_->ioc_.get_executor(); }

wrapper::RuntimeStats TcpServer::stats() const {
  auto impl = get_impl();
  auto aggregate = impl->stats_.snapshot(0, 0, false);
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
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
    aggregate.max_queued_bytes += session_stats.max_queued_bytes;
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

void TcpServer::on_bytes(OnBytes cb) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_bytes_ = std::move(cb);
}
void TcpServer::on_state(OnState cb) {
  std::lock_guard<std::mutex> lock(impl_->sessions_mutex_);
  impl_->on_state_ = std::move(cb);
}
void TcpServer::on_backpressure(OnBackpressure cb) {
  auto impl = get_impl();
  std::shared_ptr<TcpServerSession> session;
  OnBackpressure bp_cb;
  {
    std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
    impl->on_bp_ = std::move(cb);
    bp_cb = impl->on_bp_;
    session = impl->current_session_;
  }

  if (session) session->on_backpressure(bp_cb);
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
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  auto it = impl->sessions_.find(client_id);
  if (it != impl->sessions_.end() && it->second && it->second->alive()) {
    return it->second->async_write_copy(data);
  }
  impl->stats_.record_failed_send();
  return false;
}

bool TcpServer::try_send_to_client(ClientId client_id, std::string_view message) {
  return try_send_to_client(client_id,
                            memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(message.data()), message.size()));
}

bool TcpServer::try_send_to_client(ClientId client_id, memory::ConstByteSpan data) {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  auto it = impl->sessions_.find(client_id);
  if (it != impl->sessions_.end() && it->second && it->second->alive()) {
    return it->second->async_try_write_copy(data);
  }
  impl->stats_.record_failed_send();
  return false;
}

size_t TcpServer::client_count() const {
  auto impl = get_impl();
  std::lock_guard<std::mutex> lock(impl->sessions_mutex_);
  size_t alive = 0;
  for (const auto& entry : impl->sessions_)
    if (entry.second && entry.second->alive()) ++alive;
  return alive;
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
  std::lock_guard<std::mutex> l(impl_->sessions_mutex_);
  impl_->on_multi_data_ = std::move(h);
}
void TcpServer::on_multi_disconnect(MultiClientDisconnectHandler h) {
  std::lock_guard<std::mutex> l(impl_->sessions_mutex_);
  impl_->on_multi_disconnect_ = std::move(h);
}

void TcpServer::set_client_limit(size_t max) {
  auto impl = get_impl();
  std::lock_guard<std::mutex> l(impl->sessions_mutex_);
  impl->max_clients_ = max;
  if (max == 0) {
    impl->client_limit_enabled_ = false;
  } else {
    impl->client_limit_enabled_ = true;
  }
  if (impl->paused_accept_ && (!impl->client_limit_enabled_ || impl->sessions_.size() < impl->max_clients_)) {
    impl->paused_accept_ = false;
    net::post(impl->ioc_, [self = shared_from_this()] { self->get_impl()->do_accept(self); });
  }
}

base::LinkState TcpServer::state() const { return get_impl()->state_.state(); }

}  // namespace transport
}  // namespace unilink
