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

#include "wirestead/builder/tcp_server_builder.hpp"

#include <boost/asio/io_context.hpp>

#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/config/validation.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace builder {

TcpServerBuilder::TcpServerBuilder(uint16_t port)
    : port_(port),
      bind_address_("0.0.0.0"),
      auto_start_(false),
      independent_context_(false),
      shared_context_(false),
      max_clients_(0),
      client_limit_enabled_(false),
      port_retry_enabled_(false),
      max_port_retries_(3),
      port_retry_interval_ms_(1000),
      idle_timeout_(0),
      idle_timeout_set_(false),
      tcp_no_delay_(true),
      keep_alive_(false),
      send_buffer_size_(0),
      receive_buffer_size_(0),
      read_buffer_size_(base::constants::DEFAULT_READ_BUFFER_SIZE) {
  if (port == 0) throw diagnostics::BuilderException("Invalid port number: 0");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

std::unique_ptr<wrapper::TcpServer> TcpServerBuilder::build() {
  std::unique_ptr<wrapper::TcpServer> server;
  if (independent_context_) {
    server = std::make_unique<wrapper::TcpServer>(port_, std::make_shared<boost::asio::io_context>());
    server->manage_external_context(true);
  } else {
    server = std::make_unique<wrapper::TcpServer>(port_);
  }
  if (shared_context_) server->shared_context(true);

  if (this->on_data_) server->on_data(this->on_data_);
  if (this->on_data_batch_) server->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) server->on_connect(this->on_connect_);
  if (this->on_disconnect_) server->on_disconnect(this->on_disconnect_);
  if (this->on_error_) server->on_error(this->on_error_);
  if (this->on_backpressure_) server->on_backpressure(this->on_backpressure_);

  if (client_limit_enabled_) {
    server->max_clients(max_clients_);
  }

  if (bind_address_set_) server->bind_address(bind_address_);
  if (port_retry_enabled_set_ || max_port_retries_set_ || port_retry_interval_set_) {
    server->port_retry(port_retry_enabled_, static_cast<int>(max_port_retries_),
                       static_cast<int>(port_retry_interval_ms_));
  }
  if (idle_timeout_set_) server->idle_timeout(idle_timeout_);
  if (tcp_no_delay_set_) server->tcp_no_delay(tcp_no_delay_);
  if (keep_alive_set_) server->keep_alive(keep_alive_);
  if (!tls_certificate_file_.empty() || !tls_private_key_file_.empty())
    server->tls(tls_certificate_file_, tls_private_key_file_);
  if (send_buffer_size_set_) server->send_buffer_size(send_buffer_size_);
  if (receive_buffer_size_set_) server->receive_buffer_size(receive_buffer_size_);
  if (read_buffer_size_set_) server->read_buffer_size(read_buffer_size_);

  if (this->bp_strategy_set_) server->backpressure_strategy(this->bp_strategy_);
  server->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    server->framer(this->framer_factory_);
  }
  if (this->on_message_) {
    server->on_message(this->on_message_);
  }
  if (this->on_message_batch_) {
    server->on_message_batch(this->on_message_batch_);
  }

  if (auto_start_) {
    server->auto_start(true);
  }

  return server;
}

TcpServerBuilder& TcpServerBuilder::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::bind_address(const std::string& address) {
  config::detail::require(util::InputValidator::is_valid_ipv4(address) || util::InputValidator::is_valid_ipv6(address),
                          "invalid bind address");
  bind_address_ = address;
  bind_address_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::shared_context(bool use_shared) {
  shared_context_ = use_shared;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::max_clients(uint32_t max_clients) {
  config::detail::range(max_clients, 0, base::constants::MAX_MAX_CONNECTIONS, "invalid client limit");
  max_clients_ = max_clients;
  client_limit_enabled_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::enable_port_retry(bool enable) {
  port_retry_enabled_ = enable;
  port_retry_enabled_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::max_port_retries(uint32_t max_retries) {
  max_port_retries_ = max_retries;
  max_port_retries_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::port_retry_interval(std::chrono::milliseconds interval) {
  port_retry_interval_ms_ = static_cast<uint32_t>(interval.count());
  port_retry_interval_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::tcp_no_delay(bool enable) {
  tcp_no_delay_ = enable;
  tcp_no_delay_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::keep_alive(bool enable) {
  keep_alive_ = enable;
  keep_alive_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::tls(const std::string& certificate_file, const std::string& private_key_file) {
  config::detail::require(certificate_file.empty() == private_key_file.empty(), "TLS requires certificate and key");
  tls_certificate_file_ = certificate_file;
  tls_private_key_file_ = private_key_file;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::send_buffer_size(size_t bytes) {
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid send_buffer_size");
  send_buffer_size_ = bytes;
  send_buffer_size_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::receive_buffer_size(size_t bytes) {
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid receive_buffer_size");
  receive_buffer_size_ = bytes;
  receive_buffer_size_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::read_buffer_size(size_t bytes) {
  config::detail::range(bytes, base::constants::MIN_READ_BUFFER_SIZE, base::constants::MAX_READ_BUFFER_SIZE,
                        "invalid read_buffer_size");
  read_buffer_size_ = bytes;
  read_buffer_size_set_ = true;
  return *this;
}

// Backward compatibility implementations
TcpServerBuilder& TcpServerBuilder::port_retry(bool enable, int max_retries, int retry_interval_ms) {
  config::detail::range(max_retries, 0, base::constants::MAX_RETRIES_LIMIT, "invalid port retry count");
  config::detail::range(retry_interval_ms, base::constants::MIN_RETRY_INTERVAL_MS,
                        base::constants::MAX_RETRY_INTERVAL_MS, "invalid port retry interval");
  port_retry_enabled_ = enable;
  port_retry_enabled_set_ = true;
  max_port_retries_ = static_cast<uint32_t>(max_retries);
  max_port_retries_set_ = true;
  port_retry_interval_ms_ = static_cast<uint32_t>(retry_interval_ms);
  port_retry_interval_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::idle_timeout(std::chrono::milliseconds timeout) {
  config::detail::duration(timeout, base::constants::MIN_IDLE_TIMEOUT_MS, base::constants::MAX_IDLE_TIMEOUT_MS, true,
                           "invalid idle_timeout");
  idle_timeout_ = timeout;
  idle_timeout_set_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::single_client() {
  max_clients_ = 1;
  client_limit_enabled_ = true;
  return *this;
}

TcpServerBuilder& TcpServerBuilder::multi_client(size_t max) {
  if (max == 0) {
    throw diagnostics::BuilderException("multi_client max must be greater than 0");
  }
  max_clients_ = static_cast<uint32_t>(max);
  client_limit_enabled_ = true;
  return *this;
}

// Explicit template instantiations

}  // namespace builder
}  // namespace wirestead
