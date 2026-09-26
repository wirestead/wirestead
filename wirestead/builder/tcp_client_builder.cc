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

#include "wirestead/builder/tcp_client_builder.hpp"

#include <boost/asio/io_context.hpp>

#include "wirestead/base/constants.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/config/validation.hpp"
#include "wirestead/diagnostics/exceptions.hpp"
#include "wirestead/util/input_validator.hpp"

namespace wirestead {
namespace builder {

TcpClientBuilder::TcpClientBuilder(const std::string& host, uint16_t port)
    : host_(host),
      port_(port),
      auto_start_(false),
      independent_context_(false),
      retry_interval_(base::constants::DEFAULT_RETRY_INTERVAL_MS),
      max_retries_(base::constants::DEFAULT_MAX_RETRIES),
      connection_timeout_(base::constants::DEFAULT_CONNECTION_TIMEOUT_MS),
      idle_timeout_(0),
      idle_timeout_action_(IdleTimeoutAction::Reconnect),
      tcp_no_delay_(true),
      keep_alive_(false),
      send_buffer_size_(0),
      receive_buffer_size_(0),
      read_buffer_size_(base::constants::DEFAULT_READ_BUFFER_SIZE) {
  if (port == 0) throw diagnostics::BuilderException("Invalid port number: 0");
  if (host.empty()) throw diagnostics::BuilderException("Host cannot be empty");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

std::unique_ptr<wrapper::TcpClient> TcpClientBuilder::build() {
  std::unique_ptr<wrapper::TcpClient> client;
  if (independent_context_) {
    client = std::make_unique<wrapper::TcpClient>(host_, port_, std::make_shared<boost::asio::io_context>());
    client->manage_external_context(true);
  } else {
    client = std::make_unique<wrapper::TcpClient>(host_, port_);
  }

  if (this->on_data_) client->on_data(this->on_data_);
  if (this->on_data_batch_) client->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) client->on_connect(this->on_connect_);
  if (this->on_disconnect_) client->on_disconnect(this->on_disconnect_);
  if (this->on_error_) client->on_error(this->on_error_);
  if (this->on_backpressure_) client->on_backpressure(this->on_backpressure_);

  if (retry_interval_set_) client->retry_interval(retry_interval_);
  if (max_retries_set_) client->max_retries(max_retries_);
  if (tls_enabled_) client->tls(tls_ca_file_);
  if (connection_timeout_set_) client->connection_timeout(connection_timeout_);
  if (idle_timeout_set_) client->idle_timeout(idle_timeout_);
  if (idle_timeout_action_set_) client->idle_timeout_action(idle_timeout_action_);
  if (tcp_no_delay_set_) client->tcp_no_delay(tcp_no_delay_);
  if (keep_alive_set_) client->keep_alive(keep_alive_);
  if (send_buffer_size_set_) client->send_buffer_size(send_buffer_size_);
  if (receive_buffer_size_set_) client->receive_buffer_size(receive_buffer_size_);
  if (read_buffer_size_set_) client->read_buffer_size(read_buffer_size_);

  if (this->bp_strategy_set_) client->backpressure_strategy(this->bp_strategy_);
  client->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    client->framer(this->framer_factory_());
  }
  if (this->on_message_) {
    client->on_message(this->on_message_);
  }
  if (this->on_message_batch_) {
    client->on_message_batch(this->on_message_batch_);
  }

  if (auto_start_) {
    client->auto_start(true);
  }

  return client;
}

TcpClientBuilder& TcpClientBuilder::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::retry_interval(std::chrono::milliseconds interval) {
  config::detail::duration(interval, base::constants::MIN_RETRY_INTERVAL_MS, base::constants::MAX_RETRY_INTERVAL_MS,
                           false, "invalid retry_interval");
  retry_interval_ = interval;
  retry_interval_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::max_retries(int max_retries) {
  config::detail::range(max_retries, -1, base::constants::MAX_RETRIES_LIMIT, "invalid retry limit");
  max_retries_ = max_retries;
  max_retries_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::tls(const std::string& ca_file) {
  tls_enabled_ = true;
  tls_ca_file_ = ca_file;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::connection_timeout(std::chrono::milliseconds timeout) {
  config::detail::duration(timeout, base::constants::MIN_CONNECTION_TIMEOUT_MS,
                           base::constants::MAX_CONNECTION_TIMEOUT_MS, false, "invalid connection_timeout");
  connection_timeout_ = timeout;
  connection_timeout_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::idle_timeout(std::chrono::milliseconds timeout) {
  config::detail::duration(timeout, base::constants::MIN_IDLE_TIMEOUT_MS, base::constants::MAX_IDLE_TIMEOUT_MS, true,
                           "invalid idle_timeout");
  idle_timeout_ = timeout;
  idle_timeout_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::idle_timeout_action(IdleTimeoutAction action) {
  config::detail::require(action == IdleTimeoutAction::Close || action == IdleTimeoutAction::Reconnect,
                          "invalid idle timeout action");
  idle_timeout_action_ = action;
  idle_timeout_action_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::tcp_no_delay(bool enable) {
  tcp_no_delay_ = enable;
  tcp_no_delay_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::keep_alive(bool enable) {
  keep_alive_ = enable;
  keep_alive_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::send_buffer_size(size_t bytes) {
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid send_buffer_size");
  send_buffer_size_ = bytes;
  send_buffer_size_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::receive_buffer_size(size_t bytes) {
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid receive_buffer_size");
  receive_buffer_size_ = bytes;
  receive_buffer_size_set_ = true;
  return *this;
}

TcpClientBuilder& TcpClientBuilder::read_buffer_size(size_t bytes) {
  config::detail::range(bytes, base::constants::MIN_READ_BUFFER_SIZE, base::constants::MAX_READ_BUFFER_SIZE,
                        "invalid read_buffer_size");
  read_buffer_size_ = bytes;
  read_buffer_size_set_ = true;
  return *this;
}

// Explicit template instantiations

}  // namespace builder
}  // namespace wirestead
