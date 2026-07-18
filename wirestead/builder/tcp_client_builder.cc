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
#include "wirestead/diagnostics/exceptions.hpp"

namespace wirestead {
namespace builder {

template <uint32_t State>
TcpClientBuilder<State>::TcpClientBuilder(const std::string& host, uint16_t port)
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
      receive_buffer_size_(0) {
  if (port == 0) throw diagnostics::BuilderException("Invalid port number: 0");
  if (host.empty()) throw diagnostics::BuilderException("Host cannot be empty");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

template <uint32_t State>
std::unique_ptr<wrapper::TcpClient> TcpClientBuilder<State>::build() {
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
  if (connection_timeout_set_) client->connection_timeout(connection_timeout_);
  if (idle_timeout_set_) client->idle_timeout(idle_timeout_);
  if (idle_timeout_action_set_) client->idle_timeout_action(idle_timeout_action_);
  if (tcp_no_delay_set_) client->tcp_no_delay(tcp_no_delay_);
  if (keep_alive_set_) client->keep_alive(keep_alive_);
  if (send_buffer_size_set_) client->send_buffer_size(send_buffer_size_);
  if (receive_buffer_size_set_) client->receive_buffer_size(receive_buffer_size_);

  if (this->bp_strategy_set_) client->backpressure_strategy(this->bp_strategy_);
  client->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    client->framer(this->framer_factory_());
  }
  if (this->on_message_) {
    client->on_message(std::move(this->on_message_));
  }
  if (this->on_message_batch_) {
    client->on_message_batch(std::move(this->on_message_batch_));
  }

  if (auto_start_) {
    client->auto_start(true);
  }

  return client;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::retry_interval(std::chrono::milliseconds interval) {
  retry_interval_ = interval;
  retry_interval_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::max_retries(int max_retries) {
  max_retries_ = max_retries;
  max_retries_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::connection_timeout(std::chrono::milliseconds timeout) {
  connection_timeout_ = timeout;
  connection_timeout_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::idle_timeout(std::chrono::milliseconds timeout) {
  idle_timeout_ = timeout;
  idle_timeout_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::idle_timeout_action(IdleTimeoutAction action) {
  idle_timeout_action_ = action;
  idle_timeout_action_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::tcp_no_delay(bool enable) {
  tcp_no_delay_ = enable;
  tcp_no_delay_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::keep_alive(bool enable) {
  keep_alive_ = enable;
  keep_alive_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::send_buffer_size(size_t bytes) {
  send_buffer_size_ = bytes;
  send_buffer_size_set_ = true;
  return *this;
}

template <uint32_t State>
TcpClientBuilder<State>& TcpClientBuilder<State>::receive_buffer_size(size_t bytes) {
  receive_buffer_size_ = bytes;
  receive_buffer_size_set_ = true;
  return *this;
}

// Explicit template instantiations
template class TcpClientBuilder<BuilderState::None>;
template class TcpClientBuilder<BuilderState::HasData>;
template class TcpClientBuilder<BuilderState::HasError>;
template class TcpClientBuilder<BuilderState::Ready>;

}  // namespace builder
}  // namespace wirestead
