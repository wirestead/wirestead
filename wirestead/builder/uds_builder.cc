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

#include "wirestead/builder/uds_builder.hpp"

#include <boost/asio/io_context.hpp>

#include "wirestead/base/constants.hpp"
#include "wirestead/builder/auto_initializer.hpp"
#include "wirestead/diagnostics/exceptions.hpp"

namespace wirestead {
namespace builder {

// UdsClientBuilder implementation

template <uint32_t State>
UdsClientBuilder<State>::UdsClientBuilder(const std::string& socket_path)
    : socket_path_(socket_path),
      auto_start_(false),
      independent_context_(false),
      retry_interval_(base::constants::DEFAULT_RETRY_INTERVAL_MS),
      max_retries_(base::constants::DEFAULT_MAX_RETRIES),
      connection_timeout_(base::constants::DEFAULT_CONNECTION_TIMEOUT_MS) {
  if (socket_path.empty()) throw diagnostics::BuilderException("Socket path cannot be empty");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

template <uint32_t State>
std::unique_ptr<wrapper::UdsClient> UdsClientBuilder<State>::build() {
  std::unique_ptr<wrapper::UdsClient> client;
  if (independent_context_) {
    client = std::make_unique<wrapper::UdsClient>(socket_path_, std::make_shared<boost::asio::io_context>());
    client->manage_external_context(true);
  } else {
    client = std::make_unique<wrapper::UdsClient>(socket_path_);
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
UdsClientBuilder<State>& UdsClientBuilder<State>::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

template <uint32_t State>
UdsClientBuilder<State>& UdsClientBuilder<State>::retry_interval(std::chrono::milliseconds interval) {
  retry_interval_ = interval;
  retry_interval_set_ = true;
  return *this;
}

template <uint32_t State>
UdsClientBuilder<State>& UdsClientBuilder<State>::max_retries(int max_retries) {
  max_retries_ = max_retries;
  max_retries_set_ = true;
  return *this;
}

template <uint32_t State>
UdsClientBuilder<State>& UdsClientBuilder<State>::connection_timeout(std::chrono::milliseconds timeout) {
  connection_timeout_ = timeout;
  connection_timeout_set_ = true;
  return *this;
}

template <uint32_t State>
UdsClientBuilder<State>& UdsClientBuilder<State>::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

// UdsServerBuilder implementation

template <uint32_t State>
UdsServerBuilder<State>::UdsServerBuilder(const std::string& socket_path)
    : socket_path_(socket_path),
      auto_start_(false),
      independent_context_(false),
      max_clients_(0),
      client_limit_enabled_(false),
      idle_timeout_(0),
      idle_timeout_set_(false) {
  if (socket_path.empty()) throw diagnostics::BuilderException("Socket path cannot be empty");

  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

template <uint32_t State>
std::unique_ptr<wrapper::UdsServer> UdsServerBuilder<State>::build() {
  std::unique_ptr<wrapper::UdsServer> server;
  if (independent_context_) {
    server = std::make_unique<wrapper::UdsServer>(socket_path_, std::make_shared<boost::asio::io_context>());
    server->manage_external_context(true);
  } else {
    server = std::make_unique<wrapper::UdsServer>(socket_path_);
  }

  if (this->on_data_) server->on_data(this->on_data_);
  if (this->on_data_batch_) server->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) server->on_connect(this->on_connect_);
  if (this->on_disconnect_) server->on_disconnect(this->on_disconnect_);
  if (this->on_error_) server->on_error(this->on_error_);
  if (this->on_backpressure_) server->on_backpressure(this->on_backpressure_);

  if (client_limit_enabled_) {
    server->max_clients(max_clients_);
  }
  if (idle_timeout_set_) {
    server->idle_timeout(idle_timeout_);
  }

  if (this->bp_strategy_set_) server->backpressure_strategy(this->bp_strategy_);
  server->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    // Corrected: ServerInterface::framer expects FramerFactory std::function
    server->framer(this->framer_factory_);
  }
  if (this->on_message_) {
    server->on_message(std::move(this->on_message_));
  }
  if (this->on_message_batch_) {
    server->on_message_batch(std::move(this->on_message_batch_));
  }

  if (auto_start_) {
    server->auto_start(true);
  }

  return server;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::idle_timeout(std::chrono::milliseconds timeout) {
  idle_timeout_ = timeout;
  idle_timeout_set_ = true;
  return *this;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::max_clients(uint32_t max_clients) {
  max_clients_ = max_clients;
  client_limit_enabled_ = true;
  return *this;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::single_client() {
  max_clients_ = 1;
  client_limit_enabled_ = true;
  return *this;
}

template <uint32_t State>
UdsServerBuilder<State>& UdsServerBuilder<State>::multi_client(size_t max) {
  if (max == 0) {
    throw diagnostics::BuilderException("multi_client max must be greater than 0");
  }
  max_clients_ = static_cast<uint32_t>(max);
  client_limit_enabled_ = true;
  return *this;
}

// Explicit template instantiations
template class UdsClientBuilder<BuilderState::None>;
template class UdsClientBuilder<BuilderState::HasData>;
template class UdsClientBuilder<BuilderState::HasError>;
template class UdsClientBuilder<BuilderState::Ready>;
template class UdsServerBuilder<BuilderState::None>;
template class UdsServerBuilder<BuilderState::HasData>;
template class UdsServerBuilder<BuilderState::HasError>;
template class UdsServerBuilder<BuilderState::Ready>;

}  // namespace builder
}  // namespace wirestead
