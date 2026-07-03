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

#include "unilink/builder/udp_builder.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>

#include "unilink/builder/auto_initializer.hpp"
#include "unilink/diagnostics/exceptions.hpp"

namespace unilink {
namespace builder {

// UdpClientBuilder implementation

template <uint32_t State>
UdpClientBuilder<State>::UdpClientBuilder() : UdpClientBuilder(0) {}

template <uint32_t State>
UdpClientBuilder<State>::UdpClientBuilder(uint16_t local_port)
    : local_port_(local_port),
      bind_address_("0.0.0.0"),
      remote_host_(""),
      remote_port_(0),
      auto_start_(false),
      independent_context_(false),
      enable_broadcast_(false),
      reuse_address_(false),
      send_buffer_size_(0),
      receive_buffer_size_(0) {
  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

template <uint32_t State>
std::unique_ptr<wrapper::UdpClient> UdpClientBuilder<State>::build() {
  std::unique_ptr<wrapper::UdpClient> client;
  config::UdpConfig cfg;
  cfg.bind_address = bind_address_;
  cfg.local_port = local_port_;
  cfg.remote_address = remote_host_;
  cfg.remote_port = remote_port_;
  cfg.enable_broadcast = enable_broadcast_;
  cfg.reuse_address = reuse_address_;
  cfg.send_buffer_size = send_buffer_size_;
  cfg.receive_buffer_size = receive_buffer_size_;

  if (independent_context_) {
    client = std::make_unique<wrapper::UdpClient>(cfg, std::make_shared<boost::asio::io_context>());
    client->manage_external_context(true);
  } else {
    client = std::make_unique<wrapper::UdpClient>(cfg);
  }

  if (this->on_data_) client->on_data(this->on_data_);
  if (this->on_data_batch_) client->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) client->on_connect(this->on_connect_);
  if (this->on_disconnect_) client->on_disconnect(this->on_disconnect_);
  if (this->on_error_) client->on_error(this->on_error_);
  if (this->on_backpressure_) client->on_backpressure(this->on_backpressure_);

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
UdpClientBuilder<State>& UdpClientBuilder<State>::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::local_port(uint16_t port) {
  local_port_ = port;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::bind_address(const std::string& address) {
  bind_address_ = address;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::remote_endpoint(const std::string& host, uint16_t port) {
  boost::system::error_code ec;
  boost::asio::ip::make_address(host, ec);
  if (ec) {
    throw diagnostics::BuilderException("Invalid remote address: " + host, "udp");
  }
  remote_host_ = host;
  remote_port_ = port;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::broadcast(bool enable) {
  enable_broadcast_ = enable;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::reuse_address(bool enable) {
  reuse_address_ = enable;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::send_buffer_size(size_t bytes) {
  send_buffer_size_ = bytes;
  return *this;
}

template <uint32_t State>
UdpClientBuilder<State>& UdpClientBuilder<State>::receive_buffer_size(size_t bytes) {
  receive_buffer_size_ = bytes;
  return *this;
}

// UdpServerBuilder implementation

template <uint32_t State>
UdpServerBuilder<State>::UdpServerBuilder() : UdpServerBuilder(0) {}

template <uint32_t State>
UdpServerBuilder<State>::UdpServerBuilder(uint16_t local_port)
    : local_port_(local_port),
      bind_address_("0.0.0.0"),
      auto_start_(false),
      independent_context_(false),
      enable_broadcast_(false),
      reuse_address_(false),
      send_buffer_size_(0),
      receive_buffer_size_(0) {
  // Ensure background IO service is running
  AutoInitializer::ensure_io_context_running();
}

template <uint32_t State>
std::unique_ptr<wrapper::UdpServer> UdpServerBuilder<State>::build() {
  std::unique_ptr<wrapper::UdpServer> server;
  config::UdpConfig cfg;
  cfg.bind_address = bind_address_;
  cfg.local_port = local_port_;
  cfg.enable_broadcast = enable_broadcast_;
  cfg.reuse_address = reuse_address_;
  cfg.send_buffer_size = send_buffer_size_;
  cfg.receive_buffer_size = receive_buffer_size_;

  if (independent_context_) {
    server = std::make_unique<wrapper::UdpServer>(cfg, std::make_shared<boost::asio::io_context>());
    server->manage_external_context(true);
  } else {
    server = std::make_unique<wrapper::UdpServer>(cfg);
  }

  if (this->on_data_) server->on_data(this->on_data_);
  if (this->on_data_batch_) server->on_data_batch(this->on_data_batch_);
  if (this->on_connect_) server->on_connect(this->on_connect_);
  if (this->on_disconnect_) server->on_disconnect(this->on_disconnect_);
  if (this->on_error_) server->on_error(this->on_error_);
  if (this->on_backpressure_) server->on_backpressure(this->on_backpressure_);

  if (this->bp_strategy_set_) server->backpressure_strategy(this->bp_strategy_);
  server->backpressure_threshold(this->get_effective_backpressure_threshold());

  if (this->framer_factory_) {
    server->framer(this->framer_factory_);
  }
  if (this->on_message_) {
    server->on_message(std::move(this->on_message_));
  }
  if (this->on_message_batch_) {
    server->on_message_batch(std::move(this->on_message_batch_));
  }

  if (client_limit_enabled_) {
    server->max_clients(max_clients_);
  }
  if (idle_timeout_set_) {
    server->idle_timeout(idle_timeout_);
  }

  if (auto_start_) {
    server->auto_start(true);
  }

  return server;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::auto_start(bool auto_start) {
  auto_start_ = auto_start;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::local_port(uint16_t port) {
  local_port_ = port;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::bind_address(const std::string& address) {
  bind_address_ = address;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::max_clients(uint32_t max) {
  max_clients_ = max;
  client_limit_enabled_ = true;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::broadcast(bool enable) {
  enable_broadcast_ = enable;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::reuse_address(bool enable) {
  reuse_address_ = enable;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::independent_context(bool use_independent) {
  independent_context_ = use_independent;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::idle_timeout(std::chrono::milliseconds timeout) {
  idle_timeout_ = timeout;
  idle_timeout_set_ = true;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::send_buffer_size(size_t bytes) {
  send_buffer_size_ = bytes;
  return *this;
}

template <uint32_t State>
UdpServerBuilder<State>& UdpServerBuilder<State>::receive_buffer_size(size_t bytes) {
  receive_buffer_size_ = bytes;
  return *this;
}

// Explicit template instantiations
template class UdpClientBuilder<BuilderState::None>;
template class UdpClientBuilder<BuilderState::HasData>;
template class UdpClientBuilder<BuilderState::HasError>;
template class UdpClientBuilder<BuilderState::Ready>;
template class UdpServerBuilder<BuilderState::None>;
template class UdpServerBuilder<BuilderState::HasData>;
template class UdpServerBuilder<BuilderState::HasError>;
template class UdpServerBuilder<BuilderState::Ready>;

}  // namespace builder
}  // namespace unilink
