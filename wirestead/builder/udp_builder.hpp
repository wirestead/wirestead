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

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/builder/ibuilder.hpp"
#include "wirestead/wrapper/udp/udp.hpp"
#include "wirestead/wrapper/udp/udp_server.hpp"

namespace wirestead {
namespace builder {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

/**
 * @brief Modernized Builder for UdpClient
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API UdpClientBuilder : public BuilderInterface<wrapper::UdpClient, UdpClientBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = UdpClientBuilder<NewState>;

  UdpClientBuilder();
  explicit UdpClientBuilder(uint16_t local_port);

  // Allow conversion between states
  template <uint32_t OtherState>
  UdpClientBuilder(UdpClientBuilder<OtherState>&& other) noexcept
      : local_port_(other.local_port_),
        bind_address_(std::move(other.bind_address_)),
        remote_host_(std::move(other.remote_host_)),
        remote_port_(other.remote_port_),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        enable_broadcast_(other.enable_broadcast_),
        reuse_address_(other.reuse_address_),
        send_buffer_size_(other.send_buffer_size_),
        receive_buffer_size_(other.receive_buffer_size_) {
    this->on_data_ = std::move(other.on_data_);
    this->on_error_ = std::move(other.on_error_);
    this->on_connect_ = std::move(other.on_connect_);
    this->on_disconnect_ = std::move(other.on_disconnect_);
    this->on_message_ = std::move(other.on_message_);
    this->on_data_batch_ = std::move(other.on_data_batch_);
    this->on_message_batch_ = std::move(other.on_message_batch_);
    this->on_backpressure_ = std::move(other.on_backpressure_);
    this->framer_factory_ = std::move(other.framer_factory_);
    this->bp_strategy_ = other.bp_strategy_;
    this->bp_threshold_ = other.bp_threshold_;
    this->bp_strategy_set_ = other.bp_strategy_set_;
    this->bp_threshold_set_ = other.bp_threshold_set_;
  }

  // Delete copy
  UdpClientBuilder(const UdpClientBuilder&) = delete;
  UdpClientBuilder& operator=(const UdpClientBuilder&) = delete;

  std::unique_ptr<wrapper::UdpClient> build() override;

  UdpClientBuilder<State>& auto_start(bool auto_start = true) override;
  UdpClientBuilder<State>& local_port(uint16_t port);
  UdpClientBuilder<State>& bind_address(const std::string& address);
  [[deprecated("Use bind_address instead")]]
  UdpClientBuilder<State>& local_address(const std::string& address) {
    return bind_address(address);
  }
  UdpClientBuilder<State>& remote_endpoint(const std::string& host, uint16_t port);
  UdpClientBuilder<State>& remote(const std::string& host, uint16_t port) { return remote_endpoint(host, port); }
  UdpClientBuilder<State>& broadcast(bool enable = true);
  UdpClientBuilder<State>& reuse_address(bool enable = true);
  UdpClientBuilder<State>& independent_context(bool use_independent = true);
  UdpClientBuilder<State>& send_buffer_size(size_t bytes);
  UdpClientBuilder<State>& receive_buffer_size(size_t bytes);

 private:
  template <uint32_t S>
  friend class UdpClientBuilder;

  uint16_t local_port_;
  std::string bind_address_;
  std::string remote_host_;
  uint16_t remote_port_;
  bool auto_start_;
  bool independent_context_;
  bool enable_broadcast_;
  bool reuse_address_;
  size_t send_buffer_size_;
  size_t receive_buffer_size_;
};

using UdpClientBuilderDefault = UdpClientBuilder<BuilderState::None>;

/**
 * @brief Modernized Builder for UdpServer
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API UdpServerBuilder : public BuilderInterface<wrapper::UdpServer, UdpServerBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = UdpServerBuilder<NewState>;

  UdpServerBuilder();
  explicit UdpServerBuilder(uint16_t local_port);

  // Allow conversion between states
  template <uint32_t OtherState>
  UdpServerBuilder(UdpServerBuilder<OtherState>&& other) noexcept
      : local_port_(other.local_port_),
        bind_address_(std::move(other.bind_address_)),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        enable_broadcast_(other.enable_broadcast_),
        reuse_address_(other.reuse_address_),
        max_clients_(other.max_clients_),
        client_limit_enabled_(other.client_limit_enabled_),
        idle_timeout_(other.idle_timeout_),
        idle_timeout_set_(other.idle_timeout_set_),
        send_buffer_size_(other.send_buffer_size_),
        receive_buffer_size_(other.receive_buffer_size_) {
    this->on_data_ = std::move(other.on_data_);
    this->on_error_ = std::move(other.on_error_);
    this->on_connect_ = std::move(other.on_connect_);
    this->on_disconnect_ = std::move(other.on_disconnect_);
    this->on_message_ = std::move(other.on_message_);
    this->on_data_batch_ = std::move(other.on_data_batch_);
    this->on_message_batch_ = std::move(other.on_message_batch_);
    this->on_backpressure_ = std::move(other.on_backpressure_);
    this->framer_factory_ = std::move(other.framer_factory_);
    this->bp_strategy_ = other.bp_strategy_;
    this->bp_threshold_ = other.bp_threshold_;
    this->bp_strategy_set_ = other.bp_strategy_set_;
    this->bp_threshold_set_ = other.bp_threshold_set_;
  }

  // Delete copy
  UdpServerBuilder(const UdpServerBuilder&) = delete;
  UdpServerBuilder& operator=(const UdpServerBuilder&) = delete;

  std::unique_ptr<wrapper::UdpServer> build() override;

  UdpServerBuilder<State>& auto_start(bool auto_start = true) override;
  UdpServerBuilder<State>& local_port(uint16_t port);
  UdpServerBuilder<State>& bind_address(const std::string& address);
  [[deprecated("Use bind_address instead")]]
  UdpServerBuilder<State>& local_address(const std::string& address) {
    return bind_address(address);
  }
  UdpServerBuilder<State>& max_clients(uint32_t max);
  UdpServerBuilder<State>& broadcast(bool enable = true);
  UdpServerBuilder<State>& reuse_address(bool enable = true);
  UdpServerBuilder<State>& independent_context(bool use_independent = true);
  /**
   * @brief Configure application-level idle timeout for virtual sessions.
   *
   * A value of 0ms disables idle timeout. When enabled, stale UDP virtual
   * sessions are removed and a later datagram from the same endpoint creates a
   * new virtual session.
   */
  UdpServerBuilder<State>& idle_timeout(std::chrono::milliseconds timeout);
  UdpServerBuilder<State>& send_buffer_size(size_t bytes);
  UdpServerBuilder<State>& receive_buffer_size(size_t bytes);

 private:
  template <uint32_t S>
  friend class UdpServerBuilder;

  uint16_t local_port_;
  std::string bind_address_;
  bool auto_start_;
  bool independent_context_;
  bool enable_broadcast_;
  bool reuse_address_;
  uint32_t max_clients_ = 0;
  bool client_limit_enabled_ = false;
  std::chrono::milliseconds idle_timeout_{0};
  bool idle_timeout_set_ = false;
  size_t send_buffer_size_;
  size_t receive_buffer_size_;
};

using UdpServerBuilderDefault = UdpServerBuilder<BuilderState::None>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
