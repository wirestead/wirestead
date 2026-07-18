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

#include "wirestead/base/visibility.hpp"
#include "wirestead/builder/ibuilder.hpp"
#include "wirestead/wrapper/tcp_server/tcp_server.hpp"

namespace wirestead {
namespace builder {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

/**
 * @brief Modernized Builder for TcpServer with C++20 Concepts
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API TcpServerBuilder : public BuilderInterface<wrapper::TcpServer, TcpServerBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = TcpServerBuilder<NewState>;

  explicit TcpServerBuilder(uint16_t port);

  // Allow conversion between states
  template <uint32_t OtherState>
  TcpServerBuilder(TcpServerBuilder<OtherState>&& other) noexcept
      : port_(other.port_),
        bind_address_(std::move(other.bind_address_)),
        bind_address_set_(other.bind_address_set_),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        shared_context_(other.shared_context_),
        max_clients_(other.max_clients_),
        client_limit_enabled_(other.client_limit_enabled_),
        port_retry_enabled_(other.port_retry_enabled_),
        port_retry_enabled_set_(other.port_retry_enabled_set_),
        max_port_retries_(other.max_port_retries_),
        max_port_retries_set_(other.max_port_retries_set_),
        port_retry_interval_ms_(other.port_retry_interval_ms_),
        port_retry_interval_set_(other.port_retry_interval_set_),
        idle_timeout_(other.idle_timeout_),
        idle_timeout_set_(other.idle_timeout_set_),
        tcp_no_delay_(other.tcp_no_delay_),
        tcp_no_delay_set_(other.tcp_no_delay_set_),
        keep_alive_(other.keep_alive_),
        keep_alive_set_(other.keep_alive_set_),
        send_buffer_size_(other.send_buffer_size_),
        send_buffer_size_set_(other.send_buffer_size_set_),
        receive_buffer_size_(other.receive_buffer_size_),
        receive_buffer_size_set_(other.receive_buffer_size_set_) {
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
  TcpServerBuilder(const TcpServerBuilder&) = delete;
  TcpServerBuilder& operator=(const TcpServerBuilder&) = delete;

  std::unique_ptr<wrapper::TcpServer> build() override;

  TcpServerBuilder<State>& auto_start(bool auto_start = true) override;
  TcpServerBuilder<State>& bind_address(const std::string& address);
  TcpServerBuilder<State>& independent_context(bool use_independent = true);
  // Opt into the shared IoContextManager singleton instead of the default
  // dedicated io_context + thread (#440). Only meaningful for deliberately
  // trading per-instance parallelism for reduced thread/memory overhead
  // across many servers in one process; most callers should not need this.
  TcpServerBuilder<State>& shared_context(bool use_shared = true);
  TcpServerBuilder<State>& max_clients(uint32_t max_clients);
  TcpServerBuilder<State>& enable_port_retry(bool enable = true);
  TcpServerBuilder<State>& max_port_retries(uint32_t max_retries);
  TcpServerBuilder<State>& port_retry_interval(std::chrono::milliseconds interval);
  TcpServerBuilder<State>& tcp_no_delay(bool enable = true);
  TcpServerBuilder<State>& keep_alive(bool enable = true);
  TcpServerBuilder<State>& send_buffer_size(size_t bytes);
  TcpServerBuilder<State>& receive_buffer_size(size_t bytes);

  // Backward compatibility methods
  TcpServerBuilder<State>& port_retry(bool enable = true, int max_retries = 3, int retry_interval_ms = 1000);
  /**
   * @brief Configure application-level idle timeout for accepted sessions.
   *
   * A value of 0ms disables idle timeout. When enabled, only the idle client
   * session is closed; the server keeps listening for new connections.
   */
  TcpServerBuilder<State>& idle_timeout(std::chrono::milliseconds timeout);
  [[deprecated("Use max_clients(1) instead")]]
  TcpServerBuilder<State>& single_client();
  [[deprecated("Use max_clients(max) instead")]]
  TcpServerBuilder<State>& multi_client(size_t max);

 private:
  template <uint32_t S>
  friend class TcpServerBuilder;

  uint16_t port_;
  std::string bind_address_;
  bool bind_address_set_{false};
  bool auto_start_;
  bool independent_context_;
  bool shared_context_{false};

  uint32_t max_clients_;
  bool client_limit_enabled_;

  bool port_retry_enabled_;
  bool port_retry_enabled_set_{false};
  uint32_t max_port_retries_;
  bool max_port_retries_set_{false};
  uint32_t port_retry_interval_ms_;
  bool port_retry_interval_set_{false};
  std::chrono::milliseconds idle_timeout_;
  bool idle_timeout_set_;
  bool tcp_no_delay_;
  bool tcp_no_delay_set_{false};
  bool keep_alive_;
  bool keep_alive_set_{false};
  size_t send_buffer_size_;
  bool send_buffer_size_set_{false};
  size_t receive_buffer_size_;
  bool receive_buffer_size_set_{false};
};

using TcpServerBuilderDefault = TcpServerBuilder<BuilderState::None>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
