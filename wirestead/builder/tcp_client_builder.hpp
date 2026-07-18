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
#include "wirestead/wrapper/tcp_client/tcp_client.hpp"

namespace wirestead {
namespace builder {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

/**
 * @brief Modernized Builder for TcpClient
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API TcpClientBuilder : public BuilderInterface<wrapper::TcpClient, TcpClientBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = TcpClientBuilder<NewState>;

  TcpClientBuilder(const std::string& host, uint16_t port);

  // Allow conversion between states
  template <uint32_t OtherState>
  TcpClientBuilder(TcpClientBuilder<OtherState>&& other) noexcept
      : host_(std::move(other.host_)),
        port_(other.port_),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        retry_interval_(other.retry_interval_),
        retry_interval_set_(other.retry_interval_set_),
        max_retries_(other.max_retries_),
        max_retries_set_(other.max_retries_set_),
        connection_timeout_(other.connection_timeout_),
        connection_timeout_set_(other.connection_timeout_set_),
        idle_timeout_(other.idle_timeout_),
        idle_timeout_set_(other.idle_timeout_set_),
        idle_timeout_action_(other.idle_timeout_action_),
        idle_timeout_action_set_(other.idle_timeout_action_set_),
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
  TcpClientBuilder(const TcpClientBuilder&) = delete;
  TcpClientBuilder& operator=(const TcpClientBuilder&) = delete;

  std::unique_ptr<wrapper::TcpClient> build() override;

  TcpClientBuilder<State>& auto_start(bool auto_start = true) override;

  TcpClientBuilder<State>& retry_interval(std::chrono::milliseconds interval);
  TcpClientBuilder<State>& max_retries(int max_retries);
  TcpClientBuilder<State>& connection_timeout(std::chrono::milliseconds timeout);
  /**
   * @brief Configure application-level idle timeout.
   *
   * A value of 0ms disables idle timeout. When enabled, inbound or outbound
   * activity resets the timer.
   */
  TcpClientBuilder<State>& idle_timeout(std::chrono::milliseconds timeout);
  /**
   * @brief Configure what happens when an enabled idle timeout expires.
   *
   * The default is IdleTimeoutAction::Reconnect. This setting has no effect
   * while idle_timeout is 0ms.
   */
  TcpClientBuilder<State>& idle_timeout_action(IdleTimeoutAction action);
  TcpClientBuilder<State>& independent_context(bool use_independent = true);
  TcpClientBuilder<State>& tcp_no_delay(bool enable = true);
  TcpClientBuilder<State>& keep_alive(bool enable = true);
  TcpClientBuilder<State>& send_buffer_size(size_t bytes);
  TcpClientBuilder<State>& receive_buffer_size(size_t bytes);

 private:
  template <uint32_t S>
  friend class TcpClientBuilder;

  std::string host_;
  uint16_t port_;
  bool auto_start_;
  bool independent_context_;

  std::chrono::milliseconds retry_interval_;
  bool retry_interval_set_{false};
  int max_retries_;
  bool max_retries_set_{false};
  std::chrono::milliseconds connection_timeout_;
  bool connection_timeout_set_{false};
  std::chrono::milliseconds idle_timeout_;
  bool idle_timeout_set_{false};
  IdleTimeoutAction idle_timeout_action_;
  bool idle_timeout_action_set_{false};
  bool tcp_no_delay_;
  bool tcp_no_delay_set_{false};
  bool keep_alive_;
  bool keep_alive_set_{false};
  size_t send_buffer_size_;
  bool send_buffer_size_set_{false};
  size_t receive_buffer_size_;
  bool receive_buffer_size_set_{false};
};

using TcpClientBuilderDefault = TcpClientBuilder<BuilderState::None>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
