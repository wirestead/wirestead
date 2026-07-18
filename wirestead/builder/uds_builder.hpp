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
#include "wirestead/wrapper/uds_client/uds_client.hpp"
#include "wirestead/wrapper/uds_server/uds_server.hpp"

namespace wirestead {
namespace builder {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4251)
#endif

/**
 * @brief Modernized Builder for UdsClient
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API UdsClientBuilder : public BuilderInterface<wrapper::UdsClient, UdsClientBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = UdsClientBuilder<NewState>;

  explicit UdsClientBuilder(const std::string& socket_path);

  // Allow conversion between states
  template <uint32_t OtherState>
  UdsClientBuilder(UdsClientBuilder<OtherState>&& other) noexcept
      : socket_path_(std::move(other.socket_path_)),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        retry_interval_(other.retry_interval_),
        retry_interval_set_(other.retry_interval_set_),
        max_retries_(other.max_retries_),
        max_retries_set_(other.max_retries_set_),
        connection_timeout_(other.connection_timeout_),
        connection_timeout_set_(other.connection_timeout_set_) {
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
  UdsClientBuilder(const UdsClientBuilder&) = delete;
  UdsClientBuilder& operator=(const UdsClientBuilder&) = delete;

  std::unique_ptr<wrapper::UdsClient> build() override;

  UdsClientBuilder<State>& auto_start(bool auto_start = true) override;
  UdsClientBuilder<State>& retry_interval(std::chrono::milliseconds interval);
  UdsClientBuilder<State>& max_retries(int max_retries);
  UdsClientBuilder<State>& connection_timeout(std::chrono::milliseconds timeout);
  UdsClientBuilder<State>& independent_context(bool use_independent = true);

 private:
  template <uint32_t S>
  friend class UdsClientBuilder;

  std::string socket_path_;
  bool auto_start_;
  bool independent_context_;

  std::chrono::milliseconds retry_interval_;
  bool retry_interval_set_{false};
  int max_retries_;
  bool max_retries_set_{false};
  std::chrono::milliseconds connection_timeout_;
  bool connection_timeout_set_{false};
};

using UdsClientBuilderDefault = UdsClientBuilder<BuilderState::None>;

/**
 * @brief Modernized Builder for UdsServer
 */
template <uint32_t State = BuilderState::None>
class WIRESTEAD_API UdsServerBuilder : public BuilderInterface<wrapper::UdsServer, UdsServerBuilder<State>, State> {
 public:
  template <uint32_t NewState>
  using Rebind = UdsServerBuilder<NewState>;

  explicit UdsServerBuilder(const std::string& socket_path);

  // Allow conversion between states
  template <uint32_t OtherState>
  UdsServerBuilder(UdsServerBuilder<OtherState>&& other) noexcept
      : socket_path_(std::move(other.socket_path_)),
        auto_start_(other.auto_start_),
        independent_context_(other.independent_context_),
        max_clients_(other.max_clients_),
        client_limit_enabled_(other.client_limit_enabled_),
        idle_timeout_(other.idle_timeout_),
        idle_timeout_set_(other.idle_timeout_set_) {
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
  UdsServerBuilder(const UdsServerBuilder&) = delete;
  UdsServerBuilder& operator=(const UdsServerBuilder&) = delete;

  std::unique_ptr<wrapper::UdsServer> build() override;

  UdsServerBuilder<State>& auto_start(bool auto_start = true) override;
  UdsServerBuilder<State>& independent_context(bool use_independent = true);
  UdsServerBuilder<State>& idle_timeout(std::chrono::milliseconds timeout);
  UdsServerBuilder<State>& max_clients(uint32_t max_clients);
  [[deprecated("Use max_clients(1) instead")]]
  UdsServerBuilder<State>& single_client();
  [[deprecated("Use max_clients(max) instead")]]
  UdsServerBuilder<State>& multi_client(size_t max);

 private:
  template <uint32_t S>
  friend class UdsServerBuilder;

  std::string socket_path_;
  bool auto_start_;
  bool independent_context_;

  uint32_t max_clients_;
  bool client_limit_enabled_;
  std::chrono::milliseconds idle_timeout_;
  bool idle_timeout_set_;
};

using UdsServerBuilderDefault = UdsServerBuilder<BuilderState::None>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}  // namespace builder
}  // namespace wirestead
