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

#include <memory>
#include <mutex>

#include "iconfig_manager.hpp"
#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace config {

/**
 * Factory for creating configuration managers
 */
class WIRESTEAD_API ConfigFactory {
 public:
  /**
   * Create a new configuration manager instance
   */
  static std::shared_ptr<ConfigManagerInterface> create();

  /**
   * Create a configuration manager with default settings
   */
  static std::shared_ptr<ConfigManagerInterface> create_with_defaults();

  /**
   * Create a configuration manager and load from file
   */
  static std::shared_ptr<ConfigManagerInterface> create_from_file(const std::string& filepath);

  /**
   * Create a singleton configuration manager
   */
  static std::shared_ptr<ConfigManagerInterface> get_singleton();

 private:
  static std::shared_ptr<ConfigManagerInterface> singleton_instance_;
  static std::mutex singleton_mutex_;
};

/**
 * Configuration presets for common use cases
 */
class WIRESTEAD_API ConfigPresets {
 public:
  /**
   * Setup default configuration for TCP client
   */
  static void setup_tcp_client_defaults(std::shared_ptr<ConfigManagerInterface> config);

  /**
   * Setup default configuration for TCP server
   */
  static void setup_tcp_server_defaults(std::shared_ptr<ConfigManagerInterface> config);

  /**
   * Setup default configuration for Serial communication
   */
  static void setup_serial_defaults(std::shared_ptr<ConfigManagerInterface> config);

  /**
   * Setup default configuration for logging
   */
  static void setup_logging_defaults(std::shared_ptr<ConfigManagerInterface> config);

  /**
   * Setup default configuration for all components
   */
  static void setup_all_defaults(std::shared_ptr<ConfigManagerInterface> config);
};

}  // namespace config
}  // namespace wirestead
