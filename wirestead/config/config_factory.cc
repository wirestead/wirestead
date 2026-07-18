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

#include "config_factory.hpp"

#include <mutex>

#include "config_manager.hpp"

namespace wirestead {
namespace config {

std::shared_ptr<ConfigManagerInterface> ConfigFactory::singleton_instance_ = nullptr;
std::mutex ConfigFactory::singleton_mutex_;

std::shared_ptr<ConfigManagerInterface> ConfigFactory::create() { return std::make_shared<ConfigManager>(); }

std::shared_ptr<ConfigManagerInterface> ConfigFactory::create_with_defaults() {
  auto config = create();
  ConfigPresets::setup_all_defaults(config);
  return config;
}

std::shared_ptr<ConfigManagerInterface> ConfigFactory::create_from_file(const std::string& filepath) {
  auto config = create();
  if (!config->load_from_file(filepath)) {
    // If loading fails, fall back to defaults
    ConfigPresets::setup_all_defaults(config);
  }
  return config;
}

std::shared_ptr<ConfigManagerInterface> ConfigFactory::get_singleton() {
  std::lock_guard<std::mutex> lock(singleton_mutex_);
  if (!singleton_instance_) {
    singleton_instance_ = create_with_defaults();
  }
  return singleton_instance_;
}

void ConfigPresets::setup_tcp_client_defaults(std::shared_ptr<ConfigManagerInterface> config) {
  // TCP Client default configuration
  config->set("tcp.client.host", std::string("localhost"));
  config->set("tcp.client.port", 8080);
  config->set("tcp.client.retry_interval_ms", 1000);
  config->set("tcp.client.max_retries", 5);
  config->set("tcp.client.connection_timeout_ms", 5000);
  config->set("tcp.client.keep_alive", true);
  config->set("tcp.client.buffer_size", 4096);
}

void ConfigPresets::setup_tcp_server_defaults(std::shared_ptr<ConfigManagerInterface> config) {
  // TCP Server default configuration
  config->set("tcp.server.host", std::string("0.0.0.0"));
  config->set("tcp.server.port", 8080);
  config->set("tcp.server.max_connections", 100);
  config->set("tcp.server.connection_timeout_ms", 30000);
  config->set("tcp.server.keep_alive", true);
  config->set("tcp.server.buffer_size", 4096);
  config->set("tcp.server.backlog", 128);
}

void ConfigPresets::setup_serial_defaults(std::shared_ptr<ConfigManagerInterface> config) {
  // Serial communication default configuration
  config->set("serial.port", std::string("/dev/ttyUSB0"));
  config->set("serial.baud_rate", 9600);
  config->set("serial.data_bits", 8);
  config->set("serial.stop_bits", 1);
  config->set("serial.parity", std::string("none"));
  config->set("serial.flow_control", std::string("none"));
  config->set("serial.timeout_ms", 1000);
  config->set("serial.retry_interval_ms", 1000);
  config->set("serial.max_retries", 3);
}

void ConfigPresets::setup_logging_defaults(std::shared_ptr<ConfigManagerInterface> config) {
  // Logging default configuration
  config->set("logging.level", std::string("info"));
  config->set("logging.enable_console", true);
  config->set("logging.enable_file", false);
  config->set("logging.file_path", std::string("wirestead.log"));
  config->set("logging.max_file_size_mb", 10);
  config->set("logging.max_files", 5);
  config->set("logging.format", std::string("[%Y-%m-%d %H:%M:%S] [%l] %v"));
}

void ConfigPresets::setup_all_defaults(std::shared_ptr<ConfigManagerInterface> config) {
  setup_tcp_client_defaults(config);
  setup_tcp_server_defaults(config);
  setup_serial_defaults(config);
  setup_logging_defaults(config);

  // Global settings
  config->set("global.thread_pool_size", 4);
  config->set("global.enable_metrics", false);
  config->set("global.metrics_port", 9090);
  config->set("global.config_file", std::string("wirestead.conf"));
}

}  // namespace config
}  // namespace wirestead
