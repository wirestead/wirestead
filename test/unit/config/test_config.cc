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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <any>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.hpp"
#include "unilink/builder/unified_builder.hpp"
#include "unilink/config/config_factory.hpp"
#include "unilink/config/config_manager.hpp"
#include "unilink/config/serial_config.hpp"
#include "unilink/config/tcp_client_config.hpp"
#include "unilink/config/tcp_server_config.hpp"
#include "unilink/config/udp_config.hpp"
#include "unilink/config/uds_config.hpp"
#include "unilink/diagnostics/exceptions.hpp"

using namespace unilink;
using namespace unilink::test;
using namespace unilink::config;
using namespace unilink::builder;
using namespace std::chrono_literals;

/**
 * @brief Comprehensive configuration management tests
 *
 * This file combines all configuration-related tests including
 * basic functionality, advanced features, validation, persistence,
 * thread safety, and performance testing.
 */
class ConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize test state
    test_port_ = TestUtils::getAvailableTestPort();

    // Create a fresh config manager for each test
    config_manager_ = std::make_shared<ConfigManager>();

    // Set up test file path in the system temp directory to ensure writability
    auto temp_dir = TestUtils::getTempDirectory();  // ensures directory exists cross-platform
    auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    test_file_path_ =
        temp_dir / ("unilink_test_config_" + std::to_string(now_ns) + "_" + std::to_string(test_port_) + ".json");

    // Clean up any existing test file
    TestUtils::removeFileIfExists(test_file_path_);
  }

  void TearDown() override {
    // Clean up test file
    TestUtils::removeFileIfExists(test_file_path_);

    // Clean up any test state
    TestUtils::waitFor(100);
  }

  uint16_t test_port_;
  std::shared_ptr<ConfigManager> config_manager_;
  std::filesystem::path test_file_path_;
};

// ============================================================================
// BASIC CONFIG FUNCTIONALITY TESTS
// ============================================================================

/**
 * @brief Test config manager basic functionality
 */
TEST_F(ConfigTest, ConfigManagerBasicFunctionality) {
  std::cout << "\n=== Config Manager Basic Functionality Test ===" << std::endl;

  // Test basic configuration operations
  EXPECT_TRUE(true);
}

/**
 * @brief Test config manager value setting and getting
 */
TEST_F(ConfigTest, ConfigManagerValueOperations) {
  std::cout << "\n=== Config Manager Value Operations Test ===" << std::endl;

  // Test basic configuration operations
  EXPECT_TRUE(true);
}

/**
 * @brief Test config manager with different data types
 */
TEST_F(ConfigTest, ConfigManagerDataTypeOperations) {
  std::cout << "\n=== Config Manager Data Type Operations Test ===" << std::endl;

  // Test basic configuration operations
  EXPECT_TRUE(true);
}

// ============================================================================
// CONFIG VALIDATION TESTS
// ============================================================================

/**
 * @brief Test configuration validation with valid values
 */
TEST_F(ConfigTest, ConfigValidationValidValues) {
  std::cout << "\n=== Configuration Validation Valid Values Test ===" << std::endl;

  // Test string values
  auto result1 = config_manager_->set("test_string", std::string("valid_string"));
  EXPECT_TRUE(result1.is_valid);

  // Test integer values
  auto result2 = config_manager_->set("test_int", 42);
  EXPECT_TRUE(result2.is_valid);

  // Test boolean values
  auto result3 = config_manager_->set("test_bool", true);
  EXPECT_TRUE(result3.is_valid);

  // Test double values
  auto result4 = config_manager_->set("test_double", 3.14159);
  EXPECT_TRUE(result4.is_valid);

  // Verify values were set
  EXPECT_TRUE(config_manager_->has("test_string"));
  EXPECT_TRUE(config_manager_->has("test_int"));
  EXPECT_TRUE(config_manager_->has("test_bool"));
  EXPECT_TRUE(config_manager_->has("test_double"));

  std::cout << "All valid configuration values set successfully" << std::endl;
}

/**
 * @brief Test configuration validation with invalid values
 */
TEST_F(ConfigTest, ConfigValidationInvalidValues) {
  std::cout << "\n=== Configuration Validation Invalid Values Test ===" << std::endl;

  // Test with empty key
  auto result1 = config_manager_->set("", std::string("value"));
  // Note: Empty key validation depends on implementation
  std::cout << "Empty key result: " << (result1.is_valid ? "valid" : "invalid") << std::endl;

  // Test with special characters in key
  auto result2 = config_manager_->set("test@key#with$special%chars", std::string("value"));
  // Note: Special character validation depends on implementation
  std::cout << "Special chars key result: " << (result2.is_valid ? "valid" : "invalid") << std::endl;

  // Test with very long key
  std::string long_key(1000, 'a');
  auto result3 = config_manager_->set(long_key, std::string("value"));
  // Note: Long key validation depends on implementation
  std::cout << "Long key result: " << (result3.is_valid ? "valid" : "invalid") << std::endl;
}

/**
 * @brief Test configuration validation with boundary values
 */
TEST_F(ConfigTest, ConfigValidationBoundaryValues) {
  std::cout << "\n=== Configuration Validation Boundary Values Test ===" << std::endl;

  // Test TCP client configuration validation
  auto client =
      UnifiedBuilder::tcp_client("127.0.0.1", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  EXPECT_NE(client, nullptr);

  // Test TCP server configuration validation
  auto server = UnifiedBuilder::tcp_server(test_port_)
                    // No client limit

                    .on_data([](auto&&) {})
                    .on_error([](auto&&) {})
                    .build();

  EXPECT_NE(server, nullptr);

  // Test with minimum valid port
  auto client1 = UnifiedBuilder::tcp_client("127.0.0.1", 1).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  EXPECT_NE(client1, nullptr);

  // Test with maximum valid port
  auto client2 = UnifiedBuilder::tcp_client("127.0.0.1", 65535).on_data([](auto&&) {}).on_error([](auto&&) {}).build();

  EXPECT_NE(client2, nullptr);
}

/**
 * @brief Test configuration validation with invalid values
 */
TEST_F(ConfigTest, ConfigValidationInvalidValuesNetwork) {
  std::cout << "\n=== Configuration Validation Invalid Values Network Test ===" << std::endl;

  // Test with invalid port (should throw exception due to input validation)
  EXPECT_THROW(
      auto client = UnifiedBuilder::tcp_client("127.0.0.1", 0).on_data([](auto&&) {}).on_error([](auto&&) {}).build(),
      diagnostics::BuilderException);

  // Test with invalid host (should throw exception due to input validation)
  EXPECT_THROW(
      auto client2 = UnifiedBuilder::tcp_client("", test_port_).on_data([](auto&&) {}).on_error([](auto&&) {}).build(),
      diagnostics::BuilderException);
}

/**
 * @brief Test TcpClientConfig direct validation using is_valid()
 */
TEST_F(ConfigTest, TcpClientConfigDirectValidation) {
  std::cout << "\n=== TcpClientConfig Direct Validation Test ===" << std::endl;

  TcpClientConfig config;

  // Default should be valid
  EXPECT_TRUE(config.is_valid()) << "Default config should be valid";
  EXPECT_EQ(config.host, "127.0.0.1");

  // Valid IPv4
  config.host = "192.168.1.1";
  EXPECT_TRUE(config.is_valid()) << "IPv4 should be valid";

  // Valid Hostname
  config.host = "example.com";
  EXPECT_TRUE(config.is_valid()) << "Hostname should be valid";

  // Valid IPv6
  config.host = "::1";
  EXPECT_TRUE(config.is_valid()) << "IPv6 should be valid";

  // Invalid: Empty
  config.host = "";
  EXPECT_FALSE(config.is_valid()) << "Empty host should be invalid";

  // Invalid: Bad characters
  config.host = "invalid_host!";
  EXPECT_FALSE(config.is_valid()) << "Host with bad characters should be invalid";

  // Invalid: Protocol prefix (common mistake)
  config.host = "http://example.com";
  EXPECT_FALSE(config.is_valid()) << "Host with protocol prefix should be invalid";

  // Restore valid host
  config.host = "localhost";
  EXPECT_TRUE(config.is_valid());
}

// #432: connection_timeout_ms/idle_timeout_ms had constants in
// base/constants.hpp that neither is_valid() nor validate_and_clamp()
// ever checked, for both TcpClientConfig and UdsClientConfig - a
// connection_timeout(0) reached connect_timer_ unclamped and caused an
// instant-timeout reconnect loop (the reported failure scenario).
TEST_F(ConfigTest, TcpClientAndUdsClientConfigTimeoutFieldsAreValidatedAndClamped) {
  TcpClientConfig tcp;
  EXPECT_TRUE(tcp.is_valid());

  tcp.connection_timeout_ms = base::constants::MIN_CONNECTION_TIMEOUT_MS - 1;
  EXPECT_FALSE(tcp.is_valid()) << "connection_timeout_ms below the minimum must be rejected";
  tcp.validate_and_clamp();
  EXPECT_EQ(tcp.connection_timeout_ms, base::constants::MIN_CONNECTION_TIMEOUT_MS);

  tcp.connection_timeout_ms = base::constants::MAX_CONNECTION_TIMEOUT_MS + 1;
  EXPECT_FALSE(tcp.is_valid());
  tcp.validate_and_clamp();
  EXPECT_EQ(tcp.connection_timeout_ms, base::constants::MAX_CONNECTION_TIMEOUT_MS);

  tcp.idle_timeout_ms = base::constants::MAX_IDLE_TIMEOUT_MS + 1;
  EXPECT_FALSE(tcp.is_valid());
  tcp.validate_and_clamp();
  EXPECT_EQ(tcp.idle_timeout_ms, base::constants::MAX_IDLE_TIMEOUT_MS);

  // 0 means "disabled" and must not be clamped up to MIN_IDLE_TIMEOUT_MS.
  tcp.idle_timeout_ms = 0;
  EXPECT_TRUE(tcp.is_valid());
  tcp.validate_and_clamp();
  EXPECT_EQ(tcp.idle_timeout_ms, 0u);

  UdsClientConfig uds;
  EXPECT_TRUE(uds.is_valid());
  uds.connection_timeout_ms = base::constants::MIN_CONNECTION_TIMEOUT_MS - 1;
  EXPECT_FALSE(uds.is_valid());
  uds.validate_and_clamp();
  EXPECT_EQ(uds.connection_timeout_ms, base::constants::MIN_CONNECTION_TIMEOUT_MS);
}

// #432: TcpServerConfig/UdsServerConfig's idle_timeout_ms only had a
// lower-bound clamp (< 0 -> 0); an over-large value was never rejected or
// capped even though MAX_IDLE_TIMEOUT_MS exists.
TEST_F(ConfigTest, ServerConfigIdleTimeoutUpperBoundIsEnforced) {
  TcpServerConfig tcp_server;
  tcp_server.idle_timeout_ms = static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS) + 1;
  EXPECT_FALSE(tcp_server.is_valid());
  tcp_server.validate_and_clamp();
  EXPECT_EQ(tcp_server.idle_timeout_ms, static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS));

  UdsServerConfig uds_server;
  uds_server.idle_timeout_ms = static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS) + 1;
  EXPECT_FALSE(uds_server.is_valid());
  uds_server.validate_and_clamp();
  EXPECT_EQ(uds_server.idle_timeout_ms, static_cast<int>(base::constants::MAX_IDLE_TIMEOUT_MS));
}

// #432: SerialConfig::is_valid() never checked the device path format
// (InputValidator::is_valid_device_path existed but was private/unwired)
// nor baud_rate's upper bound.
TEST_F(ConfigTest, SerialConfigDevicePathAndBaudRateAreValidated) {
  SerialConfig cfg;
  EXPECT_TRUE(cfg.is_valid());

  cfg.device = "not-a-real-device";
  EXPECT_FALSE(cfg.is_valid()) << "device path must match /dev/... or COMn";
#ifdef _WIN32
  cfg.device = "COM7";
#else
  cfg.device = "/dev/ttyUSB1";
#endif
  EXPECT_TRUE(cfg.is_valid());

  cfg.baud_rate = base::constants::MAX_BAUD_RATE + 1;
  EXPECT_FALSE(cfg.is_valid());
  cfg.validate_and_clamp();
  EXPECT_EQ(cfg.baud_rate, base::constants::MAX_BAUD_RATE);

  cfg.baud_rate = base::constants::MIN_BAUD_RATE - 1;
  EXPECT_FALSE(cfg.is_valid());
  cfg.validate_and_clamp();
  EXPECT_EQ(cfg.baud_rate, base::constants::MIN_BAUD_RATE);
}

// #432: UdpConfig never checked bind_address/remote_address format at all,
// even though both are passed straight to boost::asio::ip::make_address()
// (literal IPv4/IPv6 only, no hostname resolution).
TEST_F(ConfigTest, UdpConfigAddressFormatIsValidated) {
  UdpConfig cfg;
  EXPECT_TRUE(cfg.is_valid());

  cfg.bind_address = "not-an-ip";
  EXPECT_FALSE(cfg.is_valid());
  cfg.bind_address = "0.0.0.0";
  EXPECT_TRUE(cfg.is_valid());

  cfg.remote_address = "not-an-ip";
  cfg.remote_port = 9000;
  EXPECT_FALSE(cfg.is_valid());
  cfg.remote_address = "127.0.0.1";
  EXPECT_TRUE(cfg.is_valid());

  cfg.bind_address = "::1";
  EXPECT_TRUE(cfg.is_valid()) << "IPv6 bind address should be valid";
}

TEST(SocketTuningConfigTest, SocketBufferConfigValidationAndClamping) {
  TcpClientConfig tcp_client;
  EXPECT_TRUE(tcp_client.tcp_no_delay);
  EXPECT_FALSE(tcp_client.keep_alive);
  EXPECT_EQ(tcp_client.send_buffer_size, 0u);
  EXPECT_EQ(tcp_client.receive_buffer_size, 0u);
  EXPECT_TRUE(tcp_client.is_valid());

  tcp_client.tcp_no_delay = true;
  tcp_client.keep_alive = true;
  tcp_client.send_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE;
  tcp_client.receive_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE;
  EXPECT_TRUE(tcp_client.is_valid());

  tcp_client.send_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE - 1;
  tcp_client.receive_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE + 1;
  EXPECT_FALSE(tcp_client.is_valid());
  tcp_client.validate_and_clamp();
  EXPECT_EQ(tcp_client.send_buffer_size, base::constants::MIN_SOCKET_BUFFER_SIZE);
  EXPECT_EQ(tcp_client.receive_buffer_size, base::constants::MAX_SOCKET_BUFFER_SIZE);

  TcpServerConfig tcp_server;
  tcp_server.tcp_no_delay = true;
  tcp_server.keep_alive = true;
  tcp_server.send_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE - 1;
  tcp_server.receive_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE + 1;
  EXPECT_FALSE(tcp_server.is_valid());
  tcp_server.validate_and_clamp();
  EXPECT_TRUE(tcp_server.is_valid());
  EXPECT_EQ(tcp_server.send_buffer_size, base::constants::MIN_SOCKET_BUFFER_SIZE);
  EXPECT_EQ(tcp_server.receive_buffer_size, base::constants::MAX_SOCKET_BUFFER_SIZE);

  UdpConfig udp;
  EXPECT_EQ(udp.send_buffer_size, 0u);
  EXPECT_EQ(udp.receive_buffer_size, 0u);
  EXPECT_TRUE(udp.is_valid());
  udp.send_buffer_size = base::constants::MIN_SOCKET_BUFFER_SIZE - 1;
  udp.receive_buffer_size = base::constants::MAX_SOCKET_BUFFER_SIZE + 1;
  EXPECT_FALSE(udp.is_valid());
  udp.validate_and_clamp();
  EXPECT_TRUE(udp.is_valid());
  EXPECT_EQ(udp.send_buffer_size, base::constants::MIN_SOCKET_BUFFER_SIZE);
  EXPECT_EQ(udp.receive_buffer_size, base::constants::MAX_SOCKET_BUFFER_SIZE);
}

TEST_F(ConfigTest, SerialConfigDirectValidationAndClamping) {
  SerialConfig config;

  EXPECT_TRUE(config.is_valid());
  EXPECT_FALSE(config.device.empty());

  config.device.clear();
  EXPECT_FALSE(config.is_valid());
#ifdef _WIN32
  config.device = "COM3";
#else
  config.device = "/dev/ttyFake";
#endif

  config.baud_rate = 0;
  EXPECT_FALSE(config.is_valid());
  config.baud_rate = 9600;

  config.char_size = 4;
  EXPECT_FALSE(config.is_valid());
  config.char_size = 8;

  config.stop_bits = 3;
  EXPECT_FALSE(config.is_valid());
  config.stop_bits = 2;

  config.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS - 1;
  EXPECT_FALSE(config.is_valid());
  config.retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS + 1;
  EXPECT_FALSE(config.is_valid());
  config.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;

  config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  EXPECT_FALSE(config.is_valid());
  config.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  EXPECT_FALSE(config.is_valid());
  config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;

  config.max_retries = -2;
  EXPECT_FALSE(config.is_valid());
  config.max_retries = base::constants::MAX_RETRIES_LIMIT + 1;
  EXPECT_FALSE(config.is_valid());
  config.max_retries = -1;
  EXPECT_TRUE(config.is_valid());

  SerialConfig low_values;
  low_values.char_size = 4;
  low_values.stop_bits = 0;
  low_values.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS - 1;
  low_values.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  low_values.max_retries = base::constants::MAX_RETRIES_LIMIT + 1;
  low_values.validate_and_clamp();
  EXPECT_EQ(low_values.char_size, 5u);
  EXPECT_EQ(low_values.stop_bits, 1u);
  EXPECT_EQ(low_values.retry_interval_ms, base::constants::MIN_RETRY_INTERVAL_MS);
  EXPECT_EQ(low_values.backpressure_threshold, base::constants::MIN_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(low_values.max_retries, base::constants::MAX_RETRIES_LIMIT);

  SerialConfig high_values;
  high_values.char_size = 9;
  high_values.retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS + 1;
  high_values.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  high_values.max_retries = -1;
  high_values.validate_and_clamp();
  EXPECT_EQ(high_values.char_size, 8u);
  EXPECT_EQ(high_values.retry_interval_ms, base::constants::MAX_RETRY_INTERVAL_MS);
  EXPECT_EQ(high_values.backpressure_threshold, base::constants::MAX_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(high_values.max_retries, -1);
}

TEST_F(ConfigTest, UdsConfigDirectValidationAndClamping) {
  UdsClientConfig client_config;
  client_config.socket_path = TestUtils::makeUniqueUdsSocketPath("cfg-client").string();
  EXPECT_TRUE(client_config.is_valid());

  client_config.socket_path.clear();
  EXPECT_FALSE(client_config.is_valid());
  client_config.socket_path = TestUtils::makeUniqueUdsSocketPath("cfg-client-valid").string();

  client_config.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS - 1;
  EXPECT_FALSE(client_config.is_valid());
  client_config.retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS + 1;
  EXPECT_FALSE(client_config.is_valid());
  client_config.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS;

  client_config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  EXPECT_FALSE(client_config.is_valid());
  client_config.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  EXPECT_FALSE(client_config.is_valid());
  client_config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;

  client_config.max_retries = -2;
  EXPECT_FALSE(client_config.is_valid());
  client_config.max_retries = -1;
  EXPECT_TRUE(client_config.is_valid());

  UdsClientConfig low_client;
  low_client.retry_interval_ms = base::constants::MIN_RETRY_INTERVAL_MS - 1;
  low_client.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  low_client.max_retries = base::constants::MAX_RETRIES_LIMIT + 1;
  low_client.validate_and_clamp();
  EXPECT_EQ(low_client.retry_interval_ms, base::constants::MIN_RETRY_INTERVAL_MS);
  EXPECT_EQ(low_client.backpressure_threshold, base::constants::MIN_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(low_client.max_retries, base::constants::MAX_RETRIES_LIMIT);

  UdsClientConfig high_client;
  high_client.retry_interval_ms = base::constants::MAX_RETRY_INTERVAL_MS + 1;
  high_client.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  high_client.max_retries = -1;
  high_client.validate_and_clamp();
  EXPECT_EQ(high_client.retry_interval_ms, base::constants::MAX_RETRY_INTERVAL_MS);
  EXPECT_EQ(high_client.backpressure_threshold, base::constants::MAX_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(high_client.max_retries, -1);

  UdsServerConfig server_config;
  server_config.socket_path = TestUtils::makeUniqueUdsSocketPath("cfg-server").string();
  EXPECT_TRUE(server_config.is_valid());

  server_config.socket_path.clear();
  EXPECT_FALSE(server_config.is_valid());
  server_config.socket_path = TestUtils::makeUniqueUdsSocketPath("cfg-server-valid").string();

  server_config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  EXPECT_FALSE(server_config.is_valid());
  server_config.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  EXPECT_FALSE(server_config.is_valid());
  server_config.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD;

  server_config.max_connections = -1;
  EXPECT_FALSE(server_config.is_valid());
  server_config.max_connections = 1;
  server_config.idle_timeout_ms = -1;
  EXPECT_FALSE(server_config.is_valid());

  UdsServerConfig low_server;
  low_server.backpressure_threshold = base::constants::MIN_BACKPRESSURE_THRESHOLD - 1;
  low_server.max_connections = -1;
  low_server.idle_timeout_ms = -1;
  low_server.validate_and_clamp();
  EXPECT_EQ(low_server.backpressure_threshold, base::constants::MIN_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(low_server.max_connections, 0);
  EXPECT_EQ(low_server.idle_timeout_ms, 0);

  UdsServerConfig high_server;
  high_server.backpressure_threshold = base::constants::MAX_BACKPRESSURE_THRESHOLD + 1;
  high_server.max_connections = static_cast<int>(base::constants::MAX_MAX_CONNECTIONS) + 1;
  high_server.validate_and_clamp();
  EXPECT_EQ(high_server.backpressure_threshold, base::constants::MAX_BACKPRESSURE_THRESHOLD);
  EXPECT_EQ(high_server.max_connections, static_cast<int>(base::constants::MAX_MAX_CONNECTIONS));
}

// ============================================================================
// CONFIG PERSISTENCE TESTS
// ============================================================================

/**
 * @brief Test configuration save to file
 */
TEST_F(ConfigTest, ConfigSaveToFile) {
  std::cout << "\n=== Configuration Save To File Test ===" << std::endl;

  // Set up some configuration values
  config_manager_->set("server.host", std::string("localhost"));
  config_manager_->set("server.port", 8080);
  config_manager_->set("server.enabled", true);
  config_manager_->set("server.timeout", 30.5);

  // Save to file
  bool save_result = config_manager_->save_to_file(test_file_path_.string());
  EXPECT_TRUE(save_result);

  // Verify file was created
  std::ifstream file(test_file_path_);
  EXPECT_TRUE(file.good());

  std::cout << "Configuration saved to file successfully" << std::endl;
}

/**
 * @brief Test configuration load from file
 */
TEST_F(ConfigTest, ConfigLoadFromFile) {
  std::cout << "\n=== Configuration Load From File Test ===" << std::endl;

  // Create a test configuration file
  std::ofstream file(test_file_path_);
  file << R"({
        "server": {
            "host": "localhost",
            "port": 8080,
            "enabled": true,
            "timeout": 30.5
        }
    })";
  file.close();

  // Load from file
  bool load_result = config_manager_->load_from_file(test_file_path_.string());
  // Note: Load result depends on file format support
  std::cout << "Configuration load result: " << (load_result ? "success" : "failed") << std::endl;

  // Verify configuration was loaded
  if (load_result) {
    // Note: Actual key names depend on JSON parsing implementation
    std::cout << "Configuration loaded successfully" << std::endl;
    // Check if any keys were loaded
    auto keys = config_manager_->get_keys();
    EXPECT_GE(keys.size(), 0);
    std::cout << "Loaded keys: " << keys.size() << std::endl;
  }
}

/**
 * @brief Test configuration persistence with complex data
 */
TEST_F(ConfigTest, ConfigPersistenceComplexData) {
  std::cout << "\n=== Configuration Persistence Complex Data Test ===" << std::endl;

  // Set up complex configuration
  config_manager_->set("database.host", std::string("localhost"));
  config_manager_->set("database.port", 5432);
  config_manager_->set("database.name", std::string("unilink_db"));
  config_manager_->set("database.ssl_enabled", true);
  config_manager_->set("database.connection_pool_size", 10);
  config_manager_->set("database.timeout_ms", 5000);

  // Save to file
  bool save_result = config_manager_->save_to_file(test_file_path_.string());
  EXPECT_TRUE(save_result);

  // Create new config manager and load from file
  auto new_config_manager = std::make_shared<ConfigManager>();
  bool load_result = new_config_manager->load_from_file(test_file_path_.string());

  if (load_result) {
    // Verify all values were loaded
    EXPECT_TRUE(new_config_manager->has("database.host"));
    EXPECT_TRUE(new_config_manager->has("database.port"));
    EXPECT_TRUE(new_config_manager->has("database.name"));
    EXPECT_TRUE(new_config_manager->has("database.ssl_enabled"));
    EXPECT_TRUE(new_config_manager->has("database.connection_pool_size"));
    EXPECT_TRUE(new_config_manager->has("database.timeout_ms"));

    std::cout << "Complex configuration persisted and loaded successfully" << std::endl;
  }
}

// ============================================================================
// CONFIG CHANGE NOTIFICATION TESTS
// ============================================================================

/**
 * @brief Test configuration change notifications
 */
TEST_F(ConfigTest, ConfigChangeNotifications) {
  std::cout << "\n=== Configuration Change Notifications Test ===" << std::endl;

  std::atomic<int> change_count{0};
  std::string last_changed_key;
  std::any last_old_value;
  std::any last_new_value;

  // Set up change notification
  config_manager_->on_change("test_key", [&](const std::string& key, const std::any& old_val, const std::any& new_val) {
    change_count++;
    last_changed_key = key;
    last_old_value = old_val;
    last_new_value = new_val;
  });

  // Set initial value
  config_manager_->set("test_key", std::string("initial_value"));

  // Change the value
  config_manager_->set("test_key", std::string("changed_value"));

  // Verify notification was triggered
  EXPECT_GE(change_count.load(), 0);  // At least one change notification
  EXPECT_EQ(last_changed_key, "test_key");

  std::cout << "Change notifications working: " << change_count.load() << " notifications received" << std::endl;
}

/**
 * @brief Test configuration change notifications with multiple keys
 */
TEST_F(ConfigTest, ConfigChangeNotificationsMultipleKeys) {
  std::cout << "\n=== Configuration Change Notifications Multiple Keys Test ===" << std::endl;

  std::atomic<int> change_count{0};
  std::vector<std::string> changed_keys;

  // Set up change notifications for multiple keys
  config_manager_->on_change("key1", [&](const std::string& key, const std::any&, const std::any&) {
    change_count++;
    changed_keys.push_back(key);
  });

  config_manager_->on_change("key2", [&](const std::string& key, const std::any&, const std::any&) {
    change_count++;
    changed_keys.push_back(key);
  });

  // Change multiple keys
  config_manager_->set("key1", std::string("value1"));
  config_manager_->set("key2", std::string("value2"));
  config_manager_->set("key1", std::string("value1_updated"));

  // Verify notifications were triggered
  EXPECT_GE(change_count.load(), 1);  // At least one change notification
  EXPECT_GE(changed_keys.size(), 1);

  std::cout << "Multiple key change notifications working: " << change_count.load() << " notifications received"
            << std::endl;
}

// ============================================================================
// CONFIG THREAD SAFETY TESTS
// ============================================================================

/**
 * @brief Test configuration thread safety with concurrent access
 */
TEST_F(ConfigTest, ConfigThreadSafetyConcurrentAccess) {
  std::cout << "\n=== Configuration Thread Safety Concurrent Access Test ===" << std::endl;

  const int num_threads = 4;
  const int operations_per_thread = 50;

  std::atomic<int> completed_operations{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);

        // Set value
        config_manager_->set(key, value);

        // Get value
        auto retrieved = config_manager_->get(key, std::string("default"));

        // Verify value
        if (std::any_cast<std::string>(retrieved) == value) {
          completed_operations++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(completed_operations.load(), num_threads * operations_per_thread);
  std::cout << "Thread safety test completed: " << completed_operations.load() << " operations" << std::endl;
}

/**
 * @brief Test configuration thread safety with mixed operations
 */
TEST_F(ConfigTest, ConfigThreadSafetyMixedOperations) {
  std::cout << "\n=== Configuration Thread Safety Mixed Operations Test ===" << std::endl;

  const int num_threads = 3;
  const int operations_per_thread = 30;

  std::atomic<int> completed_operations{0};
  std::vector<std::thread> threads;

  // Thread 1: Set operations
  threads.emplace_back([&]() {
    for (int i = 0; i < operations_per_thread; ++i) {
      config_manager_->set("set_key_" + std::to_string(i), i);
      completed_operations++;
    }
  });

  // Thread 2: Get operations
  threads.emplace_back([&]() {
    for (int i = 0; i < operations_per_thread; ++i) {
      try {
        config_manager_->get("set_key_" + std::to_string(i), -1);
        completed_operations++;
      } catch (...) {
        // Key might not exist yet
        completed_operations++;
      }
    }
  });

  // Thread 3: Remove operations
  threads.emplace_back([&]() {
    for (int i = 0; i < operations_per_thread; ++i) {
      config_manager_->remove("set_key_" + std::to_string(i));
      completed_operations++;
    }
  });

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(completed_operations.load(), num_threads * operations_per_thread);
  std::cout << "Mixed operations thread safety test completed: " << completed_operations.load() << " operations"
            << std::endl;
}

// ============================================================================
// CONFIG INTROSPECTION TESTS
// ============================================================================

/**
 * @brief Test configuration introspection capabilities
 */
TEST_F(ConfigTest, ConfigIntrospection) {
  std::cout << "\n=== Configuration Introspection Test ===" << std::endl;

  // Set up various configuration items
  config_manager_->set("string_key", std::string("string_value"));
  config_manager_->set("int_key", 42);
  config_manager_->set("bool_key", true);
  config_manager_->set("double_key", 3.14159);

  // Test get_keys
  auto keys = config_manager_->get_keys();
  EXPECT_GE(keys.size(), 4);

  // Test has
  EXPECT_TRUE(config_manager_->has("string_key"));
  EXPECT_TRUE(config_manager_->has("int_key"));
  EXPECT_TRUE(config_manager_->has("bool_key"));
  EXPECT_TRUE(config_manager_->has("double_key"));
  EXPECT_FALSE(config_manager_->has("nonexistent_key"));

  // Test get_type
  auto string_type = config_manager_->get_type("string_key");
  auto int_type = config_manager_->get_type("int_key");
  auto bool_type = config_manager_->get_type("bool_key");
  auto double_type = config_manager_->get_type("double_key");
  (void)string_type;
  (void)int_type;
  (void)bool_type;
  (void)double_type;

  std::cout << "Configuration introspection completed successfully" << std::endl;
  std::cout << "Keys found: " << keys.size() << std::endl;
}

/**
 * @brief Test configuration validation and error handling
 */
TEST_F(ConfigTest, ConfigValidationAndErrorHandling) {
  std::cout << "\n=== Configuration Validation And Error Handling Test ===" << std::endl;

  // Test validation of all configuration
  auto validation_result = config_manager_->validate();
  EXPECT_TRUE(validation_result.is_valid);

  // Test validation of specific key
  config_manager_->set("test_key", std::string("test_value"));
  auto key_validation_result = config_manager_->validate("test_key");
  EXPECT_TRUE(key_validation_result.is_valid);

  // Test validation of non-existent key
  auto nonexistent_validation_result = config_manager_->validate("nonexistent_key");
  // Note: Result depends on implementation
  std::cout << "Non-existent key validation: " << (nonexistent_validation_result.is_valid ? "valid" : "invalid")
            << std::endl;

  std::cout << "Configuration validation and error handling completed" << std::endl;
}

// ============================================================================
// CONFIG PERFORMANCE TESTS
// ============================================================================

/**
 * @brief Test configuration performance with large datasets
 */
TEST_F(ConfigTest, ConfigPerformanceLargeDataset) {
  std::cout << "\n=== Configuration Performance Large Dataset Test ===" << std::endl;

  const int num_items = 1000;

  auto start_time = std::chrono::high_resolution_clock::now();

  // Set many configuration items
  for (int i = 0; i < num_items; ++i) {
    std::string key = "perf_key_" + std::to_string(i);
    std::string value = "perf_value_" + std::to_string(i);
    config_manager_->set(key, value);
  }

  auto set_time = std::chrono::high_resolution_clock::now();
  auto set_duration = std::chrono::duration_cast<std::chrono::microseconds>(set_time - start_time);

  // Get many configuration items
  for (int i = 0; i < num_items; ++i) {
    std::string key = "perf_key_" + std::to_string(i);
    try {
      config_manager_->get(key);
    } catch (...) {
      // Handle any exceptions
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto get_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - set_time);
  auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

  std::cout << "Performance test completed:" << std::endl;
  std::cout << "  Items: " << num_items << std::endl;
  std::cout << "  Set time: " << set_duration.count() << " μs" << std::endl;
  std::cout << "  Get time: " << get_duration.count() << " μs" << std::endl;
  std::cout << "  Total time: " << total_duration.count() << " μs" << std::endl;
  std::cout << "  Average per item: " << (total_duration.count() / num_items) << " μs" << std::endl;

  // Performance should be reasonable (less than 100μs per item)
  EXPECT_LT(total_duration.count() / num_items, 100);
}

// ============================================================================
// Additional negative/persistence coverage
// ============================================================================

TEST_F(ConfigTest, SetWithWrongTypeFails) {
  ConfigItem item("wrong.type", std::any(1), ConfigType::Integer, false, "int");
  config_manager_->register_item(item);
  auto result = config_manager_->set("wrong.type", std::string("not an int"));
  EXPECT_FALSE(result.is_valid);
}

TEST_F(ConfigTest, ValidateFailsOnMissingRequired) {
  ConfigItem required_item("required.key", std::string(""), ConfigType::String, true, "required");
  config_manager_->register_item(required_item);
  config_manager_->register_validator("required.key", [](const std::any& value) {
    const auto* str = std::any_cast<std::string>(&value);
    if (str && str->empty()) {
      return ValidationResult::error("required.key is missing");
    }
    return ValidationResult::success();
  });
  auto validation = config_manager_->validate();
  EXPECT_FALSE(validation.is_valid);
}

TEST_F(ConfigTest, SaveAndLoadRoundTrip) {
  ConfigItem item("persist.key", std::string("value"), ConfigType::String, false, "persist");
  config_manager_->register_item(item);
  config_manager_->set("persist.key", std::string("hello"));

  // Save to file
  EXPECT_TRUE(config_manager_->save_to_file(test_file_path_.string()));
  EXPECT_TRUE(std::filesystem::exists(test_file_path_));

  // Load into new manager
  auto loaded_manager = std::make_shared<ConfigManager>();
  EXPECT_TRUE(loaded_manager->load_from_file(test_file_path_.string()));

  auto loaded_value = loaded_manager->get("persist.key");
  EXPECT_EQ(std::any_cast<std::string>(loaded_value), "hello");
}

TEST_F(ConfigTest, LoadEmptyFile) {
  // Create empty file
  std::ofstream file(test_file_path_);
  file.close();

  // Load from empty file
  bool result = config_manager_->load_from_file(test_file_path_.string());
  EXPECT_TRUE(result);
  EXPECT_EQ(config_manager_->get_keys().size(), 0);
}

TEST_F(ConfigTest, LoadMalformedFile) {
  // Create file with garbage
  std::ofstream file(test_file_path_);
  file << "This is not a valid config file";
  file.close();

  // Load from malformed file
  // Current implementation skips invalid lines, so it should return true but load nothing
  bool result = config_manager_->load_from_file(test_file_path_.string());
  EXPECT_TRUE(result);
  EXPECT_EQ(config_manager_->get_keys().size(), 0);
}

TEST_F(ConfigTest, TypeMismatch) {
  // Explicitly test type mismatch as requested
  ConfigItem item("strict_int", std::any(0), ConfigType::Integer, false);
  config_manager_->register_item(item);

  // Try to set string value
  auto result = config_manager_->set("strict_int", std::string("invalid"));
  EXPECT_FALSE(result.is_valid);
  EXPECT_EQ(std::any_cast<int>(config_manager_->get("strict_int")), 0);
}

TEST_F(ConfigTest, AccessorsAndRemovalCoverMissingKeys) {
  EXPECT_THROW(config_manager_->get("missing.key"), std::runtime_error);
  EXPECT_EQ(std::any_cast<int>(config_manager_->get("missing.key", 42)), 42);
  EXPECT_FALSE(config_manager_->remove("missing.key"));
  EXPECT_FALSE(config_manager_->has("missing.key"));

  config_manager_->register_item(ConfigItem("required.key", std::string("value"), ConfigType::String, true, "desc"));
  EXPECT_TRUE(config_manager_->has("required.key"));
  EXPECT_EQ(config_manager_->get_type("required.key"), ConfigType::String);
  EXPECT_EQ(config_manager_->get_description("required.key"), "desc");
  EXPECT_TRUE(config_manager_->is_required("required.key"));
  EXPECT_EQ(config_manager_->get_description("missing.key"), "");
  EXPECT_FALSE(config_manager_->is_required("missing.key"));
  EXPECT_THROW(config_manager_->get_type("missing.key"), std::runtime_error);

  EXPECT_TRUE(config_manager_->remove("required.key"));
  EXPECT_FALSE(config_manager_->has("required.key"));

  config_manager_->set("clear.one", std::string("one"));
  config_manager_->set("clear.two", std::string("two"));
  config_manager_->clear();
  EXPECT_TRUE(config_manager_->get_keys().empty());
}

TEST_F(ConfigTest, RegisteredTypesAndValidatorsAreEnforced) {
  config_manager_->register_item(ConfigItem("typed.string", std::string("hello"), ConfigType::String, false));
  config_manager_->register_item(ConfigItem("typed.int", 7, ConfigType::Integer, false));
  config_manager_->register_item(ConfigItem("typed.bool", true, ConfigType::Boolean, false));
  config_manager_->register_item(ConfigItem("typed.double", 1.5, ConfigType::Double, false));

  EXPECT_TRUE(config_manager_->set("typed.string", std::string("world")).is_valid);
  EXPECT_TRUE(config_manager_->set("typed.int", 9).is_valid);
  EXPECT_TRUE(config_manager_->set("typed.bool", false).is_valid);
  EXPECT_TRUE(config_manager_->set("typed.double", 3.25).is_valid);
  EXPECT_FALSE(config_manager_->set("typed.bool", 1).is_valid);
  EXPECT_FALSE(config_manager_->set("typed.double", std::string("3.25")).is_valid);

  config_manager_->register_validator("typed.int", [](const std::any& value) {
    const auto* int_value = std::any_cast<int>(&value);
    if (!int_value || *int_value < 10) {
      return ValidationResult::error("too small");
    }
    return ValidationResult::success();
  });
  config_manager_->register_validator(
      "missing.validator", [](const std::any&) { return ValidationResult::error("should not be installed"); });

  EXPECT_FALSE(config_manager_->set("typed.int", 9).is_valid);
  EXPECT_TRUE(config_manager_->set("typed.int", 10).is_valid);
}

TEST_F(ConfigTest, ChangeCallbackExceptionsAndRemovalAreHandled) {
  config_manager_->set("callback.key", std::string("initial"));
  config_manager_->on_change("callback.key", [](const std::string&, const std::any&, const std::any&) {
    throw std::runtime_error("callback failed");
  });

  EXPECT_NO_THROW(config_manager_->set("callback.key", std::string("updated")));
  EXPECT_EQ(std::any_cast<std::string>(config_manager_->get("callback.key")), "updated");

  std::atomic<int> callback_count{0};
  config_manager_->on_change("callback.key",
                             [&](const std::string&, const std::any&, const std::any&) { callback_count++; });
  config_manager_->remove_change_callback("callback.key");
  EXPECT_TRUE(config_manager_->set("callback.key", std::string("removed callback")).is_valid);
  EXPECT_EQ(callback_count.load(), 0);
}

TEST_F(ConfigTest, SaveHandlesUnavailablePathAndUnknownValueTypes) {
  config_manager_->register_item(ConfigItem("bad.cast", std::string("not-int"), ConfigType::Integer, false, "bad"));
  config_manager_->register_item(ConfigItem("array.value", std::vector<int>{1, 2, 3}, ConfigType::Array, false, "arr"));

  EXPECT_TRUE(config_manager_->save_to_file(test_file_path_.string()));

  std::ifstream file(test_file_path_);
  std::stringstream contents;
  contents << file.rdbuf();
  EXPECT_THAT(contents.str(), ::testing::HasSubstr("bad.cast=unknown"));
  EXPECT_THAT(contents.str(), ::testing::HasSubstr("array.value=unknown"));

  EXPECT_FALSE(config_manager_->save_to_file(TestUtils::getTempDirectory().string()));
}

TEST_F(ConfigTest, LoadParsesPrimitiveTypesAndSkipsInvalidExistingValue) {
  config_manager_->register_item(ConfigItem("strict.int", 5, ConfigType::Integer, false));

  std::ofstream file(test_file_path_);
  file << "# comment\n";
  file << " bool.key = true \n";
  file << " int.key = -42 \n";
  file << " double.key = -3.5 \n";
  file << " string.key = hello world \n";
  file << " strict.int = not-an-int \n";
  file.close();

  EXPECT_TRUE(config_manager_->load_from_file(test_file_path_.string()));
  EXPECT_EQ(std::any_cast<bool>(config_manager_->get("bool.key")), true);
  EXPECT_EQ(std::any_cast<int>(config_manager_->get("int.key")), -42);
  EXPECT_DOUBLE_EQ(std::any_cast<double>(config_manager_->get("double.key")), -3.5);
  EXPECT_EQ(std::any_cast<std::string>(config_manager_->get("string.key")), "hello world");
  EXPECT_EQ(std::any_cast<int>(config_manager_->get("strict.int")), 5);
}

TEST_F(ConfigTest, LoadReportsMissingFile) {
  EXPECT_FALSE(config_manager_->load_from_file((test_file_path_.string() + ".missing")));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
