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

#include <gtest/gtest.h>

#include <cctype>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "wirestead/util/input_validator.hpp"

using namespace wirestead;
using namespace wirestead::util;

namespace {

std::string make_param_name(std::string_view description) {
  std::string name;
  for (const unsigned char ch : description) {
    if (std::isalnum(ch)) {
      name.push_back(static_cast<char>(ch));
    } else if (!name.empty() && name.back() != '_') {
      name.push_back('_');
    }
  }

  while (!name.empty() && name.back() == '_') {
    name.pop_back();
  }
  if (name.empty() || std::isdigit(static_cast<unsigned char>(name.front()))) {
    name.insert(0, "Case_");
  }
  return name;
}

template <typename T>
std::string param_name_from_description(const ::testing::TestParamInfo<T>& info) {
  return make_param_name(info.param.description) + "_" + std::to_string(info.index);
}

}  // namespace

// Existing tests
TEST(InputValidatorTest, ValidatePort) {
  EXPECT_NO_THROW(InputValidator::validate_port(1));
  EXPECT_NO_THROW(InputValidator::validate_port(65535));
  EXPECT_THROW(InputValidator::validate_port(0), diagnostics::ValidationException);
  // 65536 triggers implicit conversion warning/overflow depending on call site,
  // but if passed as int literal to uint16_t arg, it becomes 0.
  // We can force it to verify behavior.
  EXPECT_THROW(InputValidator::validate_port(static_cast<uint16_t>(65536)), diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateHost) {
  EXPECT_NO_THROW(InputValidator::validate_host("localhost"));
  EXPECT_NO_THROW(InputValidator::validate_host("127.0.0.1"));
  EXPECT_NO_THROW(InputValidator::validate_host("example.com"));
  EXPECT_THROW(InputValidator::validate_host(""), diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateProtocol) {
  // IPv4
  EXPECT_NO_THROW(InputValidator::validate_ipv4_address("1.1.1.1"));
  EXPECT_NO_THROW(InputValidator::validate_ipv4_address("255.255.255.255"));
  EXPECT_THROW(InputValidator::validate_ipv4_address("256.2.3.4"), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.2.3"), diagnostics::ValidationException);  // Incomplete
  EXPECT_THROW(InputValidator::validate_ipv4_address("invalid"), diagnostics::ValidationException);

  // IPv6
  EXPECT_NO_THROW(InputValidator::validate_ipv6_address("::1"));
  EXPECT_NO_THROW(InputValidator::validate_ipv6_address("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
  EXPECT_THROW(InputValidator::validate_ipv6_address("1.1.1.1"),
               diagnostics::ValidationException);  // Not a valid IPv6 format
  EXPECT_THROW(InputValidator::validate_ipv6_address("invalid"), diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateSerialParams) {
  EXPECT_NO_THROW(InputValidator::validate_device_path("/dev/ttyUSB0"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("COM1"));
  EXPECT_THROW(InputValidator::validate_device_path(""), diagnostics::ValidationException);

  EXPECT_NO_THROW(InputValidator::validate_baud_rate(base::constants::MIN_BAUD_RATE));
  EXPECT_NO_THROW(InputValidator::validate_baud_rate(base::constants::MAX_BAUD_RATE));
  EXPECT_THROW(InputValidator::validate_baud_rate(base::constants::MIN_BAUD_RATE - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_baud_rate(base::constants::MAX_BAUD_RATE + 1),
               diagnostics::ValidationException);

  EXPECT_NO_THROW(InputValidator::validate_data_bits(base::constants::MIN_DATA_BITS));
  EXPECT_NO_THROW(InputValidator::validate_data_bits(base::constants::MAX_DATA_BITS));
  EXPECT_THROW(InputValidator::validate_data_bits(base::constants::MIN_DATA_BITS - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_data_bits(base::constants::MAX_DATA_BITS + 1),
               diagnostics::ValidationException);

  EXPECT_NO_THROW(InputValidator::validate_stop_bits(base::constants::MIN_STOP_BITS));
  EXPECT_NO_THROW(InputValidator::validate_stop_bits(base::constants::MAX_STOP_BITS));
  EXPECT_THROW(InputValidator::validate_stop_bits(base::constants::MIN_STOP_BITS - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_stop_bits(base::constants::MAX_STOP_BITS + 1),
               diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateCommonParams) {
  // Buffer Size
  EXPECT_NO_THROW(InputValidator::validate_buffer_size(base::constants::MIN_BUFFER_SIZE));
  EXPECT_NO_THROW(InputValidator::validate_buffer_size(base::constants::MAX_BUFFER_SIZE));
  EXPECT_THROW(InputValidator::validate_buffer_size(base::constants::MIN_BUFFER_SIZE - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_buffer_size(base::constants::MAX_BUFFER_SIZE + 1),
               diagnostics::ValidationException);

  // Timeout
  EXPECT_NO_THROW(InputValidator::validate_timeout(base::constants::MIN_CONNECTION_TIMEOUT_MS));
  EXPECT_NO_THROW(InputValidator::validate_timeout(base::constants::MAX_CONNECTION_TIMEOUT_MS));
  EXPECT_THROW(InputValidator::validate_timeout(base::constants::MIN_CONNECTION_TIMEOUT_MS - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_timeout(base::constants::MAX_CONNECTION_TIMEOUT_MS + 1),
               diagnostics::ValidationException);

  // Retry Interval
  EXPECT_NO_THROW(InputValidator::validate_retry_interval(base::constants::MIN_RETRY_INTERVAL_MS));
  EXPECT_NO_THROW(InputValidator::validate_retry_interval(base::constants::MAX_RETRY_INTERVAL_MS));
  EXPECT_THROW(InputValidator::validate_retry_interval(base::constants::MIN_RETRY_INTERVAL_MS - 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_retry_interval(base::constants::MAX_RETRY_INTERVAL_MS + 1),
               diagnostics::ValidationException);

  // Retry Count
  EXPECT_NO_THROW(InputValidator::validate_retry_count(0));                                     // Valid finite
  EXPECT_NO_THROW(InputValidator::validate_retry_count(base::constants::MAX_RETRIES_LIMIT));    // Valid finite max
  EXPECT_NO_THROW(InputValidator::validate_retry_count(base::constants::DEFAULT_MAX_RETRIES));  // -1, Valid infinite
  EXPECT_THROW(InputValidator::validate_retry_count(base::constants::MAX_RETRIES_LIMIT + 1),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_retry_count(-2),
               diagnostics::ValidationException);  // Any value less than -1 or less than 0 but not -1
}

TEST(InputValidatorTest, ValidateGenericHelpers) {
  // Non-empty String
  EXPECT_NO_THROW(InputValidator::validate_non_empty_string("test", "name"));
  EXPECT_THROW(InputValidator::validate_non_empty_string("", "name"), diagnostics::ValidationException);

  // String Length
  EXPECT_NO_THROW(InputValidator::validate_string_length("test", 10, "string_field"));
  EXPECT_NO_THROW(InputValidator::validate_string_length("longstring", 10, "string_field"));
  EXPECT_THROW(InputValidator::validate_string_length("too long string", 10, "string_field"),
               diagnostics::ValidationException);

  // Very long string (> 256 chars)
  std::string very_long_string(300, 'a');
  EXPECT_THROW(InputValidator::validate_string_length(very_long_string, 256, "long_string"),
               diagnostics::ValidationException);

  // Positive Number
  EXPECT_NO_THROW(InputValidator::validate_positive_number(static_cast<int64_t>(1), "val"));
  EXPECT_THROW(InputValidator::validate_positive_number(static_cast<int64_t>(0), "val"),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_positive_number(static_cast<int64_t>(-1), "val"),
               diagnostics::ValidationException);

  // Validate Range (int64_t)
  EXPECT_NO_THROW(InputValidator::validate_range(static_cast<int64_t>(10), static_cast<int64_t>(0),
                                                 static_cast<int64_t>(20), "val"));
  EXPECT_NO_THROW(InputValidator::validate_range(static_cast<int64_t>(0), static_cast<int64_t>(0),
                                                 static_cast<int64_t>(20), "val"));
  EXPECT_NO_THROW(InputValidator::validate_range(static_cast<int64_t>(20), static_cast<int64_t>(0),
                                                 static_cast<int64_t>(20), "val"));
  EXPECT_THROW(InputValidator::validate_range(static_cast<int64_t>(-1), static_cast<int64_t>(0),
                                              static_cast<int64_t>(20), "val"),
               diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_range(static_cast<int64_t>(21), static_cast<int64_t>(0),
                                              static_cast<int64_t>(20), "val"),
               diagnostics::ValidationException);

  // Validate Range (size_t)
  EXPECT_NO_THROW(
      InputValidator::validate_range(static_cast<size_t>(10), static_cast<size_t>(0), static_cast<size_t>(20), "val"));
  EXPECT_NO_THROW(
      InputValidator::validate_range(static_cast<size_t>(0), static_cast<size_t>(0), static_cast<size_t>(20), "val"));
  EXPECT_NO_THROW(
      InputValidator::validate_range(static_cast<size_t>(20), static_cast<size_t>(0), static_cast<size_t>(20), "val"));
  EXPECT_THROW(
      InputValidator::validate_range(static_cast<size_t>(21), static_cast<size_t>(0), static_cast<size_t>(20), "val"),
      diagnostics::ValidationException);

  // Validate memory alignment
  void* aligned_ptr = reinterpret_cast<void*>(0x1000);  // Dummy pointer
  EXPECT_NO_THROW(InputValidator::validate_memory_alignment(aligned_ptr, 8));
  EXPECT_THROW(InputValidator::validate_memory_alignment(nullptr, 8), diagnostics::ValidationException);
  void* unaligned_ptr = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(aligned_ptr) + 1);
  EXPECT_THROW(InputValidator::validate_memory_alignment(unaligned_ptr, 8), diagnostics::ValidationException);
}

TEST(InputValidatorTest, DetailedHelperLogic) {
  // IPv4 Edge Cases
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.2.3"), diagnostics::ValidationException);  // Too few octets
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.2.3.4.5"),
               diagnostics::ValidationException);                                                   // Too many octets
  EXPECT_THROW(InputValidator::validate_ipv4_address("1..3.4"), diagnostics::ValidationException);  // Empty octet
  EXPECT_THROW(InputValidator::validate_ipv4_address(".1.2.3"), diagnostics::ValidationException);  // Empty first octet
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.2.3."), diagnostics::ValidationException);  // Empty last octet
  EXPECT_THROW(InputValidator::validate_ipv4_address("01.1.1.1"), diagnostics::ValidationException);  // Leading zero
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.01.1.1"), diagnostics::ValidationException);  // Leading zero
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.1.1.01"), diagnostics::ValidationException);  // Leading zero
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.a.1.1"), diagnostics::ValidationException);   // Non-digit
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.1.1.256"),
               diagnostics::ValidationException);  // Out of range > 255
  EXPECT_THROW(InputValidator::validate_ipv4_address("256.1.1.1"),
               diagnostics::ValidationException);  // Out of range > 255

  // IPv6 Edge Cases
  EXPECT_THROW(InputValidator::validate_ipv6_address("1:2"), diagnostics::ValidationException);   // Malformed
  EXPECT_THROW(InputValidator::validate_ipv6_address("g::1"), diagnostics::ValidationException);  // Invalid hex char

  // Hostname Edge Cases
  EXPECT_THROW(InputValidator::validate_host("-test.com"), diagnostics::ValidationException);  // Starts with hyphen
  EXPECT_THROW(InputValidator::validate_host("test.com-"), diagnostics::ValidationException);  // Ends with hyphen
  EXPECT_THROW(InputValidator::validate_host("invalid_host.com"),
               diagnostics::ValidationException);                                              // Underscore invalid
  EXPECT_THROW(InputValidator::validate_host("test..com"), diagnostics::ValidationException);  // Empty label

  std::string long_label(64, 'a');
  EXPECT_THROW(InputValidator::validate_host(long_label + ".com"), diagnostics::ValidationException);  // Label too long

  // Hyphen in label edge cases
  EXPECT_THROW(InputValidator::validate_host("example.-com"),
               diagnostics::ValidationException);  // Label starts with hyphen
  EXPECT_THROW(InputValidator::validate_host("example.com-"),
               diagnostics::ValidationException);                  // Ends with hyphen (caught by back() check too)
  EXPECT_NO_THROW(InputValidator::validate_host("example.c-om"));  // Middle hyphen is valid

  // Device Path Edge Cases
  EXPECT_THROW(InputValidator::validate_device_path("/dev/bad?"),
               diagnostics::ValidationException);  // Invalid char in unix path
  EXPECT_THROW(InputValidator::validate_device_path("COM"), diagnostics::ValidationException);     // Incomplete COM
  EXPECT_THROW(InputValidator::validate_device_path("COM0"), diagnostics::ValidationException);    // COM0 invalid
  EXPECT_THROW(InputValidator::validate_device_path("COM256"), diagnostics::ValidationException);  // COM256 invalid
  EXPECT_THROW(InputValidator::validate_device_path("COM1a"),
               diagnostics::ValidationException);  // Invalid number format

  EXPECT_NO_THROW(InputValidator::validate_device_path("NUL"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("CON"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("PRN"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("AUX"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("LPT1"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("LPT2"));
  EXPECT_NO_THROW(InputValidator::validate_device_path("LPT3"));
}

TEST(InputValidatorTest, ValidateIPv4Overflow) {
  // Test for std::stoll overflow/exception catch
  // This string of digits is too large for long long
  EXPECT_THROW(InputValidator::validate_ipv4_address("1.1.1.9999999999999999999999999"),
               diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateParityCaseInsensitive) {
  EXPECT_NO_THROW(InputValidator::validate_parity("none"));
  EXPECT_NO_THROW(InputValidator::validate_parity("NONE"));
  EXPECT_NO_THROW(InputValidator::validate_parity("NoNe"));

  EXPECT_NO_THROW(InputValidator::validate_parity("odd"));
  EXPECT_NO_THROW(InputValidator::validate_parity("ODD"));
  EXPECT_NO_THROW(InputValidator::validate_parity("Odd"));

  EXPECT_NO_THROW(InputValidator::validate_parity("even"));
  EXPECT_NO_THROW(InputValidator::validate_parity("EVEN"));
  EXPECT_NO_THROW(InputValidator::validate_parity("eVen"));

  EXPECT_THROW(InputValidator::validate_parity("mark"), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_parity(""), diagnostics::ValidationException);
}

TEST(InputValidatorTest, ValidateIPv6ExtraCases) {
  EXPECT_THROW(InputValidator::validate_ipv6_address("[::1]:80"), diagnostics::ValidationException);  // With port
  EXPECT_NO_THROW(InputValidator::validate_ipv6_address("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
  // EXPECT_NO_THROW(InputValidator::validate_ipv6_address("2001:db8:85a3::8a2e:370:7334")); // Shortened - Not
  // supported by current regex
}

TEST(InputValidatorTest, ValidateIPv6WhitespaceAndPrefix) {
  // Whitespace
  EXPECT_THROW(InputValidator::validate_ipv6_address(" ::1"), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_ipv6_address("::1 "), diagnostics::ValidationException);

  // Protocol Prefix
  EXPECT_THROW(InputValidator::validate_ipv6_address("tcp://[::1]"), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_ipv6_address("udp://::1"), diagnostics::ValidationException);
}

// ----------------------------------------------------------------------------
// NEW PARAMETERIZED TESTS
// ----------------------------------------------------------------------------

struct IPv4TestCase {
  std::string address;
  bool should_throw;
  std::string description;
};

void PrintTo(const IPv4TestCase& param, std::ostream* os) { *os << param.description; }

class IPv4ParamTest : public ::testing::TestWithParam<IPv4TestCase> {};

TEST_P(IPv4ParamTest, ValidateAddress) {
  const auto& param = GetParam();
  if (param.should_throw) {
    EXPECT_THROW(InputValidator::validate_ipv4_address(param.address), diagnostics::ValidationException)
        << "Should throw for: " << param.description;
  } else {
    EXPECT_NO_THROW(InputValidator::validate_ipv4_address(param.address))
        << "Should not throw for: " << param.description;
  }
}

INSTANTIATE_TEST_SUITE_P(IPv4Scenarios, IPv4ParamTest,
                         ::testing::Values(IPv4TestCase{"1.1.1.1.1", true, "Too many octets"},
                                           IPv4TestCase{"256.0.0.1", true, "First octet overflow"},
                                           IPv4TestCase{"192.168.1", true, "Incomplete address"},
                                           IPv4TestCase{"abc.def.ghi.jkl", true, "Non-digit characters"},
                                           IPv4TestCase{" 127.0.0.1", true, "Leading whitespace"},
                                           IPv4TestCase{"127.0.0.1 ", true, "Trailing whitespace"},
                                           IPv4TestCase{"tcp://1.1.1.1", true, "Protocol prefix"},
                                           IPv4TestCase{"1.1.1.1", false, "Valid simple address"},
                                           IPv4TestCase{"255.255.255.255", false, "Valid max address"},
                                           IPv4TestCase{"0.0.0.0", false, "Valid min address"}),
                         param_name_from_description<IPv4TestCase>);

struct HostTestCase {
  std::string host;
  bool should_throw;
  std::string description;
};

void PrintTo(const HostTestCase& param, std::ostream* os) { *os << param.description; }

class HostParamTest : public ::testing::TestWithParam<HostTestCase> {};

TEST_P(HostParamTest, ValidateHost) {
  const auto& param = GetParam();
  if (param.should_throw) {
    EXPECT_THROW(InputValidator::validate_host(param.host), diagnostics::ValidationException)
        << "Should throw for: " << param.description;
  } else {
    EXPECT_NO_THROW(InputValidator::validate_host(param.host)) << "Should not throw for: " << param.description;
  }
}

INSTANTIATE_TEST_SUITE_P(HostScenarios, HostParamTest,
                         ::testing::Values(HostTestCase{"tcp://example.com", true, "Protocol prefix"},
                                           HostTestCase{"http://example.com", true, "Protocol prefix"},
                                           HostTestCase{" example.com", true, "Leading whitespace"},
                                           HostTestCase{"example.com ", true, "Trailing whitespace"},
                                           HostTestCase{"[::1]:80", true, "IPv6 with port"},
                                           HostTestCase{"::1", false, "IPv6 address"},
                                           HostTestCase{"localhost", false, "Simple hostname"},
                                           HostTestCase{"1.1.1.1", false, "IPv4 address"}),
                         param_name_from_description<HostTestCase>);

struct DevicePathTestCase {
  std::string path;
  bool should_throw;
  std::string description;
};

void PrintTo(const DevicePathTestCase& param, std::ostream* os) { *os << param.description; }

class DevicePathParamTest : public ::testing::TestWithParam<DevicePathTestCase> {};

TEST_P(DevicePathParamTest, ValidatePath) {
  const auto& param = GetParam();
  if (param.should_throw) {
    EXPECT_THROW(InputValidator::validate_device_path(param.path), diagnostics::ValidationException)
        << "Should throw for: " << param.description;
  } else {
    EXPECT_NO_THROW(InputValidator::validate_device_path(param.path)) << "Should not throw for: " << param.description;
  }
}

INSTANTIATE_TEST_SUITE_P(
    DevicePathScenarios, DevicePathParamTest,
    ::testing::Values(
        // Windows style
        DevicePathTestCase{"C:\\Windows\\System32", true, "Windows absolute path (rejected as device)"},
        DevicePathTestCase{"D:\\Data\\file.txt", true, "Windows file path (rejected as device)"},
        // Linux style
        DevicePathTestCase{"/usr/bin/bash", true, "Linux absolute path (rejected as not /dev/)"},
        DevicePathTestCase{"/etc/passwd", true, "Non-device path (rejected)"},
        DevicePathTestCase{"/dev/ttyUSB0", false, "Linux device path"},
        // Invalid
        DevicePathTestCase{"", true, "Empty path"}, DevicePathTestCase{"/dev/bad?", true, "Invalid char ?"}),
    param_name_from_description<DevicePathTestCase>);

struct PortTestCase {
  uint16_t port;
  bool should_throw;
  std::string description;
};

void PrintTo(const PortTestCase& param, std::ostream* os) { *os << param.description; }

class PortParamTest : public ::testing::TestWithParam<PortTestCase> {};

TEST_P(PortParamTest, ValidatePort) {
  const auto& param = GetParam();
  if (param.should_throw) {
    EXPECT_THROW(InputValidator::validate_port(param.port), diagnostics::ValidationException)
        << "Should throw for: " << param.description;
  } else {
    EXPECT_NO_THROW(InputValidator::validate_port(param.port)) << "Should not throw for: " << param.description;
  }
}

INSTANTIATE_TEST_SUITE_P(PortScenarios, PortParamTest,
                         ::testing::Values(PortTestCase{0, true, "Port 0 (invalid)"},
                                           PortTestCase{1, false, "Port 1 (min valid)"},
                                           PortTestCase{65535, false, "Port 65535 (max valid)"},
                                           PortTestCase{8080, false, "Standard port"}),
                         param_name_from_description<PortTestCase>);

TEST(InputValidatorTest, ValidateSecurityExtendedAscii) {
  // Extended ASCII chars (negative when char is signed)
  // \x80 is -128, \xFF is -1

  // Test Device Path with extended ASCII
  // Should reject because it's not alphanumeric (unless it hits UB and misbehaves)
  // Explicitly casting to std::string to ensure null termination isn't an issue if implicitly constructed
  EXPECT_THROW(InputValidator::validate_device_path(std::string("/dev/tty\x80")), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_device_path(std::string("/dev/tty\xFF")), diagnostics::ValidationException);

  // Test Parity with extended ASCII
  // Should reject because it's not "none", "odd", "even"
  // And it calls ::tolower which might be UB
  EXPECT_THROW(InputValidator::validate_parity(std::string("od\x80")), diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_parity(std::string("\xFFven")), diagnostics::ValidationException);
}
