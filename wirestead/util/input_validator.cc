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

#include "wirestead/util/input_validator.hpp"

#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <boost/system/error_code.hpp>
#include <string_view>

namespace wirestead {
namespace util {

void InputValidator::validate_host(const std::string& host) {
  validate_non_empty_string(host, "host");
  validate_string_length(host, base::constants::MAX_HOSTNAME_LENGTH, "host");

  if (is_valid_host(host)) {
    return;
  }

  throw diagnostics::ValidationException("invalid host format", "host", "valid IPv4, IPv6, or hostname");
}

void InputValidator::validate_ipv4_address(const std::string& address) {
  validate_non_empty_string(address, "ipv4_address");

  if (!is_valid_ipv4(address)) {
    throw diagnostics::ValidationException("invalid IPv4 address format", "ipv4_address", "valid IPv4 address");
  }
}

void InputValidator::validate_ipv6_address(const std::string& address) {
  validate_non_empty_string(address, "ipv6_address");

  if (!is_valid_ipv6(address)) {
    throw diagnostics::ValidationException("invalid IPv6 address format", "ipv6_address", "valid IPv6 address");
  }
}

void InputValidator::validate_uds_path(const std::string& path) {
  validate_non_empty_string(path, "uds_path");
  validate_string_length(path, base::constants::MAX_UDS_PATH_LENGTH, "uds_path");

  if (!is_valid_uds_path(path)) {
    throw diagnostics::ValidationException("invalid UDS path format", "uds_path", "valid Unix Domain Socket path");
  }
}

void InputValidator::validate_device_path(const std::string& device) {
  validate_non_empty_string(device, "device_path");
  validate_string_length(device, base::constants::MAX_DEVICE_PATH_LENGTH, "device_path");

  if (!is_valid_device_path(device)) {
    throw diagnostics::ValidationException("invalid device path format", "device_path", "valid device path");
  }
}

void InputValidator::validate_parity(const std::string& parity) {
  validate_non_empty_string(parity, "parity");

  // Convert to lowercase for case-insensitive comparison
  std::string lower_parity = parity;
  std::transform(lower_parity.begin(), lower_parity.end(), lower_parity.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (lower_parity != "none" && lower_parity != "odd" && lower_parity != "even") {
    throw diagnostics::ValidationException("invalid parity value", "parity", "none, odd, or even");
  }
}

bool InputValidator::is_valid_host(const std::string& host) {
  // Check if it's an IPv4 address
  if (is_valid_ipv4(host)) {
    return true;
  }

  // Check if it's an IPv6 address
  if (is_valid_ipv6(host)) {
    return true;
  }

  // Check if it's a valid hostname
  if (is_valid_hostname(host)) {
    return true;
  }

  return false;
}

bool InputValidator::is_valid_ipv4(std::string_view address) {
  if (address.empty()) return false;

  // Use Boost.Asio for parsing to ensure standard compliance
  // and robust validation, but enforce strict canonical form
  // to reject ambiguous formats (e.g., octal, hex, whitespace).
  boost::system::error_code ec;

  // Create string copy for compatibility with older Boost versions
  std::string addr_str(address);
  auto ip = boost::asio::ip::make_address_v4(addr_str, ec);

  if (ec) {
    return false;
  }

  // Canonicalization check: The string representation of the parsed IP
  // must match the input exactly. This rejects:
  // - Octal (0127.0.0.1 -> 87.0.0.1 != 0127.0.0.1)
  // - Hex (0x7F000001 -> 127.0.0.1 != 0x7F000001)
  // - Leading/trailing whitespace
  // - Leading zeros in octets (01.1.1.1 -> 1.1.1.1 != 01.1.1.1)
  if (ip.to_string() != addr_str) {
    return false;
  }

  return true;
}

bool InputValidator::is_valid_ipv6(const std::string& address) {
  // Reject addresses containing brackets (e.g. [::1]:80) or port numbers
  // boost::asio::ip::make_address_v6 behavior on Windows regarding this might be permissive
  // or platform-dependent, so we explicitly reject them for consistency.
  if (address.find('[') != std::string::npos || address.find(']') != std::string::npos) {
    return false;
  }

  boost::system::error_code ec;
  boost::asio::ip::make_address_v6(address, ec);
  return !ec;
}

bool InputValidator::is_valid_hostname(std::string_view hostname) {
  // Hostname validation according to RFC 1123
  // - Must not be empty
  // - Must not start or end with hyphen
  // - Must contain only alphanumeric characters and hyphens
  // - Each label must be 1-63 characters
  // - Total length must not exceed 253 characters

  if (hostname.empty() || hostname.length() > base::constants::MAX_HOSTNAME_LENGTH) {
    return false;
  }

  if (hostname.front() == '-' || hostname.back() == '-') {
    return false;
  }

  // Check each label (separated by dots)
  size_t start = 0;
  size_t end = 0;

  while ((end = hostname.find('.', start)) != std::string_view::npos) {
    std::string_view label = hostname.substr(start, end - start);

    if (label.empty() || label.length() > 63) {
      return false;
    }

    if (label.front() == '-' || label.back() == '-') {
      return false;
    }

    // Check if label contains only valid characters
    for (char c : label) {
      if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-') {
        return false;
      }
    }
    start = end + 1;
  }

  // Check last label
  std::string_view label = hostname.substr(start);
  if (label.empty() || label.length() > 63) {
    return false;
  }

  if (label.front() == '-' || label.back() == '-') {
    return false;
  }

  for (char c : label) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-') {
      return false;
    }
  }

  return true;
}

bool InputValidator::is_valid_uds_path(const std::string& path) {
  if (path.empty() || path.length() > base::constants::MAX_UDS_PATH_LENGTH) {
    return false;
  }

  // UDS path should be a valid file system path.
  // For simplicity, we check if it's not empty and doesn't contain null characters.
  // On Linux/Unix, almost any character except null is valid in a filename.
  if (path.find('\0') != std::string::npos) {
    return false;
  }

  return true;
}

bool InputValidator::is_valid_device_path(const std::string& device) {
  // Basic device path validation
  // - Must not be empty
  // - Must start with '/' (Unix-style) or be a COM port (Windows-style)
  // - Must not contain invalid characters

  if (device.empty()) {
    return false;
  }

  // Unix-style device path (e.g., /dev/ttyUSB0, /dev/ttyACM0)
  // Must start with "/dev/" for security
  if (device.length() >= 5 && device.substr(0, 5) == "/dev/") {
    // Check for valid Unix device path characters
    for (char c : device) {
      if (!std::isalnum(static_cast<unsigned char>(c)) && c != '/' && c != '_' && c != '-') {
        return false;
      }
    }
    return true;
  }

  // Windows-style COM port (e.g., COM1, COM2, etc.)
  if (device.length() >= 4 && device.substr(0, 3) == "COM") {
    std::string port_num = device.substr(3);

    // Check if port_num contains only digits
    if (port_num.empty() ||
        !std::all_of(port_num.begin(), port_num.end(), [](unsigned char c) { return std::isdigit(c); })) {
      return false;
    }

    try {
      int port = std::stoi(port_num);
      return port >= 1 && port <= 255;
    } catch (const std::exception&) {
      return false;
    }
  }

  // Windows special device names
  if (device == "NUL" || device == "CON" || device == "PRN" || device == "AUX" || device == "LPT1" ||
      device == "LPT2" || device == "LPT3") {
    return true;
  }

  return false;
}

}  // namespace util
}  // namespace wirestead
