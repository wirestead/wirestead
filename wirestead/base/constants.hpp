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

#include <cstddef>
#include <cstdint>

namespace wirestead {

/**
 * @brief Action taken by TCP clients when an enabled idle timeout expires.
 *
 * Idle timeout is disabled when the configured timeout is 0ms. This action is
 * only consulted after a positive idle timeout has expired.
 */
enum class IdleTimeoutAction {
  /// Close the stale socket and follow the existing reconnect policy.
  Reconnect,
  /// Close the stale socket and leave the client closed.
  Close,
};

namespace base {

// Network and I/O constants
namespace constants {

// Backpressure strategy
enum class BackpressureStrategy {
  Reliable,  // Queue until hard limit; completeness first (default). For UDP: prevents sender-side queue drops only —
             // no receiver-side flow control (UDP is connectionless).
  BestEffort,  // Reject new ordinary wrapper sends under pressure; preserve accepted data
};

// Backpressure threshold constants
constexpr size_t DEFAULT_THRESHOLD_RELIABLE = 1 * 1024 * 1024;  // 1 MiB (Industry standard)
constexpr size_t DEFAULT_THRESHOLD_BEST_EFFORT = 512 * 1024;    // 512 KiB (Robotics/Real-time)
constexpr size_t DEFAULT_BACKPRESSURE_THRESHOLD = DEFAULT_THRESHOLD_RELIABLE;
constexpr size_t MIN_BACKPRESSURE_THRESHOLD = 1024;       // 1 KiB minimum
constexpr size_t MAX_BACKPRESSURE_THRESHOLD = 100 << 20;  // 100 MiB maximum
constexpr size_t DEFAULT_READ_BUFFER_SIZE = 4096;         // 4 KiB
// Bounds for the per-connection userspace read buffer (the buffer each
// async_read_some() fills, sized by *Config::read_buffer_size). A larger
// buffer means fewer read completions and fewer callback dispatches for bulk
// transfers. The ceiling is far below MAX_SOCKET_BUFFER_SIZE below because
// this buffer is allocated per connection and a server multiplies it by
// max_connections, whereas SO_RCVBUF is charged to the kernel per socket.
constexpr size_t MIN_READ_BUFFER_SIZE = 512;          // 512 B
constexpr size_t MAX_READ_BUFFER_SIZE = 1024 * 1024;  // 1 MiB

// Retry and timeout constants
constexpr unsigned DEFAULT_RETRY_INTERVAL_MS = 1000;      // 1 second
constexpr unsigned MIN_RETRY_INTERVAL_MS = 100;           // 100ms minimum
constexpr unsigned MAX_RETRY_INTERVAL_MS = 300000;        // 5 minutes maximum
constexpr unsigned DEFAULT_CONNECTION_TIMEOUT_MS = 5000;  // 5 seconds
constexpr unsigned MIN_CONNECTION_TIMEOUT_MS = 100;       // 100ms minimum
constexpr unsigned MAX_CONNECTION_TIMEOUT_MS = 300000;    // 5 minutes maximum

// Queue management constants
constexpr int DEFAULT_MAX_RETRIES = -1;  // Unlimited retries
constexpr int MAX_RETRIES_LIMIT = 1000;  // Maximum retry limit

// Socket option constants
constexpr bool DEFAULT_TCP_NO_DELAY = true;  // Disable Nagle's algorithm by default (#429)
constexpr bool DEFAULT_KEEP_ALIVE = false;   // TCP keep-alive off by default

// Buffer size constants
constexpr size_t MAX_BUFFER_SIZE = 64 * 1024 * 1024;  // 64MB maximum buffer size
constexpr size_t MIN_BUFFER_SIZE = 1;                 // 1 byte minimum buffer size
constexpr size_t LARGE_BUFFER_THRESHOLD = 65536;      // 64KB threshold for large buffers
constexpr size_t MIN_SOCKET_BUFFER_SIZE = 1024;       // 1 KiB minimum requested socket buffer
constexpr size_t MAX_SOCKET_BUFFER_SIZE = 256 << 20;  // 256 MiB maximum requested socket buffer

// Connection and session constants
constexpr size_t MAX_MAX_CONNECTIONS = 10000;  // Maximum allowed connections
// #437: a server started with defaults must have some ceiling on total
// memory a large number of slow/malicious clients can consume - 0 (the old
// default) meant unlimited.
constexpr size_t DEFAULT_MAX_CONNECTIONS = 1024;
constexpr size_t DEFAULT_IDLE_TIMEOUT_MS = 0;   // Idle timeout disabled by default
constexpr size_t MIN_IDLE_TIMEOUT_MS = 1;       // 1ms minimum idle timeout when enabled
constexpr size_t MAX_IDLE_TIMEOUT_MS = 300000;  // 5m maximum idle timeout

// Validation constants
constexpr size_t MAX_HOSTNAME_LENGTH = 253;  // Maximum hostname length (RFC 1123)
#if defined(__APPLE__)
constexpr size_t MAX_UDS_PATH_LENGTH = 103;  // macOS sockaddr_un::sun_path is 104 including null terminator
#else
constexpr size_t MAX_UDS_PATH_LENGTH = 107;  // Linux sockaddr_un::sun_path is 108 including null terminator
#endif
constexpr uint32_t MIN_BAUD_RATE = 50;       // Minimum baud rate
constexpr uint32_t MAX_BAUD_RATE = 4000000;  // Maximum baud rate
constexpr uint8_t MIN_DATA_BITS = 5;         // Minimum data bits
constexpr uint8_t MAX_DATA_BITS = 8;         // Maximum data bits
constexpr uint8_t MIN_STOP_BITS = 1;         // Minimum stop bits
constexpr uint8_t MAX_STOP_BITS = 2;         // Maximum stop bits

}  // namespace constants

}  // namespace base

}  // namespace wirestead
