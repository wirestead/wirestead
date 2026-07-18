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

#include <string>

#include "visibility.hpp"

#if !defined(WIRESTEAD_PLATFORM_WINDOWS) && defined(_WIN32)
#define WIRESTEAD_PLATFORM_WINDOWS 1
#endif

#if !defined(WIRESTEAD_ARCH_X64) && !defined(WIRESTEAD_ARCH_X86) && !defined(WIRESTEAD_ARCH_ARM64)
#if defined(_M_ARM64) || defined(__aarch64__)
#define WIRESTEAD_ARCH_ARM64 1
#elif defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__) || defined(__amd64__)
#define WIRESTEAD_ARCH_X64 1
#elif defined(_M_IX86) || defined(__i386__)
#define WIRESTEAD_ARCH_X86 1
#endif
#endif

#if defined(WIRESTEAD_PLATFORM_WINDOWS)
#if !defined(_AMD64_) && (defined(_M_AMD64) || defined(_M_X64))
#define _AMD64_ 1
#endif
#if !defined(_X86_) && defined(_M_IX86)
#define _X86_ 1
#endif
#if !defined(_ARM64_) && defined(_M_ARM64)
#define _ARM64_ 1
#endif
#ifndef BOOST_ASIO_DISABLE_WINDOWS_OBJECT_HANDLE
#define BOOST_ASIO_DISABLE_WINDOWS_OBJECT_HANDLE
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <threadpoolapiset.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

// Ensure fundamental Windows callback aliases exist even when lean headers are
// used. Some SDK combinations omit these when WIN32_LEAN_AND_MEAN is defined.
#ifndef CALLBACK
#define CALLBACK __stdcall
#endif
#ifdef BOOST_ASIO_HAS_WINDOWS_OBJECT_HANDLE
#undef BOOST_ASIO_HAS_WINDOWS_OBJECT_HANDLE
#endif
#ifndef VOID
typedef void VOID;
#endif
#ifndef PVOID
typedef void* PVOID;
#endif
#ifndef BOOLEAN
typedef unsigned char BOOLEAN;
#endif
#endif  // defined(WIRESTEAD_PLATFORM_WINDOWS)

namespace wirestead {
namespace base {

/**
 * @brief Platform detection and feature availability
 */

// Platform detection macros (set by CMake or inferred from compiler)
#if defined(WIRESTEAD_PLATFORM_WINDOWS)
#define WIRESTEAD_FEATURE_LEVEL 3  // Windows matches standard feature set
#elif defined(WIRESTEAD_PLATFORM_MACOS) || defined(__APPLE__)
#define WIRESTEAD_PLATFORM_MACOS 1
#define WIRESTEAD_FEATURE_LEVEL 3  // macOS matches standard feature set
#elif defined(WIRESTEAD_PLATFORM_POSIX)
#define WIRESTEAD_FEATURE_LEVEL 3
#else
#define WIRESTEAD_FEATURE_LEVEL 3  // Default to standard
#endif

// Feature availability macros
#define WIRESTEAD_ENABLE_ADVANCED_LOGGING (WIRESTEAD_FEATURE_LEVEL >= 2)
#define WIRESTEAD_ENABLE_PERFORMANCE_MONITORING (WIRESTEAD_FEATURE_LEVEL >= 2)
#define WIRESTEAD_ENABLE_LATEST_OPTIMIZATIONS (WIRESTEAD_FEATURE_LEVEL >= 3)
#define WIRESTEAD_ENABLE_EXPERIMENTAL_FEATURES (WIRESTEAD_FEATURE_LEVEL >= 3)

/**
 * @brief Platform information utilities
 */
class WIRESTEAD_API PlatformInfo {
 public:
  /**
   * @brief Get the feature level
   * @return Feature level (1=basic, 2=standard, 3=all)
   */
  static int get_feature_level() { return WIRESTEAD_FEATURE_LEVEL; }

  /**
   * @brief Get a human-readable platform description
   * @return Platform description string
   */
  static std::string get_platform_description() {
#if defined(WIRESTEAD_PLATFORM_WINDOWS)
    return "Windows (Full Features)";
#elif defined(WIRESTEAD_PLATFORM_MACOS)
    return "macOS (Full Features)";
#elif defined(WIRESTEAD_PLATFORM_POSIX)
    return "POSIX Platform (Full Features)";
#else
    return "Unknown Platform";
#endif
  }

  /**
   * @brief Check if advanced logging is available
   * @return true if advanced logging is available
   */
  static bool is_advanced_logging_available() { return WIRESTEAD_ENABLE_ADVANCED_LOGGING; }

  /**
   * @brief Check if performance monitoring is available
   * @return true if performance monitoring is available
   */
  static bool is_performance_monitoring_available() { return WIRESTEAD_ENABLE_PERFORMANCE_MONITORING; }

  /**
   * @brief Check if latest optimizations are available
   * @return true if latest optimizations are available
   */
  static bool is_latest_optimizations_available() { return WIRESTEAD_ENABLE_LATEST_OPTIMIZATIONS; }

  /**
   * @brief Check if experimental features are available
   * @return true if experimental features are available
   */
  static bool is_experimental_features_available() { return WIRESTEAD_ENABLE_EXPERIMENTAL_FEATURES; }

  /**
   * @brief Get a warning message for limited support platforms
   * @return Warning message or empty string
   */
  static std::string get_support_warning() { return ""; }
};

}  // namespace base

}  // namespace wirestead
