# Wirestead compiler configuration This file handles compiler-specific settings
# and optimizations.
#
# IMPORTANT: Do NOT redefine compiler/toolchain-provided platform/architecture
# macros such as _WIN32, _WIN64, _M_IX86, _M_X64, _M_AMD64, etc. Those macros
# are consumed by the Windows SDK, STL and third-party libraries.

# -----------------------------------------------------------------------------
# Compiler-specific flags
# -----------------------------------------------------------------------------

if(MSVC)
  # Microsoft Visual C++ (including clang-cl)
  set(WIRESTEAD_COMPILER_MSVC ON)

  # Remove any existing /W[0-4] flags the generator might have added so we
  # control warning level explicitly.
  foreach(
    flag_var
    CMAKE_C_FLAGS
    CMAKE_CXX_FLAGS
    CMAKE_C_FLAGS_DEBUG
    CMAKE_CXX_FLAGS_DEBUG
    CMAKE_C_FLAGS_RELEASE
    CMAKE_CXX_FLAGS_RELEASE
    CMAKE_C_FLAGS_RELWITHDEBINFO
    CMAKE_CXX_FLAGS_RELWITHDEBINFO
    CMAKE_C_FLAGS_MINSIZEREL
    CMAKE_CXX_FLAGS_MINSIZEREL
  )
    if(DEFINED ${flag_var})
      string(REGEX REPLACE "(/W[0-4])" "" _cleaned_flags "${${flag_var}}")
      string(REGEX REPLACE "  +" " " _cleaned_flags "${_cleaned_flags}")
      string(STRIP "${_cleaned_flags}" _cleaned_flags)
      set(${flag_var}
          "${_cleaned_flags}"
          CACHE STRING "" FORCE
      )
    endif()
  endforeach()

  if(WIRESTEAD_ENABLE_WARNINGS)
    # /W4: Warning level 4 /permissive-: Standards conformance /utf-8: Set
    # source and execution character sets to UTF-8 /wd4251: Suppress 'needs to
    # have dll-interface' warning for STL members in exported classes /wd4275:
    # Suppress 'non dll-interface class used as base for dll-interface class'
    # (safe for std::runtime_error)
    add_compile_options(/W4 /permissive- /utf-8 /wd4251 /wd4275)
    if(WIRESTEAD_ENABLE_WERROR)
      add_compile_options(/WX)
    endif()
  endif()

  # MSVC-specific optimizations and debug information
  set(CMAKE_CXX_FLAGS_RELEASE
      "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Ob2 /Oi /Ot /Oy /GL /wd4251 /wd4275"
  )
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} /O2 /wd4251 /wd4275"
  )
  set(CMAKE_CXX_FLAGS_MINSIZEREL
      "${CMAKE_CXX_FLAGS_MINSIZEREL} /O1 /Os /wd4251 /wd4275"
  )
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /LTCG")
  set(CMAKE_SHARED_LINKER_FLAGS_RELEASE
      "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} /LTCG"
  )

elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  # GCC or Clang
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(WIRESTEAD_COMPILER_GCC ON)
  else()
    set(WIRESTEAD_COMPILER_CLANG ON)
  endif()

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                              VERSION_LESS "10.0"
  )
    message(FATAL_ERROR "GCC 10.0+ is required for wirestead's C++20 features")
  elseif(WIRESTEAD_COMPILER_CLANG AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS
                                      "14.0"
  )
    message(
      FATAL_ERROR "Clang 14.0+ is required for wirestead's C++20 features"
    )
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_compile_definitions(WIRESTEAD_PLATFORM_MACOS=1)
    message(STATUS "Detected macOS compatibility mode")
    # std::jthread / stop_token / stop_callback are gated on
    # __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ >= 150000 in Apple's libc++.
    # The vcpkg arm64-osx triplet can lower CMAKE_OSX_DEPLOYMENT_TARGET to 11.0
    # via its toolchain; force 15.0 here before any project targets are defined.
    if(NOT CMAKE_OSX_DEPLOYMENT_TARGET VERSION_GREATER_EQUAL "15.0")
      set(CMAKE_OSX_DEPLOYMENT_TARGET
          "15.0"
          CACHE STRING "Minimum macOS deployment target" FORCE
      )
      message(
        STATUS
          "Bumped macOS deployment target to 15.0 (std::jthread requires Apple libc++ macOS 15+)"
      )
    endif()
    # Belt-and-suspenders: directly add the flag so it overrides any earlier
    # -mmacosx-version-min that CMake or vcpkg already wrote into the build
    # rules.
    add_compile_options(-mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET})

    if(WIRESTEAD_COMPILER_CLANG)
      include(CheckCXXSourceCompiles)
      set(_WIRESTEAD_REQUIRED_FLAGS_SAVE "${CMAKE_REQUIRED_FLAGS}")
      set(CMAKE_REQUIRED_FLAGS
          "${CMAKE_REQUIRED_FLAGS} -std=c++20 -fexperimental-library"
      )
      check_cxx_source_compiles(
        "#include <thread>
#include <stop_token>
int main() {
  std::jthread worker([](std::stop_token) {});
  worker.request_stop();
  return 0;
}"
        WIRESTEAD_HAS_FEXPERIMENTAL_LIBRARY
      )
      set(CMAKE_REQUIRED_FLAGS "${_WIRESTEAD_REQUIRED_FLAGS_SAVE}")
      unset(_WIRESTEAD_REQUIRED_FLAGS_SAVE)
      if(WIRESTEAD_HAS_FEXPERIMENTAL_LIBRARY)
        # libc++ still gates P0660 (std::jthread, std::stop_token and
        # std::stop_callback) behind this flag on Apple's Clang toolchain.
        add_compile_options(-fexperimental-library)
        add_link_options(-fexperimental-library)
        message(STATUS "Enabled libc++ experimental C++20 library support")
      else()
        message(
          FATAL_ERROR
            "Configured macOS Clang/libc++ does not provide std::jthread/stop_token support with -fexperimental-library"
        )
      endif()
    endif()
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_compile_definitions(WIRESTEAD_PLATFORM_LINUX=1)
    message(STATUS "Detected Linux compatibility mode")
  else()
    add_compile_definitions(WIRESTEAD_PLATFORM_POSIX=1)
    message(STATUS "Detected generic POSIX compatibility mode")
  endif()

  # Common flags for GCC and Clang
  if(WIRESTEAD_ENABLE_WARNINGS)
    add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)

    # Suppress warnings for older GCC versions (equivalent to Ubuntu 20.04 era
    # compilers)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                VERSION_LESS "11.0"
    )
      add_compile_options(-Wno-deprecated-declarations)
      add_compile_options(-Wno-unused-variable)
      message(STATUS "Applied legacy GCC warning suppressions")
    endif()

    # Additional warnings for Clang
    if(WIRESTEAD_COMPILER_CLANG)
      add_compile_options(
        -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic
        # Suppress noisy warnings from Boost headers
        -Wno-padded -Wno-suggest-override -Wno-suggest-destructor-override
      )
    endif()

    if(WIRESTEAD_ENABLE_WERROR)
      add_compile_options(-Werror)
    endif()
  endif()

  # Visibility settings (disabled for now to avoid linking issues)
  # add_compile_options(-fvisibility=hidden -fvisibility-inlines-hidden)

  # Optimization flags
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -DNDEBUG)
    if(WIRESTEAD_ENABLE_LTO)
      add_compile_options(-flto)
      add_link_options(-flto)
    endif()
  elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0)
  endif()

  # Sanitizer support
  if(WIRESTEAD_ENABLE_SANITIZERS AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(WIRESTEAD_ENABLE_ASAN)
      add_compile_options(-fsanitize=address)
      add_link_options(-fsanitize=address)
    endif()
    if(WIRESTEAD_ENABLE_UBSAN)
      add_compile_options(-fsanitize=undefined)
      add_link_options(-fsanitize=undefined)
    endif()
    if(WIRESTEAD_ENABLE_TSAN)
      add_compile_options(-fsanitize=thread)
      add_link_options(-fsanitize=thread)
    endif()
  endif()

  # Coverage support
  if(WIRESTEAD_ENABLE_COVERAGE)
    add_compile_options(--coverage)
    add_link_options(--coverage)
    add_compile_definitions(WIRESTEAD_COVERAGE_ENABLED=1)
  endif()

else()
  message(WARNING "Unknown compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# -----------------------------------------------------------------------------
# Platform-specific settings
# -----------------------------------------------------------------------------

if(WIN32)
  # Wirestead-private platform/architecture macros. These are safe to define.
  add_compile_definitions(WIRESTEAD_PLATFORM_WINDOWS=1)

  # Architecture detection: - Prefer CMAKE_SYSTEM_PROCESSOR for distinguishing
  # arm64 vs x64. - Fall back to pointer size when processor hint is not
  # specific.
  set(_WIRESTEAD_IS_ARM64 OFF)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "(ARM64|arm64|aarch64)")
    set(_WIRESTEAD_IS_ARM64 ON)
  endif()

  if(_WIRESTEAD_IS_ARM64)
    add_compile_definitions(WIRESTEAD_ARCH_ARM64=1)
  elseif(CMAKE_SIZEOF_VOID_P EQUAL 8)
    add_compile_definitions(WIRESTEAD_ARCH_X64=1)
  elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    add_compile_definitions(WIRESTEAD_ARCH_X86=1)
  endif()

  add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX _WIN32_WINNT=0x0A00)

  if(WIRESTEAD_BUILD_SHARED)
    add_compile_definitions(WIRESTEAD_EXPORTS)
  endif()

elseif(UNIX)
  if(APPLE)
    # Enable full BSD/POSIX feature set on macOS (needed for networking macros
    # like NI_MAXHOST)
    add_compile_definitions(_DARWIN_C_SOURCE)
  else()
    add_compile_definitions(_POSIX_C_SOURCE=200809L)
  endif()
endif()
