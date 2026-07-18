# Unilink build options This file centralizes all build options for better
# maintainability

# WIRESTEAD_<X> mirrors UNILINK_<X> for every build option below (see the
# reconciliation loop after they're all declared). Capture which of each pair
# the user explicitly set - via -D or an existing cache entry - *before* any
# option() calls run, since DEFINED becomes true for every option() variable
# afterward regardless of how it ended up with its value. See
# docs/migration-from-unilink.md for the conflict-handling policy this
# implements.
set(_unilink_option_suffixes
    BUILD_SHARED
    BUILD_STATIC
    BUILD_TESTS
    BUILD_DOCS
    ENABLE_CONFIG
    ENABLE_MEMORY_TRACKING
    ENABLE_SANITIZERS
    ENABLE_INSTALL
    ENABLE_PKGCONFIG
    ENABLE_EXPORT_HEADER
    ENABLE_WARNINGS
    ENABLE_WERROR
    ENABLE_COVERAGE
    ENABLE_ASAN
    ENABLE_UBSAN
    ENABLE_TSAN
    ENABLE_LTO
    ENABLE_PCH
)
foreach(_suffix IN LISTS _unilink_option_suffixes)
  if(DEFINED UNILINK_${_suffix})
    set(_unilink_explicit_UNILINK_${_suffix} TRUE)
  endif()
  if(DEFINED WIRESTEAD_${_suffix})
    set(_unilink_explicit_WIRESTEAD_${_suffix} TRUE)
  endif()
endforeach()

# Build type options
option(UNILINK_BUILD_SHARED "Build shared library" ON)
option(UNILINK_BUILD_STATIC "Build static library" ON)
option(UNILINK_BUILD_TESTS "Build tests" ON)
option(
  UNILINK_BUILD_DOCS
  "Compatibility option for legacy docs builds. Full docs live in unilink-docs."
  OFF
)

if(DEFINED BUILD_PYTHON_BINDINGS AND BUILD_PYTHON_BINDINGS)
  message(
    FATAL_ERROR
      "BUILD_PYTHON_BINDINGS has been removed. "
      "Python bindings live at https://github.com/unilink-lab/unilink-python."
  )
endif()

if(DEFINED UNILINK_BUILD_EXAMPLES AND UNILINK_BUILD_EXAMPLES)
  message(
    FATAL_ERROR
      "UNILINK_BUILD_EXAMPLES has been removed. "
      "Examples live at https://github.com/unilink-lab/unilink-examples."
  )
endif()

# Feature options
option(UNILINK_ENABLE_CONFIG "Enable configuration management API" ON)
option(UNILINK_ENABLE_MEMORY_TRACKING "Enable memory tracking for debugging"
       OFF
)
option(UNILINK_ENABLE_SANITIZERS "Enable sanitizers in Debug builds" OFF)

# Installation options
option(UNILINK_ENABLE_INSTALL "Enable install/export targets" ON)
option(UNILINK_ENABLE_PKGCONFIG "Install pkg-config file" ON)
option(UNILINK_ENABLE_EXPORT_HEADER "Generate export header" ON)

# Compiler options
option(UNILINK_ENABLE_WARNINGS "Enable compiler warnings" ON)
option(UNILINK_ENABLE_WERROR "Treat warnings as errors" OFF)
option(UNILINK_ENABLE_COVERAGE "Enable code coverage" OFF)

# Platform-specific options
option(UNILINK_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(UNILINK_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(UNILINK_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# Performance options
option(UNILINK_ENABLE_LTO "Enable Link Time Optimization" OFF)
option(UNILINK_ENABLE_PCH "Enable Precompiled Headers" OFF)

# Wirestead option mirrors. For each UNILINK_<X> above, define WIRESTEAD_<X> as
# a visible CACHE option defaulting to the same value, then reconcile: an
# explicit conflict between the two is a hard error (build-time configuration
# should never be silently ambiguous); otherwise whichever one was explicitly
# set wins and is forwarded onto the other, so UNILINK_<X> - which the rest of
# the build reads - is always the effective value either way.
foreach(_suffix IN LISTS _unilink_option_suffixes)
  set(_unilink_var "UNILINK_${_suffix}")
  set(_wirestead_var "WIRESTEAD_${_suffix}")

  option(${_wirestead_var} "Wirestead alias for ${_unilink_var}"
         ${${_unilink_var}}
  )

  if(_unilink_explicit_UNILINK_${_suffix}
     AND _unilink_explicit_WIRESTEAD_${_suffix}
  )
    if(NOT "${${_unilink_var}}" STREQUAL "${${_wirestead_var}}")
      message(
        FATAL_ERROR
          "${_unilink_var}=${${_unilink_var}} conflicts with "
          "${_wirestead_var}=${${_wirestead_var}}. Set only one of them, or "
          "set both to the same value."
      )
    endif()
  elseif(_unilink_explicit_WIRESTEAD_${_suffix})
    set(${_unilink_var}
        ${${_wirestead_var}}
        CACHE BOOL "" FORCE
    )
  endif()
  # else: UNILINK_<X> was explicit-only, or neither was explicit - UNILINK_<X>
  # (read by the rest of the build) is already correct, and WIRESTEAD_<X>
  # already defaulted to match it above.
endforeach()

unset(_unilink_option_suffixes)

# Consolidate build outputs into predictable bin/lib directories so Docker and
# tests can copy artifacts reliably without relying on generator defaults.
if(NOT DEFINED CMAKE_ARCHIVE_OUTPUT_DIRECTORY)
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
endif()
if(NOT DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
endif()
if(NOT DEFINED CMAKE_RUNTIME_OUTPUT_DIRECTORY)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()
if(CMAKE_CONFIGURATION_TYPES)
  foreach(cfg ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER "${cfg}" cfg_upper)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${cfg_upper}
        "${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}"
    )
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${cfg_upper}
        "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
    )
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${cfg_upper}
        "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
    )
  endforeach()
endif()

# Set default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      "Release"
      CACHE STRING "Build type" FORCE
  )
endif()

# Validate build type
set(CMAKE_CONFIGURATION_TYPES "Debug;Release;RelWithDebInfo;MinSizeRel")
if(NOT CMAKE_BUILD_TYPE IN_LIST CMAKE_CONFIGURATION_TYPES)
  message(FATAL_ERROR "Invalid build type: ${CMAKE_BUILD_TYPE}. "
                      "Valid options are: ${CMAKE_CONFIGURATION_TYPES}"
  )
endif()

# Set default C++ standard
set(CMAKE_CXX_STANDARD
    20
    CACHE STRING "C++ standard"
)
set_property(CACHE CMAKE_CXX_STANDARD PROPERTY STRINGS 20 23)
if(CMAKE_CXX_STANDARD LESS 20)
  message(
    FATAL_ERROR
      "unilink requires C++20 or newer. Configure with -DCMAKE_CXX_STANDARD=20."
  )
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable position independent code
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
