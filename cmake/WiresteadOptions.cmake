# Wirestead build options. WIRESTEAD_* is the canonical option surface;
# UNILINK_* is accepted as a v0.9.x compatibility input and forwarded during
# configure.

set(_wirestead_option_suffixes
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

foreach(_suffix IN LISTS _wirestead_option_suffixes)
  if(DEFINED WIRESTEAD_${_suffix})
    set(_wirestead_explicit_wirestead_${_suffix} TRUE)
  endif()
  if(DEFINED UNILINK_${_suffix})
    set(_wirestead_explicit_unilink_${_suffix} TRUE)
  endif()
endforeach()

option(WIRESTEAD_BUILD_SHARED "Build shared library" ON)
option(WIRESTEAD_BUILD_STATIC "Build static library" ON)
option(WIRESTEAD_BUILD_TESTS "Build tests" ON)
option(
  WIRESTEAD_BUILD_DOCS
  "Compatibility option for legacy docs builds. Full docs live in wirestead-docs."
  OFF
)

if(DEFINED BUILD_PYTHON_BINDINGS AND BUILD_PYTHON_BINDINGS)
  message(
    FATAL_ERROR
      "BUILD_PYTHON_BINDINGS has been removed. "
      "Python bindings live at https://github.com/wirestead/unilink-python."
  )
endif()

if(DEFINED WIRESTEAD_BUILD_EXAMPLES AND WIRESTEAD_BUILD_EXAMPLES)
  message(
    FATAL_ERROR
      "WIRESTEAD_BUILD_EXAMPLES has been removed. "
      "Examples live at https://github.com/wirestead/unilink-examples."
  )
endif()

if(DEFINED UNILINK_BUILD_EXAMPLES AND UNILINK_BUILD_EXAMPLES)
  message(
    FATAL_ERROR
      "UNILINK_BUILD_EXAMPLES has been removed. "
      "Examples live at https://github.com/wirestead/unilink-examples."
  )
endif()

option(WIRESTEAD_ENABLE_CONFIG "Enable configuration management API" ON)
option(WIRESTEAD_ENABLE_MEMORY_TRACKING "Enable memory tracking for debugging"
       OFF
)
option(WIRESTEAD_ENABLE_SANITIZERS "Enable sanitizers in Debug builds" OFF)

option(WIRESTEAD_ENABLE_INSTALL "Enable install/export targets" ON)
option(WIRESTEAD_ENABLE_PKGCONFIG "Install pkg-config file" ON)
option(WIRESTEAD_ENABLE_EXPORT_HEADER "Generate export header" ON)

option(WIRESTEAD_ENABLE_WARNINGS "Enable compiler warnings" ON)
option(WIRESTEAD_ENABLE_WERROR "Treat warnings as errors" OFF)
option(WIRESTEAD_ENABLE_COVERAGE "Enable code coverage" OFF)

option(WIRESTEAD_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(WIRESTEAD_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(WIRESTEAD_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

option(WIRESTEAD_ENABLE_LTO "Enable Link Time Optimization" OFF)
option(WIRESTEAD_ENABLE_PCH "Enable Precompiled Headers" OFF)

foreach(_suffix IN LISTS _wirestead_option_suffixes)
  set(_wirestead_var "WIRESTEAD_${_suffix}")
  set(_unilink_var "UNILINK_${_suffix}")

  if(_wirestead_explicit_wirestead_${_suffix}
     AND _wirestead_explicit_unilink_${_suffix}
  )
    if(NOT "${${_wirestead_var}}" STREQUAL "${${_unilink_var}}")
      message(
        FATAL_ERROR
          "${_unilink_var}=${${_unilink_var}} conflicts with "
          "${_wirestead_var}=${${_wirestead_var}}. Set only one of them, or "
          "set both to the same value."
      )
    endif()
  elseif(_wirestead_explicit_unilink_${_suffix})
    set(${_wirestead_var}
        ${${_unilink_var}}
        CACHE BOOL "" FORCE
    )
  endif()

  set(${_unilink_var}
      ${${_wirestead_var}}
      CACHE BOOL "Legacy alias for ${_wirestead_var}" FORCE
  )
endforeach()

unset(_wirestead_option_suffixes)

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

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      "Release"
      CACHE STRING "Build type" FORCE
  )
endif()

set(CMAKE_CONFIGURATION_TYPES "Debug;Release;RelWithDebInfo;MinSizeRel")
if(NOT CMAKE_BUILD_TYPE IN_LIST CMAKE_CONFIGURATION_TYPES)
  message(FATAL_ERROR "Invalid build type: ${CMAKE_BUILD_TYPE}. "
                      "Valid options are: ${CMAKE_CONFIGURATION_TYPES}"
  )
endif()

set(CMAKE_CXX_STANDARD
    20
    CACHE STRING "C++ standard"
)
set_property(CACHE CMAKE_CXX_STANDARD PROPERTY STRINGS 20 23)
if(CMAKE_CXX_STANDARD LESS 20)
  message(
    FATAL_ERROR
      "wirestead requires C++20 or newer. Configure with -DCMAKE_CXX_STANDARD=20."
  )
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
