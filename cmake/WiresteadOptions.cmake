# Wirestead build options.

option(WIRESTEAD_BUILD_SHARED "Build shared library" ON)
option(WIRESTEAD_LIMIT_EXPORTED_SYMBOLS
       "Restrict shared library exports to wirestead symbols" ON
)
option(WIRESTEAD_BUILD_STATIC "Build static library" ON)
# OFF by default because a build that does not ask for tests must not need the
# network: WIRESTEAD_BUILD_TESTS pulls GoogleTest through FetchContent, and the
# packaging environments that consume this project build offline. vcpkg, the
# Conan recipe and the ROS integration CI all pass OFF explicitly today, but
# Bloom generates its debian rules without knowing this option exists, so a
# default of ON fails the ROS build farm at configure time. Everything that
# wants tests - CI, the presets, scripts/verify.sh, the release workflow -
# passes ON explicitly.
option(WIRESTEAD_BUILD_TESTS "Build tests" OFF)
option(
  WIRESTEAD_BUILD_DOCS
  "Compatibility option for legacy docs builds. Full docs live in wirestead-docs."
  OFF
)

if(DEFINED BUILD_PYTHON_BINDINGS AND BUILD_PYTHON_BINDINGS)
  message(
    FATAL_ERROR
      "BUILD_PYTHON_BINDINGS has been removed. "
      "Python bindings live at https://github.com/wirestead/wirestead-python."
  )
endif()

if(DEFINED WIRESTEAD_BUILD_EXAMPLES AND WIRESTEAD_BUILD_EXAMPLES)
  message(
    FATAL_ERROR
      "WIRESTEAD_BUILD_EXAMPLES has been removed. "
      "Examples live at https://github.com/wirestead/wirestead-examples."
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

# Off by default on purpose. Turning it on pulls in OpenSSL, which is a cost the
# local and embedded deployments this library targets should not pay to get a
# feature they do not use. See docs/security.md.
option(WIRESTEAD_ENABLE_TLS "Enable optional TLS support for the TCP server"
       OFF
)

option(WIRESTEAD_ENABLE_LTO "Enable Link Time Optimization" OFF)

# Resolved once here so the target helpers can just read it. Asking the
# toolchain beats guessing: it covers MSVC /GL plus /LTCG and GCC/Clang -flto
# behind one property, and reports rather than fails when unavailable.
set(WIRESTEAD_IPO_SUPPORTED OFF)
if(WIRESTEAD_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(
    RESULT WIRESTEAD_IPO_SUPPORTED OUTPUT _wirestead_ipo_error
  )
  if(NOT WIRESTEAD_IPO_SUPPORTED)
    message(
      WARNING "WIRESTEAD_ENABLE_LTO is ON but this toolchain does not support "
              "interprocedural optimization: ${_wirestead_ipo_error}"
    )
  endif()
endif()
option(WIRESTEAD_ENABLE_PCH "Enable Precompiled Headers" OFF)

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
# Debian packaging uses None to preserve externally supplied compiler flags.
if(NOT CMAKE_BUILD_TYPE IN_LIST CMAKE_CONFIGURATION_TYPES
   AND NOT CMAKE_BUILD_TYPE STREQUAL "None"
)
  message(FATAL_ERROR "Invalid build type: ${CMAKE_BUILD_TYPE}. "
                      "Valid options are: ${CMAKE_CONFIGURATION_TYPES};None"
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
