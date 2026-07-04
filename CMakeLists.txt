cmake_minimum_required(VERSION 3.12)

project(
  unilink
  VERSION 0.8.8
  DESCRIPTION
    "Unified, cross-platform async C++ library for Serial, TCP, UDP, and UDS"
  HOMEPAGE_URL "https://github.com/jwsung91/unilink"
  LANGUAGES CXX
)

# Include build options
include(${CMAKE_CURRENT_LIST_DIR}/cmake/UnilinkOptions.cmake)

# Include compiler configuration (must remain before any target definitions)
include(${CMAKE_CURRENT_LIST_DIR}/cmake/UnilinkCompiler.cmake)

# Include dependencies
include(${CMAKE_CURRENT_LIST_DIR}/cmake/UnilinkDependencies.cmake)

# Include library sources and target construction
include(${CMAKE_CURRENT_LIST_DIR}/cmake/UnilinkSources.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/cmake/UnilinkTargets.cmake)
unilink_add_library_targets()

# Disable MSVC LTCG for unilink libraries to avoid link.exe access violations.
# This is a known workaround. Future investigation should aim to resolve the
# underlying conflict that causes these violations when LTCG is enabled.
if(MSVC)
  foreach(_target IN LISTS UNILINK_LIBRARY_TARGETS)
    target_compile_options(
      ${_target}
      PRIVATE "$<$<CONFIG:Release>:/GL->" "$<$<CONFIG:RelWithDebInfo>:/GL->"
              "$<$<CONFIG:MinSizeRel>:/GL->"
    )
    set_property(
      TARGET ${_target}
      APPEND_STRING
      PROPERTY LINK_FLAGS_RELEASE " /LTCG:OFF"
    )
    set_property(
      TARGET ${_target}
      APPEND_STRING
      PROPERTY LINK_FLAGS_RELWITHDEBINFO " /LTCG:OFF"
    )
    set_property(
      TARGET ${_target}
      APPEND_STRING
      PROPERTY LINK_FLAGS_MINSIZEREL " /LTCG:OFF"
    )
  endforeach()
endif()

# Export header generation
if(UNILINK_ENABLE_EXPORT_HEADER)
  include(GenerateExportHeader)

  if(UNILINK_EXPORT_HEADER_TARGET)
    generate_export_header(
      ${UNILINK_EXPORT_HEADER_TARGET} EXPORT_FILE_NAME
      "${CMAKE_CURRENT_BINARY_DIR}/unilink_export.hpp" BASE_NAME UNILINK
    )
    target_include_directories(
      ${UNILINK_EXPORT_HEADER_TARGET}
      PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    )
  endif()
endif()

# Testing
if(UNILINK_BUILD_TESTS)
  include(CTest)
  enable_testing()
  add_subdirectory(test)
endif()

add_custom_target(
  docs
  COMMAND
    ${CMAKE_COMMAND} -E echo
    "Documentation generation moved to https://github.com/unilink-lab/unilink-docs"
  COMMENT "Documentation generation moved to unilink-docs"
)

# Include packaging configuration
include(cmake/UnilinkPackaging.cmake)
