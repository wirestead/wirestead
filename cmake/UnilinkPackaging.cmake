# Unilink packaging configuration This file handles all packaging-related
# settings

# Basic package information. CPACK_PACKAGE_NAME drives the release archive
# filename (wirestead-<version>-<os>-<arch>.tar.gz/.zip) - it does not rename
# the installed library, unilink:: CMake package, or unilink.pc, which stay
# unilink-named per docs/migration-from-unilink.md's compatibility policy.
set(CPACK_PACKAGE_NAME "wirestead")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "Cross-platform async C++ communication library for Serial, TCP, UDP, and UDS"
)
set(CPACK_PACKAGE_DESCRIPTION
    "A cross-platform asynchronous C++ communication library providing a unified API for Serial, TCP, UDP, and UDS transports"
)
set(CPACK_PACKAGE_VENDOR "Jinwoo Sung")
set(CPACK_PACKAGE_CONTACT "jwsung91@gmail.com")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/wirestead/wirestead")

# License and documentation
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_RESOURCE_FILE_WELCOME "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

# Install dirs
include(GNUInstallDirs)

# Package file naming. Normalize architecture to the labels used by release
# assets: x86_64/AMD64/x64 -> amd64, aarch64/arm64 -> arm64.
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _unilink_processor_lower)
if(_unilink_processor_lower MATCHES "x86_64|amd64|x64")
  set(_unilink_arch "amd64")
elseif(_unilink_processor_lower MATCHES "aarch64|arm64")
  set(_unilink_arch "arm64")
else()
  set(_unilink_arch "${_unilink_processor_lower}")
endif()

# UNILINK_OS_LABEL can be passed via -DUNILINK_OS_LABEL=ubuntu-22.04 etc. Falls
# back to cmake system name if not provided.
if(UNILINK_OS_LABEL)
  set(_unilink_os "${UNILINK_OS_LABEL}")
elseif(APPLE)
  set(_unilink_os "macos")
elseif(WIN32)
  set(_unilink_os "windows")
else()
  set(_unilink_os "linux")
endif()

set(CPACK_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${_unilink_os}-${_unilink_arch}"
)
set(CPACK_SOURCE_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-source"
)

# Platform-specific settings
if(WIN32)
  set(CPACK_GENERATOR "ZIP;NSIS;WIX")
  set(CPACK_NSIS_DISPLAY_NAME "Wirestead ${PROJECT_VERSION}")
  set(CPACK_NSIS_PACKAGE_NAME "Wirestead")
  set(CPACK_NSIS_CONTACT "${CPACK_PACKAGE_CONTACT}")
  set(CPACK_NSIS_URL_INFO_ABOUT "${CPACK_PACKAGE_HOMEPAGE_URL}")
  set(CPACK_NSIS_MODIFY_PATH ON)

  # WIX settings
  set(CPACK_WIX_PRODUCT_GUID "12345678-1234-1234-1234-123456789012")
  set(CPACK_WIX_UPGRADE_GUID "87654321-4321-4321-4321-210987654321")

elseif(APPLE)
  set(CPACK_GENERATOR "TGZ;DragNDrop")
  set(CPACK_DMG_VOLUME_NAME "Wirestead ${PROJECT_VERSION}")
  set(CPACK_DMG_FORMAT "UDZO")

elseif(UNIX)
  set(CPACK_GENERATOR "TGZ;DEB;RPM")

  # DEB package settings
  set(CPACK_DEBIAN_PACKAGE_MAINTAINER
      "${CPACK_PACKAGE_VENDOR} <${CPACK_PACKAGE_CONTACT}>"
  )
  set(CPACK_DEBIAN_PACKAGE_SECTION "libs")
  set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
  set(_unilink_debian_package_depends
      "libboost-system-dev (>= ${UNILINK_MIN_BOOST_VERSION})"
  )
  if(NOT UNILINK_SPDLOG_BUNDLED)
    list(APPEND _unilink_debian_package_depends
         "libspdlog-dev (>= ${UNILINK_MIN_SPDLOG_VERSION})"
    )
  endif()
  string(JOIN ", " CPACK_DEBIAN_PACKAGE_DEPENDS
         ${_unilink_debian_package_depends}
  )
  set(CPACK_DEBIAN_PACKAGE_SUGGESTS "cmake, pkg-config")
  set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "libc6-dev")

  # RPM package settings
  set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
  set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
  set(CPACK_RPM_PACKAGE_URL "${CPACK_PACKAGE_HOMEPAGE_URL}")
  set(_unilink_rpm_package_requires
      "boost-system >= ${UNILINK_MIN_BOOST_VERSION}"
  )
  if(NOT UNILINK_SPDLOG_BUNDLED)
    list(APPEND _unilink_rpm_package_requires
         "spdlog-devel >= ${UNILINK_MIN_SPDLOG_VERSION}"
    )
  endif()
  string(JOIN ", " CPACK_RPM_PACKAGE_REQUIRES ${_unilink_rpm_package_requires})
  set(CPACK_RPM_PACKAGE_SUGGESTS "cmake, pkg-config")
  set(CPACK_RPM_PACKAGE_RECOMMENDS "glibc-devel")

  # Set architecture
  if(_unilink_processor_lower MATCHES "x86_64|amd64|x64")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
  elseif(_unilink_processor_lower MATCHES "i[3-6]86")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "i386")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
  elseif(_unilink_processor_lower MATCHES "aarch64|arm64")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "arm64")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "aarch64")
  endif()
endif()

# Source package settings
set(CPACK_SOURCE_GENERATOR "TGZ;ZIP")
set(CPACK_SOURCE_IGNORE_FILES
    "/build/"
    "/.git/"
    "/.gitignore"
    "/.vscode/"
    "/.idea/"
    "*.swp"
    "*.swo"
    "*~"
    "/Testing/"
    "/test/"
    "CMakeCache.txt"
    "CMakeFiles/"
    "Makefile"
    "cmake_install.cmake"
    "install_manifest.txt"
    "CTestTestfile.cmake"
    "_CPack_Packages/"
    "CPackConfig.cmake"
    "CPackSourceConfig.cmake"
)

# Component-based packaging
set(CPACK_COMPONENTS_ALL libraries headers cmake pkgconfig documentation)

# Component descriptions
set(CPACK_COMPONENT_LIBRARIES_DISPLAY_NAME "Libraries")
set(CPACK_COMPONENT_LIBRARIES_DESCRIPTION
    "Wirestead shared and static libraries"
)
set(CPACK_COMPONENT_LIBRARIES_REQUIRED TRUE)

set(CPACK_COMPONENT_HEADERS_DISPLAY_NAME "Header Files")
set(CPACK_COMPONENT_HEADERS_DESCRIPTION "Wirestead C++ header files")
set(CPACK_COMPONENT_HEADERS_REQUIRED TRUE)

set(CPACK_COMPONENT_CMAKE_DISPLAY_NAME "CMake Files")
set(CPACK_COMPONENT_CMAKE_DESCRIPTION
    "CMake configuration files for find_package"
)
set(CPACK_COMPONENT_CMAKE_REQUIRED TRUE)

set(CPACK_COMPONENT_PKGCONFIG_DISPLAY_NAME "pkg-config Files")
set(CPACK_COMPONENT_PKGCONFIG_DESCRIPTION "pkg-config configuration files")
set(CPACK_COMPONENT_PKGCONFIG_DEPENDS libraries)

set(CPACK_COMPONENT_DOCUMENTATION_DISPLAY_NAME "Documentation")
set(CPACK_COMPONENT_DOCUMENTATION_DESCRIPTION
    "Minimal project documentation, license, and notices"
)
set(CPACK_COMPONENT_DOCUMENTATION_DEPENDS headers)

# Package config and pkg-config generation
if(UNILINK_ENABLE_INSTALL)
  include(CMakePackageConfigHelpers)

  configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/UnilinkConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unilink
  )

  write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
  )

  # find_package(wirestead) - see cmake/WiresteadConfig.cmake.in for how this
  # reuses unilink's own config instead of duplicating dependency resolution.
  configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/WiresteadConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wirestead
  )

  write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
  )

  if(UNILINK_ENABLE_PKGCONFIG)
    set(PKGCONFIG_REQUIRES "")
    # Derived from
    # UNILINK_PKGCONFIG_LIBS_PRIVATE/UNILINK_PKGCONFIG_REQUIRES_PRIVATE (built
    # up in UnilinkDependencies.cmake alongside the real
    # target_link_libraries(unilink_dependencies ...) calls) instead of a
    # separately hardcoded list, so this can no longer silently drift out of
    # sync with what unilink_static actually links against.
    list(JOIN UNILINK_PKGCONFIG_LIBS_PRIVATE " " PKGCONFIG_LIBS_PRIVATE)
    list(JOIN UNILINK_PKGCONFIG_REQUIRES_PRIVATE " " PKGCONFIG_REQUIRES_PRIVATE)
    set(PKGCONFIG_CFLAGS "")

    configure_file(
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/unilink.pc.in
      ${CMAKE_CURRENT_BINARY_DIR}/unilink.pc @ONLY
    )

    configure_file(
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/wirestead.pc.in
      ${CMAKE_CURRENT_BINARY_DIR}/wirestead.pc @ONLY
    )
  endif()
endif()

# Install rules for components
if(UNILINK_ENABLE_INSTALL AND TARGET unilink_shared)
  install(
    TARGETS unilink_shared
    EXPORT unilinkTargets
    COMPONENT libraries
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

if(UNILINK_ENABLE_INSTALL AND TARGET unilink_static)
  install(
    TARGETS unilink_static
    EXPORT unilinkTargets
    COMPONENT libraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

if(UNILINK_ENABLE_INSTALL
   AND TARGET unilink
   AND NOT TARGET unilink_shared
   AND NOT TARGET unilink_static
)
  install(
    TARGETS unilink
    EXPORT unilinkTargets
    COMPONENT libraries
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

# Export dependency interface target so install(EXPORT ...) has all required
# targets
if(UNILINK_ENABLE_INSTALL AND TARGET unilink_dependencies)
  install(
    TARGETS unilink_dependencies
    EXPORT unilinkTargets
    COMPONENT cmake
  )
endif()

if(UNILINK_ENABLE_INSTALL)
  install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/unilink/
    COMPONENT headers
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/unilink
    FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "*.h"
  )

  # <wirestead/unilink.hpp> forwarding header - see wirestead/unilink.hpp. Deep
  # <unilink/...> includes have no wirestead equivalent; only the documented
  # public entrypoint does (docs/api_stability.md).
  install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/wirestead/unilink.hpp
    COMPONENT headers
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wirestead
  )

  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfigVersion.cmake
    COMPONENT cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unilink
  )

  install(
    EXPORT unilinkTargets
    COMPONENT cmake
    NAMESPACE unilink::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unilink
  )

  # find_package(wirestead) config - installed alongside, not instead of, the
  # unilink one above. See cmake/WiresteadConfig.cmake.in.
  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfigVersion.cmake
    COMPONENT cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wirestead
  )

  if(UNILINK_ENABLE_PKGCONFIG)
    install(
      FILES ${CMAKE_CURRENT_BINARY_DIR}/unilink.pc
      COMPONENT pkgconfig
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )

    install(
      FILES ${CMAKE_CURRENT_BINARY_DIR}/wirestead.pc
      COMPONENT pkgconfig
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )
  endif()

  install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE ${CMAKE_CURRENT_SOURCE_DIR}/NOTICE
          ${CMAKE_CURRENT_SOURCE_DIR}/README.md
    COMPONENT documentation
    DESTINATION ${CMAKE_INSTALL_DOCDIR}
  )

endif()

# Package versioning
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

# Include CPack
include(CPack)
