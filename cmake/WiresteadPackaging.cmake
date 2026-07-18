# Wirestead packaging configuration.

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

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_RESOURCE_FILE_WELCOME "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

include(GNUInstallDirs)

string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _wirestead_processor_lower)
if(_wirestead_processor_lower MATCHES "x86_64|amd64|x64")
  set(_wirestead_arch "amd64")
elseif(_wirestead_processor_lower MATCHES "aarch64|arm64")
  set(_wirestead_arch "arm64")
else()
  set(_wirestead_arch "${_wirestead_processor_lower}")
endif()

if(WIRESTEAD_OS_LABEL)
  set(_wirestead_os "${WIRESTEAD_OS_LABEL}")
elseif(UNILINK_OS_LABEL)
  set(_wirestead_os "${UNILINK_OS_LABEL}")
elseif(APPLE)
  set(_wirestead_os "macos")
elseif(WIN32)
  set(_wirestead_os "windows")
else()
  set(_wirestead_os "linux")
endif()

set(CPACK_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${_wirestead_os}-${_wirestead_arch}"
)
set(CPACK_SOURCE_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-source"
)

if(WIN32)
  set(CPACK_GENERATOR "ZIP;NSIS;WIX")
  set(CPACK_NSIS_DISPLAY_NAME "Wirestead ${PROJECT_VERSION}")
  set(CPACK_NSIS_PACKAGE_NAME "Wirestead")
  set(CPACK_NSIS_CONTACT "${CPACK_PACKAGE_CONTACT}")
  set(CPACK_NSIS_URL_INFO_ABOUT "${CPACK_PACKAGE_HOMEPAGE_URL}")
  set(CPACK_NSIS_MODIFY_PATH ON)
  set(CPACK_WIX_PRODUCT_GUID "12345678-1234-1234-1234-123456789012")
  set(CPACK_WIX_UPGRADE_GUID "87654321-4321-4321-4321-210987654321")
elseif(APPLE)
  set(CPACK_GENERATOR "TGZ;DragNDrop")
  set(CPACK_DMG_VOLUME_NAME "Wirestead ${PROJECT_VERSION}")
  set(CPACK_DMG_FORMAT "UDZO")
elseif(UNIX)
  set(CPACK_GENERATOR "TGZ;DEB;RPM")
  set(CPACK_DEBIAN_PACKAGE_MAINTAINER
      "${CPACK_PACKAGE_VENDOR} <${CPACK_PACKAGE_CONTACT}>"
  )
  set(CPACK_DEBIAN_PACKAGE_SECTION "libs")
  set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
  set(_wirestead_debian_package_depends
      "libboost-system-dev (>= ${WIRESTEAD_MIN_BOOST_VERSION})"
  )
  if(NOT WIRESTEAD_SPDLOG_BUNDLED)
    list(APPEND _wirestead_debian_package_depends
         "libspdlog-dev (>= ${WIRESTEAD_MIN_SPDLOG_VERSION})"
    )
  endif()
  string(JOIN ", " CPACK_DEBIAN_PACKAGE_DEPENDS
         ${_wirestead_debian_package_depends}
  )
  set(CPACK_DEBIAN_PACKAGE_SUGGESTS "cmake, pkg-config")
  set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "libc6-dev")
  set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
  set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
  set(CPACK_RPM_PACKAGE_URL "${CPACK_PACKAGE_HOMEPAGE_URL}")
  set(_wirestead_rpm_package_requires
      "boost-system >= ${WIRESTEAD_MIN_BOOST_VERSION}"
  )
  if(NOT WIRESTEAD_SPDLOG_BUNDLED)
    list(APPEND _wirestead_rpm_package_requires
         "spdlog-devel >= ${WIRESTEAD_MIN_SPDLOG_VERSION}"
    )
  endif()
  string(JOIN ", " CPACK_RPM_PACKAGE_REQUIRES
         ${_wirestead_rpm_package_requires}
  )
  set(CPACK_RPM_PACKAGE_SUGGESTS "cmake, pkg-config")
  set(CPACK_RPM_PACKAGE_RECOMMENDS "glibc-devel")

  if(_wirestead_processor_lower MATCHES "x86_64|amd64|x64")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
  elseif(_wirestead_processor_lower MATCHES "i[3-6]86")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "i386")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
  elseif(_wirestead_processor_lower MATCHES "aarch64|arm64")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "arm64")
    set(CPACK_RPM_PACKAGE_ARCHITECTURE "aarch64")
  endif()
endif()

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

set(CPACK_COMPONENTS_ALL libraries headers cmake pkgconfig documentation)
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

if(WIRESTEAD_ENABLE_INSTALL)
  include(CMakePackageConfigHelpers)

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

  if(WIRESTEAD_ENABLE_PKGCONFIG)
    set(PKGCONFIG_REQUIRES "")
    list(JOIN WIRESTEAD_PKGCONFIG_LIBS_PRIVATE " " PKGCONFIG_LIBS_PRIVATE)
    list(JOIN WIRESTEAD_PKGCONFIG_REQUIRES_PRIVATE " "
         PKGCONFIG_REQUIRES_PRIVATE
    )
    set(PKGCONFIG_CFLAGS "")
    configure_file(
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/wirestead.pc.in
      ${CMAKE_CURRENT_BINARY_DIR}/wirestead.pc @ONLY
    )
    configure_file(
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/unilink.pc.in
      ${CMAKE_CURRENT_BINARY_DIR}/unilink.pc @ONLY
    )
  endif()
endif()

if(WIRESTEAD_ENABLE_INSTALL AND TARGET wirestead_shared)
  install(
    TARGETS wirestead_shared
    EXPORT wiresteadTargets
    COMPONENT libraries
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

if(WIRESTEAD_ENABLE_INSTALL AND TARGET wirestead_static)
  install(
    TARGETS wirestead_static
    EXPORT wiresteadTargets
    COMPONENT libraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

if(WIRESTEAD_ENABLE_INSTALL
   AND TARGET wirestead
   AND NOT TARGET wirestead_shared
   AND NOT TARGET wirestead_static
)
  install(
    TARGETS wirestead
    EXPORT wiresteadTargets
    COMPONENT libraries
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

if(WIRESTEAD_ENABLE_INSTALL AND TARGET wirestead_dependencies)
  install(
    TARGETS wirestead_dependencies
    EXPORT wiresteadTargets
    COMPONENT cmake
  )
endif()

if(WIRESTEAD_ENABLE_INSTALL)
  install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/wirestead/
    COMPONENT headers
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wirestead
    FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "*.h"
  )

  install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/unilink/
    COMPONENT headers
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/unilink
    FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "*.h"
  )

  if(WIRESTEAD_ENABLE_EXPORT_HEADER)
    install(
      FILES ${CMAKE_CURRENT_BINARY_DIR}/wirestead_export.hpp
      COMPONENT headers
      DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
  endif()

  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/wiresteadConfigVersion.cmake
    COMPONENT cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wirestead
  )

  install(
    EXPORT wiresteadTargets
    COMPONENT cmake
    NAMESPACE wirestead::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wirestead
  )

  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/unilinkConfigVersion.cmake
    COMPONENT cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unilink
  )

  if(WIRESTEAD_ENABLE_PKGCONFIG)
    install(
      FILES ${CMAKE_CURRENT_BINARY_DIR}/wirestead.pc
      COMPONENT pkgconfig
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )
    install(
      FILES ${CMAKE_CURRENT_BINARY_DIR}/unilink.pc
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

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

include(CPack)
