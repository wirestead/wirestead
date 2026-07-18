# Wirestead library target construction and shared target utilities.

function(wirestead_configure_library_target target)
  target_link_libraries(${target} PUBLIC wirestead_dependencies)
  target_compile_features(${target} PUBLIC cxx_std_20)
  target_include_directories(
    ${target} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                     $<INSTALL_INTERFACE:include>
  )
endfunction()

function(wirestead_configure_shared_target target output_name)
  set_target_properties(
    ${target}
    PROPERTIES VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR}
               OUTPUT_NAME "${output_name}"
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
               WINDOWS_EXPORT_ALL_SYMBOLS OFF
  )
  target_compile_definitions(
    ${target}
    PUBLIC WIRESTEAD_BUILD_SHARED UNILINK_BUILD_SHARED
    PRIVATE WIRESTEAD_BUILDING_LIBRARY UNILINK_BUILDING_LIBRARY
  )
  wirestead_configure_library_target(${target})
endfunction()

function(wirestead_configure_static_target target output_name)
  set_target_properties(
    ${target}
    PROPERTIES OUTPUT_NAME "${output_name}"
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
  )
  target_compile_definitions(
    ${target} PUBLIC WIRESTEAD_BUILD_STATIC UNILINK_BUILD_STATIC
  )
  wirestead_configure_library_target(${target})
endfunction()

function(wirestead_add_library_targets)
  set(_wirestead_library_targets)
  set(_wirestead_export_header_target)
  set(_wirestead_shared_library_target)

  if(WIRESTEAD_BUILD_SHARED AND WIRESTEAD_BUILD_STATIC)
    add_library(
      wirestead_shared SHARED ${WIRESTEAD_SOURCES} ${WIRESTEAD_HEADERS}
    )
    add_library(
      wirestead_static STATIC ${WIRESTEAD_SOURCES} ${WIRESTEAD_HEADERS}
    )

    wirestead_configure_shared_target(wirestead_shared "wirestead")
    wirestead_configure_static_target(wirestead_static "wirestead_static")

    add_library(wirestead INTERFACE)
    target_link_libraries(
      wirestead
      INTERFACE $<$<TARGET_EXISTS:wirestead_shared>:wirestead_shared>
                $<$<NOT:$<TARGET_EXISTS:wirestead_shared>>:wirestead_static>
                wirestead_dependencies
    )

    list(APPEND _wirestead_library_targets wirestead_shared wirestead_static)
    set(_wirestead_export_header_target wirestead_shared)
    set(_wirestead_shared_library_target wirestead_shared)

  elseif(WIRESTEAD_BUILD_SHARED)
    add_library(wirestead SHARED ${WIRESTEAD_SOURCES} ${WIRESTEAD_HEADERS})
    wirestead_configure_shared_target(wirestead "wirestead")

    list(APPEND _wirestead_library_targets wirestead)
    set(_wirestead_export_header_target wirestead)
    set(_wirestead_shared_library_target wirestead)

  elseif(WIRESTEAD_BUILD_STATIC)
    add_library(wirestead STATIC ${WIRESTEAD_SOURCES} ${WIRESTEAD_HEADERS})
    wirestead_configure_static_target(wirestead "wirestead")

    list(APPEND _wirestead_library_targets wirestead)
    set(_wirestead_export_header_target wirestead)

  else()
    message(
      FATAL_ERROR "At least one library type (shared or static) must be enabled"
    )
  endif()

  if(TARGET wirestead_shared)
    add_library(wirestead::wirestead_shared ALIAS wirestead_shared)
    add_library(unilink_shared ALIAS wirestead_shared)
    add_library(unilink::unilink_shared ALIAS wirestead_shared)
  endif()
  if(TARGET wirestead_static)
    add_library(wirestead::wirestead_static ALIAS wirestead_static)
    add_library(unilink_static ALIAS wirestead_static)
    add_library(unilink::unilink_static ALIAS wirestead_static)
  endif()
  if(TARGET wirestead)
    add_library(wirestead::wirestead ALIAS wirestead)
    add_library(unilink ALIAS wirestead)
    add_library(unilink::unilink ALIAS wirestead)
  endif()

  set(WIRESTEAD_LIBRARY_TARGETS
      ${_wirestead_library_targets}
      PARENT_SCOPE
  )
  set(WIRESTEAD_EXPORT_HEADER_TARGET
      ${_wirestead_export_header_target}
      PARENT_SCOPE
  )
  set(WIRESTEAD_SHARED_LIBRARY_TARGET
      ${_wirestead_shared_library_target}
      PARENT_SCOPE
  )
endfunction()

function(wirestead_configure_executable target)
  if(WIN32
     AND WIRESTEAD_BUILD_SHARED
     AND WIRESTEAD_SHARED_LIBRARY_TARGET
  )
    add_custom_command(
      TARGET ${target}
      POST_BUILD
      COMMAND
        ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:${WIRESTEAD_SHARED_LIBRARY_TARGET}>
        $<TARGET_FILE_DIR:${target}>
      COMMENT
        "Copying ${WIRESTEAD_SHARED_LIBRARY_TARGET} runtime to ${target} output directory"
    )
  endif()
endfunction()
