# Copyright 2025 Jinwoo Sung
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Unit Tests - Fast, isolated tests Each test file is built as a separate
# executable due to having its own main()

get_property(_unilink_test_lib GLOBAL PROPERTY UNILINK_TEST_LIB)
find_package(
  Python3
  COMPONENTS Interpreter
  REQUIRED
)

# PRE_TEST (not the default POST_BUILD): running --gtest_list_tests for many
# targets concurrently as part of the parallel build intermittently crashed
# discovery on macOS CI, non-deterministically hitting a different target each
# run and independent of DISCOVERY_TIMEOUT. PRE_TEST defers discovery to
# ctest-time, after the build has finished.
set(_UNILINK_GTEST_DISCOVERY_MODE PRE_TEST)

set(_unilink_unit_test_targets)

function(unilink_add_unit_gtest target source_path labels timeout)
  add_executable(${target} ${source_path})
  target_link_libraries(
    ${target} PRIVATE ${_unilink_test_lib} GTest::gtest GTest::gtest_main
                      GTest::gmock
  )
  unilink_set_test_warning_options(${target})
  target_include_directories(
    ${target} PRIVATE ${CMAKE_SOURCE_DIR}/test/utils
                      ${CMAKE_SOURCE_DIR}/test/fixtures
  )

  set(_test_properties LABELS "${labels}" TIMEOUT ${timeout})
  if(DEFINED ARGV4 AND NOT "${ARGV4}" STREQUAL "")
    list(APPEND _test_properties RESOURCE_LOCK "${ARGV4}")
  endif()

  gtest_discover_tests(
    ${target} DISCOVERY_TIMEOUT 60 DISCOVERY_MODE
    ${_UNILINK_GTEST_DISCOVERY_MODE} PROPERTIES ${_test_properties}
  )

  list(APPEND _unilink_unit_test_targets ${target})
  set(_unilink_unit_test_targets
      "${_unilink_unit_test_targets}"
      PARENT_SCOPE
  )
endfunction()

foreach(
  test_file
  test_core.cc
  test_memory.cc
  test_pooled_buffer.cc
  test_boundary.cc
  test_error_handler.cc
  test_input_validator.cc
  test_input_validator_edge.cc
  test_logger_behaviors.cc
  test_memory_tracker_and_buffer.cc
  test_pool_stress.cc
  test_io_context_manager_lifecycle.cc
  test_error_mapping.cc
  test_error_mapping_exhaustive.cc
  test_error_codes_to_string.cc
  test_error_mapping_edge_cases.cc
  test_memory_pool_edge_cases.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(resource_lock "")
  if(test_name STREQUAL "test_core")
    set(resource_lock "log_file_io")
  endif()
  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/common/${test_file}
    "unit_common_fast" 30 "${resource_lock}"
  )
endforeach()

# Builder tests (separate executables)
foreach(test_file test_builder.cc test_udp_builder_options.cc
                  test_uds_builder_options.cc test_builder_option_wiring.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/builder/${test_file}
    "unit_builder_fast" 30
  )
endforeach()

# Config tests (separate executables)
foreach(test_file test_config.cc test_config_security.cc
                  test_config_failure.cpp
)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/config/${test_file}
    "unit_config_fast" 30
  )
endforeach()

# Memory tests (separate executables)
foreach(test_file test_pool_limits.cc)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/memory/${test_file}
    "unit_memory_fast" 30
  )
endforeach()

# Diagnostics tests
unilink_add_unit_gtest(
  run_unit_test_runtime_stats_counter
  ${CMAKE_CURRENT_SOURCE_DIR}/diagnostics/test_runtime_stats_counter.cc
  "unit_diagnostics_fast" 30
)

# Wrapper tests (separate executables across TCP/UDP/UDS/Serial)
foreach(
  test_file
  test_serial_wrapper_lifecycle.cc test_serial_wrapper_options.cc
  test_runtime_stats.cc test_send_buffer_apis.cc test_error_context_builder.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(test_label "unit_wrapper")
  if(test_name MATCHES "tcp")
    set(test_label "unit_wrapper_tcp")
  elseif(test_name MATCHES "udp")
    set(test_label "unit_wrapper_udp")
  elseif(test_name MATCHES "uds")
    set(test_label "unit_wrapper_uds")
  elseif(test_name MATCHES "serial")
    set(test_label "unit_wrapper_serial")
  endif()

  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/wrapper/${test_file}
    "${test_label}" 120
  )
endforeach()

# Wrapper serial config mapping
unilink_add_unit_gtest(
  run_unit_wrapper_serial_config_mapping
  ${CMAKE_CURRENT_SOURCE_DIR}/wrapper/test_serial_config_mapping.cc
  "unit_wrapper_serial" 30
)

# Transport tests (fast, unit-level without real network dependencies) Updated
# list to include fuzz tests, server session tests, and timeout tests
foreach(test_file transport_uds_client.cc transport_uds_server.cc
                  transport_serial.cc transport_uds_session_lifecycle.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(test_label "unit_transport_fast")
  if(test_name MATCHES "tcp")
    set(test_label "unit_transport_tcp_fast")
  elseif(test_name MATCHES "udp")
    set(test_label "unit_transport_udp_fast")
  elseif(test_name MATCHES "uds")
    set(test_label "unit_transport_uds_fast")
  elseif(test_name MATCHES "serial")
    set(test_label "unit_transport_serial_fast")
  endif()
  if(test_name MATCHES "reconnect")
    set(test_label "unit_transport_reconnect_fast")
  endif()
  if(test_name MATCHES "backpressure")
    set(test_label "unit_transport_backpressure_fast")
  endif()

  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/transport/${test_file}
    "${test_label}" 30
  )
endforeach()

# Deterministic reconnect decision tests (policy/max-retries/clamp logic)
unilink_add_unit_gtest(
  run_unit_test_reconnect_decider
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_reconnect_decider.cc
  "unit_transport_reconnect_stable" 30
)

# Real-socket TcpClient reconnect behavior (server restart, live retry_interval
# clamping - #432). Uses real sockets/timing so it's not in the fast foreach
# above; was previously written but never wired into the build (added #332,
# orphaned since).
unilink_add_unit_gtest(
  run_unit_transport_tcp_client_reconnect_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_tcp_client_reconnect_test.cc
  "unit_transport_reconnect_stable" 30
)

# Shared backpressure state machine (#434) - isolated, no sockets/io_context
unilink_add_unit_gtest(
  run_unit_test_bp_state_machine
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_bp_state_machine.cc
  "unit_transport_backpressure_fast" 30
)

# Shared error-info holder (#445) - isolated, no sockets/io_context
unilink_add_unit_gtest(
  run_unit_test_error_info_holder
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_error_info_holder.cc
  "unit_transport_fast" 30
)

# New error handling tests
unilink_add_unit_gtest(
  run_unit_test_tcp_abort
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_tcp_abort.cc
  "unit_transport_tcp_error" 30
)

unilink_add_unit_gtest(
  run_unit_test_udp_error
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_udp_error.cc
  "unit_transport_udp_error" 30
)

unilink_add_unit_gtest(
  run_unit_test_serial_fail
  ${CMAKE_CURRENT_SOURCE_DIR}/wrapper/test_serial_fail.cc
  "unit_wrapper_serial_error" 30
)

# Transport TCP Server Session Test
unilink_add_unit_gtest(
  run_unit_transport_tcp_server_session
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_tcp_server_session.cc
  "unit_transport_tcp_session" 30
)

# Transport TCP Fuzz Test
unilink_add_unit_gtest(
  run_unit_transport_tcp_fuzz_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_tcp_fuzz_test.cc
  "unit_transport_tcp_fuzz" 30
)

# Transport TCP Timeout Test
unilink_add_unit_gtest(
  run_unit_transport_tcp_timeout_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_tcp_timeout_test.cc
  "unit_transport_tcp_timeout" 30
)

# Framer tests (separate executables)
foreach(test_file test_line_framer.cc test_packet_framer.cc)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_unit_gtest(
    run_unit_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/framer/${test_file}
    "unit_framer" 30
  )
endforeach()

add_custom_target(unilink_unit_tests)
add_dependencies(unilink_unit_tests ${_unilink_unit_test_targets})
