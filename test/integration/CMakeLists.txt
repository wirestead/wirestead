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

# Integration Tests - Component integration tests Each test file is built as a
# separate executable

# TCP integration tests NOTE: test_communication.cc temporarily disabled due to
# last_error_ issues
get_property(_unilink_test_lib GLOBAL PROPERTY UNILINK_TEST_LIB)

function(unilink_add_integration_gtest target source_path labels timeout)
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

  gtest_discover_tests(${target} PROPERTIES ${_test_properties})
endfunction()

foreach(
  test_file
  test_integration.cc
  test_simple_server.cc
  test_client_limit_integration.cc
  test_multithread_backpressure.cc
  test_stop_from_callback.cc
  test_start_stop_stress.cc
  test_stop_contract.cc
  test_dos_protection.cc
  test_tcp_server_chaos.cc
  test_tcp_flood.cc
  test_tcp_abort.cc
  test_tcp_cancel.cc
  test_tcp_nodelay_default.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(timeout 300)
  if(test_name STREQUAL "test_integration")
    set(timeout 600)
  endif()

  unilink_add_integration_gtest(
    run_integration_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/tcp/${test_file}
    "integration_tcp_medium" ${timeout} "tcp_port_allocation"
  )
endforeach()

# Wrapper real-I/O integration tests
foreach(
  test_file
  test_tcp_server_wrapper_lifecycle.cc
  test_tcp_client_wrapper_lifecycle.cc
  test_udp_stop_lifecycle.cc
  test_udp_options.cc
  test_udp_failure.cc
  test_udp_state.cc
  test_udp_callback_race_stress.cc
  test_tcp_client_error_mapping.cc
  test_udp_server_wrapper_transport.cc
  test_udp_server_wrapper_lifecycle.cc
  test_udp_client_wrapper_lifecycle.cc
  test_uds_wrapper_lifecycle.cc
  test_send_contract.cc
  test_server_broadcast_contract.cc
  test_server_broadcast_slow_consumer_contract.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(test_label "integration_wrapper_medium")
  if(test_name MATCHES "tcp")
    set(test_label "integration_wrapper_tcp_medium")
  elseif(test_name MATCHES "udp")
    set(test_label "integration_wrapper_udp_medium")
  elseif(test_name MATCHES "uds")
    set(test_label "integration_wrapper_uds_medium")
  endif()

  set(resource_lock "tcp_port_allocation")
  if(test_name MATCHES "uds")
    set(resource_lock "uds_socket_path_allocation")
  endif()

  unilink_add_integration_gtest(
    run_integration_${test_name}
    ${CMAKE_CURRENT_SOURCE_DIR}/wrapper/${test_file} "${test_label}" 300
    "${resource_lock}"
  )
endforeach()

unilink_add_integration_gtest(
  run_integration_udp_large_payload_receive
  ${CMAKE_CURRENT_SOURCE_DIR}/wrapper/test_udp_large_payload_receive.cc
  "integration_udp_large_payload_diagnostic" 60 "tcp_port_allocation"
)

# Transport real-I/O integration tests
foreach(
  test_file
  transport_tcp_client.cc
  transport_tcp_server.cc
  transport_udp.cc
  transport_udp_extended.cc
  transport_tcp_client_policy.cc
  transport_uds_error_test.cc
  test_tcp_client_lifecycle.cc
  test_backpressure_strategy.cc
  test_try_write_contract.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)

  set(test_label "integration_transport_medium")
  if(test_name MATCHES "backpressure")
    set(test_label "integration_transport_backpressure_medium")
  elseif(test_name MATCHES "tcp")
    set(test_label "integration_transport_tcp_medium")
  elseif(test_name MATCHES "udp")
    set(test_label "integration_transport_udp_medium")
  elseif(test_name MATCHES "uds")
    set(test_label "integration_transport_uds_medium")
  endif()

  set(resource_lock "")
  if(test_name MATCHES "uds")
    set(resource_lock "uds_socket_path_allocation")
  endif()

  unilink_add_integration_gtest(
    run_integration_${test_name}
    ${CMAKE_CURRENT_SOURCE_DIR}/transport/${test_file} "${test_label}" 300
    "${resource_lock}"
  )
endforeach()

unilink_add_integration_gtest(
  run_integration_udp_large_payload_transport
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_udp_large_payload_transport.cc
  "integration_udp_large_payload_diagnostic" 60 "tcp_port_allocation"
)

unilink_add_integration_gtest(
  run_integration_test_contract_compliance
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_contract_compliance.cc
  "integration_transport_contract_medium" 300
)

unilink_add_integration_gtest(
  run_integration_test_tcp_server_connectivity
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/test_tcp_server_connectivity.cc
  "integration_transport_tcp_medium" 300
)

unilink_add_integration_gtest(
  run_integration_transport_tcp_server_security_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_tcp_server_security_test.cc
  "integration_transport_tcp_security_medium" 300
)

unilink_add_integration_gtest(
  run_integration_transport_uds_server_security_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/transport_uds_server_security_test.cc
  "integration_transport_uds_security_medium" 300 "uds_socket_path_allocation"
)

# Serial integration tests NOTE: test_serial.cc temporarily disabled due to
# complex variable naming issues
foreach(test_file test_serial_builder_validation.cc test_serial_loopback.cc
                  test_serial_timeout.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_integration_gtest(
    run_integration_${test_name}
    ${CMAKE_CURRENT_SOURCE_DIR}/serial/${test_file} "integration_serial_medium"
    300 "tcp_port_allocation"
  )
endforeach()

# Builder integration tests
foreach(test_file test_builder_integration.cc)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_integration_gtest(
    run_integration_${test_name}
    ${CMAKE_CURRENT_SOURCE_DIR}/builder/${test_file}
    "integration_builder_medium" 300 "tcp_port_allocation"
  )
endforeach()

# UDS integration tests
foreach(test_file test_uds_integration.cc)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_integration_gtest(
    run_integration_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/uds/${test_file}
    "integration_uds_medium" 300 "uds_socket_path_allocation"
  )
endforeach()

# Core/General integration tests
foreach(test_file test_stable_integration.cc test_core_integrated.cc
                  test_safety_integrated.cc
)
  get_filename_component(test_name ${test_file} NAME_WE)
  unilink_add_integration_gtest(
    run_integration_${test_name} ${CMAKE_CURRENT_SOURCE_DIR}/core/${test_file}
    "integration_core_medium" 600 "tcp_port_allocation"
  )
endforeach()
