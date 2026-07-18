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

# End-to-End Tests - Full system tests Each test file is built as a separate
# executable

# Scenario tests
get_property(_unilink_test_lib GLOBAL PROPERTY UNILINK_TEST_LIB)

foreach(test_file test_architecture.cc test_error_recovery.cc)
  get_filename_component(test_name ${test_file} NAME_WE)
  add_executable(run_e2e_${test_name} scenarios/${test_file})
  target_link_libraries(
    run_e2e_${test_name} PRIVATE ${_unilink_test_lib} GTest::gtest GTest::gmock
  )
  unilink_set_test_warning_options(run_e2e_${test_name})
  target_include_directories(
    run_e2e_${test_name} PRIVATE ${CMAKE_SOURCE_DIR}/test/utils
                                 ${CMAKE_SOURCE_DIR}/test/fixtures
  )
  gtest_discover_tests(
    run_e2e_${test_name}
    DISCOVERY_TIMEOUT 60
    PROPERTIES LABELS "e2e_scenario_slow"
    TIMEOUT 90 RESOURCE_LOCK "tcp_port_allocation"
  )
endforeach()

# Stress tests
add_executable(run_e2e_test_stress stress/test_stress.cc)
target_link_libraries(
  run_e2e_test_stress PRIVATE ${_unilink_test_lib} GTest::gtest
                              GTest::gtest_main GTest::gmock
)
unilink_set_test_warning_options(run_e2e_test_stress)
target_include_directories(
  run_e2e_test_stress PRIVATE ${CMAKE_SOURCE_DIR}/test/utils
                              ${CMAKE_SOURCE_DIR}/test/fixtures
)
gtest_discover_tests(
  run_e2e_test_stress
  DISCOVERY_TIMEOUT 60
  PROPERTIES LABELS "e2e_stress_slow"
  TIMEOUT 120 RESOURCE_LOCK "tcp_port_allocation"
)
