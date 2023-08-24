cmake_minimum_required(VERSION 2.8.12)

include(GoogleTest)

file(GLOB_RECURSE LJDB_TEST_SOURCES "${PROJECT_SOURCE_DIR}/tests/*/*test.cpp")

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make check-tests"
# #########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

# #########################################
# "make XYZ_test"
# #########################################
foreach (ljdb_test_source ${LJDB_TEST_SOURCES})
  # Create a human readable name.
  get_filename_component(ljdb_test_filename ${ljdb_test_source} NAME)
  string(REPLACE ".cpp" "" ljdb_test_name ${ljdb_test_filename})

  # Add the test target separately and as part of "make check-tests".
  add_executable(${ljdb_test_name} EXCLUDE_FROM_ALL ${ljdb_test_source})
  add_dependencies(build-tests ${ljdb_test_name})
  add_dependencies(check-tests ${ljdb_test_name})

  gtest_discover_tests(${ljdb_test_name}
          EXTRA_ARGS
          --gtest_color=auto
          --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${ljdb_test_name}.xml
          --gtest_catch_exceptions=0
          DISCOVERY_TIMEOUT 120
          PROPERTIES
          TIMEOUT 120
          )

  target_link_libraries(${ljdb_test_name} ljdb gtest gmock_main gmock snappy)

  # Set test target properties and dependencies.
  set_target_properties(${ljdb_test_name}
          PROPERTIES
          RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
          COMMAND ${ljdb_test_name}
          )
endforeach ()