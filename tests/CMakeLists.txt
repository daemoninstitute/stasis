set(gtest_build_tests OFF)

FetchContent_MakeAvailable(googletest)

add_executable(unit_tests test_stasis.cpp)

target_link_libraries(unit_tests
  PRIVATE
    stasis
    GTest::gmock_main
)

include(GoogleTest)
gtest_discover_tests(unit_tests)
