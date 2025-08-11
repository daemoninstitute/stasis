cmake_minimum_required(VERSION 3.25)

project(stasis VERSION 0.0.1 LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(STATUS "Clang compiler detected, enabling fuzzing target.")
  add_library(Fuzzing INTERFACE)
  target_compile_options(Fuzzing INTERFACE -g -fsanitize=address,fuzzer-no-link)
  target_link_options(Fuzzing INTERFACE -g -fsanitize=address,fuzzer)
endif()


include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  benchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.9.4.zip
)


add_library(stasis INTERFACE)
target_include_directories(stasis INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_features(stasis INTERFACE cxx_std_23)


add_subdirectory(examples)

enable_testing()
add_subdirectory(tests)
# add_subdirectory(bench)
# add_subdirectory(fuzz)


find_program(CLANGTIDY_EXE clang-tidy)
if(CLANGTIDY_EXE)
  set(LINT_SOURCES
    "${CMAKE_SOURCE_DIR}/include/stasis/stasis.hpp"
    "${CMAKE_SOURCE_DIR}/examples/cli/main.cpp"
    "${CMAKE_SOURCE_DIR}/tests/test_stasis.cpp"
    # "${CMAKE_SOURCE_DIR}/bench/bench_stasis.cpp"
    # "${CMAKE_SOURCE_DIR}/fuzz/fuzz_target.cpp"
  )

  add_custom_target(lint
    COMMAND ${CLANGTIDY_EXE}
            -p=${CMAKE_BINARY_DIR}
            ${LINT_SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running clang-tidy..."
  )
else()
  message(STATUS "clang-tidy not found, 'lint' target will not be available.")
endif()


