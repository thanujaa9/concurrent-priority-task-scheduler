cmake_minimum_required(VERSION 3.16)
project(multithreaded_task_scheduler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(scheduler src/task_scheduler.cpp)
target_include_directories(scheduler PUBLIC include)
target_link_libraries(scheduler PUBLIC Threads::Threads)
target_compile_options(scheduler PRIVATE -Wall -Wextra -Wpedantic)

add_executable(benchmark src/benchmark.cpp)
target_link_libraries(benchmark PRIVATE scheduler)
target_compile_options(benchmark PRIVATE -Wall -Wextra -Wpedantic)

add_executable(scheduler_tests tests/scheduler_tests.cpp)
target_link_libraries(scheduler_tests PRIVATE scheduler)
target_compile_options(scheduler_tests PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
add_test(NAME scheduler_tests COMMAND scheduler_tests)
