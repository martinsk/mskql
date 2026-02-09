cmake_minimum_required(VERSION 3.12)
include(FetchContent)

project(mskql_benchmarks VERSION 0.0.0 LANGUAGES C CXX)

set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON)
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    google_benchmark
    GIT_REPOSITORY https://github.com/CodSpeedHQ/codspeed-cpp
    SOURCE_SUBDIR  google_benchmark
    GIT_TAG        v2.1.0
)
FetchContent_MakeAvailable(google_benchmark)

# Build the mskql C library (everything except main.c and pgwire.c)
set(MSKQL_LIB_SOURCES
    src/database.c
    src/table.c
    src/query.c
    src/row.c
    src/parser.c
    src/index.c
    src/column.c
)

add_library(mskql_lib STATIC ${MSKQL_LIB_SOURCES})
target_compile_options(mskql_lib PRIVATE
    -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE
)
target_include_directories(mskql_lib PUBLIC ${CMAKE_SOURCE_DIR}/src)

# Benchmark executable (C++ wrapper calling into C library)
add_executable(mskql_benchmarks bench/bench_codspeed.cpp)
target_link_libraries(mskql_benchmarks benchmark::benchmark mskql_lib)
