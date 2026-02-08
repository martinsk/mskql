cmake_minimum_required(VERSION 3.12)
project(mskql VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wswitch-enum -Wpedantic")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Source files for the mskql library
set(MSKQL_SOURCES
    src/database.c
    src/table.c
    src/query.c
    src/row.c
    src/parser.c
    src/pgwire.c
    src/index.c
    src/column.c
)

set(MSKQL_HEADERS
    src/database.h
    src/table.h
    src/query.h
    src/row.h
    src/parser.h
    src/pgwire.h
    src/index.h
    src/column.h
    src/dynamic_array.h
    src/stringview.h
)

# Create a library from the source files (excluding main.c)
add_library(mskql_lib STATIC ${MSKQL_SOURCES} ${MSKQL_HEADERS})
target_include_directories(mskql_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
# Ensure the library is compiled as C
set_target_properties(mskql_lib PROPERTIES LINKER_LANGUAGE C)

# Main executable
add_executable(mskql src/main.c)
target_link_libraries(mskql mskql_lib)

# Benchmarks
option(BUILD_BENCHMARKS "Build benchmarks" ON)

if(BUILD_BENCHMARKS)
    include(FetchContent)
    
    # CodSpeed mode configuration
    if(NOT DEFINED CODSPEED_MODE)
        set(CODSPEED_MODE "off" CACHE STRING "CodSpeed mode: off, simulation, or walltime")
    endif()
    
    message(STATUS "CodSpeed mode: ${CODSPEED_MODE}")
    
    # Fetch CodSpeed's Google Benchmark fork
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/CodSpeedHQ/benchmark.git
        GIT_TAG main
    )
    
    set(BENCHMARK_ENABLE_TESTING OFF CACHEBOOL "Disable benchmark tests")
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable gtest")
    
    FetchContent_MakeAvailable(benchmark)
    
    # Add benchmark executables
    add_executable(mskql_benchmarks
        benchmarks/database_benchmarks.cpp
    )
    
    target_link_libraries(mskql_benchmarks
        PRIVATE
        mskql_lib
        benchmark::benchmark
        benchmark::benchmark_main
    )
    
    target_include_directories(mskql_benchmarks
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    
    # Allow implicit conversions for C/C++ interop
    target_compile_options(mskql_benchmarks PRIVATE -Wno-error=conversion -fpermissive)
    
    # Set CodSpeed mode definition
    if(CODSPEED_MODE STREQUAL "simulation")
        target_compile_definitions(mskql_benchmarks PRIVATE CODSPEED_MODE_SIMULATION)
    elseif(CODSPEED_MODE STREQUAL "walltime")
        target_compile_definitions(mskql_benchmarks PRIVATE CODSPEED_MODE_WALLTIME)
    endif()
endif()
