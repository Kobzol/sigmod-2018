cmake_minimum_required(VERSION 3.4)
project(sigmod2018)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

OPTION(REAL_RUN "Real run" OFF)

if(NOT REAL_RUN)
    add_subdirectory("reference")
else()
    ADD_DEFINITIONS(-DREAL_RUN)
endif()

set(SOURCE_FILES
        src/aggregator.cpp
        src/executor.cpp
        src/io.cpp
        src/join/hash-joiner.cpp
        src/join/joiner.cpp
        src/join/nested-joiner.cpp
        src/relation/column-relation.cpp
        src/relation/filter-iterator.cpp
        src/relation/iterator.cpp
        src/stats.cpp
)

add_executable(sigmod-2018 ${SOURCE_FILES} src/main.cpp)
add_executable(tester ${SOURCE_FILES} src/test/main.cpp)
