cmake_minimum_required(VERSION 3.4)
project(sigmod2018)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address -Wall")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fsanitize=address -Wall")

add_subdirectory("reference")

set(SOURCE_FILES
        src/relation/column-relation.cpp
        src/join/hash-joiner.cpp
        src/relation/filter-view.cpp
        src/relation/view.cpp
        src/aggregator.cpp
        src/executor.cpp
        src/stats.cpp
        src/io.cpp
        src/join/joiner.cpp
        src/relation/iterator.cpp
        src/join/nested-joiner.cpp src/join/nested-joiner.h)

add_executable(sigmod-2018 ${SOURCE_FILES} src/main.cpp)
add_executable(tester ${SOURCE_FILES} src/test/main.cpp)
