cmake_minimum_required(VERSION 3.4)
project(sigmod2018)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address -Wall")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fsanitize=address -Wall")

add_subdirectory("reference")

set(SOURCE_FILES
        src/main.cpp
        src/relation/column-relation.cpp
        src/join/hash-joiner.cpp
        src/relation/filter-view.cpp
        src/relation/view.cpp
        src/aggregator.cpp src/aggregator.h src/executor.cpp src/executor.h)

add_executable(sigmod-2018 ${SOURCE_FILES})
