cmake_minimum_required(VERSION 3.4)
project(sigmod2018)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -fno-rtti -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -flto -ftree-vectorize -march=native")

OPTION(REAL_RUN "Real run" OFF)
if(NOT REAL_RUN)
    add_subdirectory("reference")
else()
    ADD_DEFINITIONS(-DREAL_RUN)
endif()

set(SOURCE_FILES
        src/database.cpp
        src/executor.cpp
        src/io.cpp
        src/query.cpp
        src/stats.cpp
        src/timer.cpp
        src/aggregation/aggregated-iterator.cpp
        src/aggregation/aggregator.cpp
        src/aggregation/indexed-aggregated-iterator.cpp
        src/emit/filter-compiler.cpp
        src/foreign-key/foreign-key-checker.cpp
        src/index/aggregate-index.cpp
        src/index/hash-index.cpp
        src/index/index.cpp
        src/index/index-builder.cpp
        src/index/index-thread-pool.cpp
        src/index/primary-index.cpp
        src/index/sort-index.cpp
        src/join/hash-joiner.cpp
        src/join/index-joiner.cpp
        src/join/joiner.cpp
        src/join/merge-sort-joiner.cpp
        src/join/nested-joiner.cpp
        src/join/self-join.cpp
        src/relation/column-relation.cpp
        src/relation/filter-iterator.cpp
        src/relation/hash-filter-iterator.cpp
        src/relation/iterator.cpp
        src/relation/maxdiff_histogram.cpp
        src/relation/primary-index-iterator.cpp
        src/relation/sort-index-iterator.cpp
        src/rewrite/rewrite.cpp
)

#add_library(joiner STATIC ${SOURCE_FILES})

add_executable(sigmod-2018 src/main.cpp ${SOURCE_FILES})
#target_link_libraries(sigmod-2018 joiner)
#add_executable(tester src/test/main.cpp)
#target_link_libraries(tester joiner)
