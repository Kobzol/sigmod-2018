#pragma once

#include <unordered_map>

#define INPUT_FILE "small.test"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

//#define STATISTICS
//#define MEASURE_REAL_TIME

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

//#define USE_HASH_INDEX
#define USE_SORT_INDEX                  // index of sorted <value, rowid> pairs
#define USE_PRIMARY_INDEX               // index of sorted <value, row content> pairs
#define USE_AGGREGATE_INDEX             // index of sorted <value, count, sums for all columns>

#define USE_SELF_JOIN                   // merge 0.0=0.1 into a filter
//#define USE_SEQUENTIAL_FILTER         // use sequential filter instead of indexed filters

//#define COMPILE_FILTERS               // compile filters to x64 assembly
#define AGGREGATE_PUSH                  // use direct aggregation

//#define USE_HISTOGRAM
#define BUCKET_N 50

#if defined(USE_SORT_INDEX) || defined(USE_PRIMARY_INDEX)
    #define INDEX_AVAILABLE
#endif

#ifndef USE_SORT_INDEX                  // aggregate index requires sort index
    #undef USE_AGGREGATE_INDEX
#endif

#define USE_BLOOM_FILTER                // use bloom filter in hash join
#ifdef USE_BLOOM_FILTER
    #define BLOOM_FILTER_SIZE (2 << 18)
#else
    #define BLOOM_FILTER_SIZE (2 << 3)
#endif

#ifndef INDEX_AVAILABLE                 // if there's no index, use sequential filters
    #define USE_SEQUENTIAL_FILTER
#endif

#define USE_THREADS
#define QUERY_NUM_THREADS 20            // number of threads to execute queries
#define HASH_AGGREGATE_THREADS 4        // number of threads to aggregate results in top-level hash join

//#define COLLECT_JOIN_SIZE             // collect sizes of intermediate results

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;
