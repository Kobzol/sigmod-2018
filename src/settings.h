#pragma once

#include <unordered_map>

#define INPUT_FILE "small.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

#define MEASURE_REAL_TIME

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

//#define USE_HASH_INDEX
//#define USE_SORT_INDEX
//#define USE_PRIMARY_INDEX
//#define USE_SELF_JOIN
//#define USE_SEQUENTIAL_FILTER

//#define COMPILE_FILTERS
//#define AGGREGATE_PUSH

//#define USE_HISTOGRAM
#define BUCKET_N 50

#if defined(USE_SORT_INDEX) || defined(USE_PRIMARY_INDEX)
    #define INDEX_AVAILABLE
#endif

#define USE_BLOOM_FILTER
#ifdef USE_BLOOM_FILTER
    #define BLOOM_FILTER_SIZE (2 << 18)
#else
    #define BLOOM_FILTER_SIZE (2 << 3)
#endif

#ifndef INDEX_AVAILABLE
    #define USE_SEQUENTIAL_FILTER
#endif

//#define USE_THREADS
#define QUERY_NUM_THREADS 20
#define HASH_AGGREGATE_THREADS 4

//#define COLLECT_JOIN_SIZE

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;
