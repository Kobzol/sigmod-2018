#pragma once

#include <unordered_map>

#define INPUT_FILE "public.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

#define MEASURE_REAL_TIME
#define STATISTICS

#define SORT_JOINS_BY_SIZE

//#define USE_HASH_INDEX
#define USE_SORT_INDEX                  // index of sorted <value, rowid> pairs
#define USE_PRIMARY_INDEX               // index of sorted <value, row content> pairs
//#define USE_AGGREGATE_INDEX             // index of sorted <value, count, sums for all columns>

//#define USE_SELF_JOIN                   // merge 0.0=0.1 into a filter
//#define USE_SEQUENTIAL_FILTER         // use sequential filter instead of indexed filters
//#define FORCE_INDEXED_FILTER            // always use indexed filter

//#define COMPILE_FILTERS               // compile filters to x64 assembly
#define EXPAND_FILTERS                // expand filters to all members of a join component
//#define AGGREGATE_PUSH                  // use direct aggregation
#define REWRITE_FKS                     // rewrite PK-FK joins
//#define USE_MULTILEVEL_INDEX            // use multi-level primary index

#define CACHE_ITERATE_VALUE             // cache iterators used in iterateValue
#define PREBUILD_PRIMARY_COLUMNS 3       // how many columns for primary index should be prebuilt


//#define USE_HISTOGRAM
#define BUCKET_N 50

#if defined(USE_SORT_INDEX) || defined(USE_PRIMARY_INDEX)
    #define INDEX_AVAILABLE
#endif

#ifndef USE_SORT_INDEX                  // aggregate index requires sort index
    #undef USE_AGGREGATE_INDEX
#endif

//#define USE_BLOOM_FILTER                // use bloom filter in hash join
#ifdef USE_BLOOM_FILTER
    #define BLOOM_FILTER_SIZE (2 << 18)
#else
    #define BLOOM_FILTER_SIZE (2 << 3)
#endif

#ifndef INDEX_AVAILABLE                 // if there's no index, use sequential filters
    #define USE_SEQUENTIAL_FILTER
#endif

#ifndef INDEX_AVAILABLE
    #undef AGGREGATE_PUSH
#endif

#ifndef AGGREGATE_PUSH
    #undef USE_AGGREGATE_INDEX
#endif

#define USE_THREADS
#define HASH_AGGREGATE_THREADS 8        // number of threads to aggregate results in top-level hash join
#define FK_CHECK_SPLIT 120
#define PARALLEL_JOIN_SPLIT 80

//#define USE_INDEX_THREADPOOL
#define INDEX_THREADS 30
#define INDEX_THREAD_BAILOUT 1200       // how many milliseconds should the index be precalculated

//#define COLLECT_JOIN_SIZE             // collect sizes of intermediate results

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;

extern int THREAD_COUNT;
