#pragma once

#include <unordered_map>

#define INPUT_FILE "public.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

#define MEASURE_REAL_TIME

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

//#define USE_HASH_INDEX
//#define USE_SORT_INDEX
#define USE_PRIMARY_INDEX
#define USE_SELF_JOIN

//#define USE_HISTOGRAM
#define BUCKET_N 50

#if defined(USE_SORT_INDEX) || defined(USE_PRIMARY_INDEX)
    #define INDEX_AVAILABLE
#endif

#ifdef USE_SORT_INDEX
    #define FILTER_ITERATOR SortIndexIterator
#elif defined(USE_PRIMARY_INDEX)
    #define FILTER_ITERATOR PrimaryIndexIterator
#else
    #define FILTER_ITERATOR FilterIterator
#endif

#ifdef USE_PRIMARY_INDEX
    #define INDEXED_FILTER PrimaryIndexIterator
#else
    #define INDEXED_FILTER SortIndexIterator
#endif

#define USE_BLOOM_FILTER
#ifdef USE_BLOOM_FILTER
    #define BLOOM_FILTER_SIZE (2 << 18)
#else
    #define BLOOM_FILTER_SIZE (2 << 3)
#endif

#define USE_THREADS
#define QUERY_NUM_THREADS 20
#define HASH_AGGREGATE_THREADS 4

//#define COLLECT_JOIN_SIZE

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;
