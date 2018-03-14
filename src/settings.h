#pragma once

#include <unordered_map>

#define INPUT_FILE "small.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

#define MEASURE_REAL_TIME

#ifdef STATISTICS
    #define COLLECT_JOIN_SIZE
#endif

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

#define USE_HASH_INDEX
#define USE_SORT_INDEX
#define USE_SELF_JOIN

//#define USE_HISTOGRAM
#define BUCKET_N 50

#ifdef USE_SORT_INDEX
    #define FILTER_ITERATOR SortFilterIterator
#else
    #define FILTER_ITERATOR FilterIterator
#endif

#define INDEXED_FILTER SortFilterIterator

#define USE_BLOOM_FILTER
#ifdef USE_BLOOM_FILTER
    #define BLOOM_FILTER_SIZE (2 << 18)
#else
    #define BLOOM_FILTER_SIZE (2 << 3)
#endif

#define USE_THREADS
#define QUERY_NUM_THREADS 20

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;
