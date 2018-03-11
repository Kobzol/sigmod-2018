#pragma once

#include "hash/sparse_map.h"
#include "hash/hopscotch_map.h"
#include <unordered_map>

#define INPUT_FILE "small.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
#endif

#define MEASURE_REAL_TIME
#define STATISTICS

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

#define USE_HASH_INDEX
//#define USE_SORT_INDEX
//#define USE_SELF_JOIN

#ifdef USE_SORT_INDEX
    #define FILTER_ITERATOR SortFilterIterator
#else
    #define FILTER_ITERATOR FilterIterator
#endif

#if defined(USE_HASH_INDEX) || defined(USE_SORT_INDEX)
    #define SORTED_ROWS
#endif

#define QUERY_NUM_THREADS 20

template <typename K, typename V>
using HashMap = std::unordered_map<K, V>;
