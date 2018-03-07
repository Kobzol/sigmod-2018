#pragma once

#define INPUT_FILE "small.complete"

#ifndef NDEBUG
    #define STATISTICS
    #define LOAD_FROM_FILE INPUT_FILE
    #define CHECK_ERRORS
#endif

//#define SORT_JOINS_BY_SIZE
//#define TRANSPOSE_RELATIONS

#define QUERY_NUM_THREADS 20
