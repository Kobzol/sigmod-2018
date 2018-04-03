#pragma once

#include "settings.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <atomic>

#ifdef STATISTICS
    extern double queryLoadTime;
    extern double relationLoadTime;
    extern double transposeTime;
    extern double indicesInitTime;
    extern double startIndexTime;
    extern double histogramTime;
    extern double indexBuildTime;
    extern double queryRewriteTime;
    extern size_t tupleCount;
    extern size_t columnCount;
    extern uint32_t minColumns;
    extern uint32_t maxColumns;
    extern size_t minTuples;
    extern size_t maxTuples;
    extern size_t queryCount;
    extern size_t joinCount;
    extern size_t filterCount;
    extern size_t filterEqualsCount;
    extern size_t batchCount;
    extern size_t multipleColumnsPerRelJoins;
    extern size_t columnsPerJoin;
    extern size_t joinsOnFirstColumn;
    extern size_t filtersOnFirstColumn;
    extern size_t selfJoinCount;
    extern size_t filtersSkippedByHistogram;
    extern size_t filterEqualsJoined;
    extern size_t joinOneUnique;
    extern size_t joinBothUnique;
    extern size_t skippableFK;
    extern size_t skippedJoins;
    extern std::atomic<size_t> plansSkipped;
    extern std::atomic<size_t> indexMinMaxTime;
    extern std::atomic<size_t> indexGroupCountTime;
    extern std::atomic<size_t> indexCopyToBucketsTime;
    extern std::atomic<size_t> indexSortTime;
    extern std::atomic<size_t> indexFinalizeTime;
#endif
