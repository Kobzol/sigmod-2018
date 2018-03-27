#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <atomic>

extern double queryLoadTime;
extern double relationLoadTime;
extern double transposeTime;
extern double indicesInitTime;
extern double startIndexTime;
extern double histogramTime;
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
extern size_t sortedOnFirstColumn;
extern size_t joinsOnFirstColumn;
extern size_t filtersOnFirstColumn;
extern size_t selfJoinCount;
extern size_t filtersSkippedByHistogram;
extern size_t filterEqualsJoined;
extern size_t joinOneUnique;
extern size_t joinBothUnique;
extern size_t skippableFK;
extern std::atomic<size_t> averageRowsInHash;
extern std::atomic<size_t> averageRowsInHashCount;
extern std::atomic<size_t> emptyHashTableCount;
