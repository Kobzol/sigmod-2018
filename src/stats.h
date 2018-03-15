#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

extern double queryLoadTime;
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
extern std::unordered_map<uint32_t, uint32_t> columnIndexCounter;
