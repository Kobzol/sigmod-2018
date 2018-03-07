#pragma once

#include <cstddef>
#include <cstdint>

extern size_t tupleCount;
extern size_t columnCount;
extern uint32_t minColumns;
extern uint32_t maxColumns;
extern size_t minTuples;
extern size_t maxTuples;
extern size_t queryCount;
extern size_t joinCount;
extern size_t filterCount;
extern size_t batchCount;
extern size_t multipleColumnsPerRelJoins;
extern size_t columnsPerJoin;
extern size_t sortedOnFirstColumn;
extern size_t joinsOnFirstColumn;
extern size_t filtersOnFirstColumn;
