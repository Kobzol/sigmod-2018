#include "stats.h"

#include <limits>

#include "settings.h"

#ifdef STATISTICS
    double queryLoadTime = 0.0;
    double relationLoadTime = 0.0;
    double transposeTime = 0.0;
    double indicesInitTime = 0.0;
    double startIndexTime = 0.0;
    double histogramTime = 0.0;
    size_t tupleCount = 0;
    size_t columnCount = 0;
    uint32_t minColumns = std::numeric_limits<uint32_t>::max();
    uint32_t maxColumns = 0;
    size_t minTuples = std::numeric_limits<size_t>::max();
    size_t maxTuples = 0;
    size_t queryCount = 0;
    size_t joinCount = 0;
    size_t filterCount = 0;
    size_t filterEqualsCount = 0;
    size_t batchCount = 0;
    size_t multipleColumnsPerRelJoins = 0;
    size_t columnsPerJoin = 0;
    size_t sortedOnFirstColumn = 0;
    size_t joinsOnFirstColumn = 0;
    size_t filtersOnFirstColumn = 0;
    size_t selfJoinCount = 0;
    size_t filtersSkippedByHistogram = 0;
    std::atomic<size_t> averageRowsInHash{0};
    std::atomic<size_t> averageRowsInHashCount{0};
    std::atomic<size_t> emptyHashTableCount{0};
#endif
