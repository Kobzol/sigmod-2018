#include "stats.h"

#include <limits>

#include "settings.h"

#ifdef STATISTICS
    size_t tupleCount = 0;
    size_t columnCount = 0;
    size_t minColumns = std::numeric_limits<size_t>::max();
    size_t maxColumns = 0;
    size_t minTuples = std::numeric_limits<size_t>::max();
    size_t maxTuples = 0;
    size_t queryCount = 0;
    size_t joinCount = 0;
    size_t filterCount = 0;
    size_t batchCount = 0;
    size_t multipleColumnsPerRelJoins = 0;
    size_t columnsPerJoin = 0;
    size_t relationsMissingInJoins = 0;
    size_t sortedOnFirstColumn = 0;
    size_t joinsOnFirstColumn = 0;
    size_t filtersOnFirstColumn = 0;
#endif
