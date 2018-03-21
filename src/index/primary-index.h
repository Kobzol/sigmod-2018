#pragma once

#include <atomic>
#include <limits>
#include <vector>

#include "index.h"

#define PRIMARY_INDEX_COLUMNS 5

struct PrimaryRowEntry
{
public:
    uint64_t row[PRIMARY_INDEX_COLUMNS];
};

/**
 * Primary index for a given relation and column.
 * Stores a sorted list of <value, row data> pairs.
 */
class PrimaryIndex: public Index
{
public:
    static bool canBuild(ColumnRelation& relation);

    PrimaryIndex(ColumnRelation& relation, uint32_t column, std::vector<PrimaryRowEntry> data);

    void build() final;

    std::vector<PrimaryRowEntry> data;
};
