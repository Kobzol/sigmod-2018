#pragma once

#include <atomic>
#include <limits>
#include <vector>

#include "index.h"

#define MAX_COLUMNS 10

struct PrimaryRowEntry
{
public:
    uint64_t row[MAX_COLUMNS];
};

/**
 * Primary index for a given relation and column.
 * Stores a sorted list of <value, row data> pairs.
 */
class PrimaryIndex: public Index
{
public:
    PrimaryIndex(ColumnRelation& relation, uint32_t column, std::vector<PrimaryRowEntry> data);

    void build() final;

    std::vector<PrimaryRowEntry> data;
};
