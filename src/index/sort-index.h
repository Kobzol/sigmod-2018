#pragma once

#include <atomic>
#include <vector>
#include <limits>

#include "../util.h"

class ColumnRelation;

struct RowEntry
{
public:
    bool operator<(const RowEntry& other) const
    {
        /*if (this->value == other.value)
        {
            return this->row < other.row;
        }*/

        return this->value < other.value;
    }

    uint64_t value;
    uint32_t row;
};

/**
 * Sort index for a given relation and column.
 * Stores a sorted list of <value, rowid> pairs.
 */
class SortIndex
{
public:
    SortIndex(ColumnRelation& relation, uint32_t column);

    void build();

    std::atomic_flag buildStarted = ATOMIC_FLAG_INIT;
    std::atomic<bool> buildCompleted { false };

    ColumnRelation& relation;
    uint32_t column;

    std::vector<RowEntry> data;
    uint64_t minValue = std::numeric_limits<uint64_t>::max();
    uint64_t maxValue = 0;
};
