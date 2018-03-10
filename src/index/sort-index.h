#pragma once

#include <atomic>
#include <vector>

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
};
