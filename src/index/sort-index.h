#pragma once

#include <atomic>
#include <vector>

#include "../util.h"
#include "index.h"
#include "../settings.h"

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
    uint64_t row;
};

/**
 * Sort index for a given relation and column.
 * Stores a sorted list of <value, rowid> pairs.
 */
class SortIndex: public Index
{
public:
    SortIndex(ColumnRelation& relation, uint32_t column);

    bool build() final
    {
        return this->build(4);
    }
    bool build(uint32_t threads);

    RowEntry* move(RowEntry* ptr, int offset)
    {
        return ptr + offset;
    }
    RowEntry* inc(RowEntry* ptr)
    {
        return ptr + 1;
    }
    RowEntry* dec(RowEntry* ptr)
    {
        return ptr - 1;
    }

    RowEntry* lowerBound(uint64_t value);
    RowEntry* upperBound(uint64_t value);

    int64_t count(RowEntry* from, RowEntry* to);

    RowEntry* begin;
    RowEntry* end;

    std::vector<RowEntry> data;

private:
    RowEntry* toPtr(const std::vector<RowEntry>::iterator& iterator)
    {
        return this->data.data() + (iterator - this->data.begin());
    }
};
