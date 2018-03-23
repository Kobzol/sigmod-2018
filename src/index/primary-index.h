#pragma once

#include <atomic>
#include <limits>
#include <vector>

#include "index.h"

#define PRIMARY_INDEX_MAX_COLUMNS 9

struct PrimaryRowEntry
{
public:
    uint64_t row[1];
};

/**
 * Primary index for a given relation and column.
 * Stores a sorted list of <value, row data> pairs.
 */
class PrimaryIndex: public Index
{
public:
    static bool canBuild(ColumnRelation& relation);
    static int rowSize(ColumnRelation& relation);
    static PrimaryRowEntry* move(ColumnRelation& relation, PrimaryRowEntry* entry, int offset);

    PrimaryIndex(ColumnRelation& relation, uint32_t column, uint64_t* init);

    bool build() final;

    PrimaryRowEntry* move(PrimaryRowEntry* ptr, int offset)
    {
        return ptr + (offset * this->rowOffset);
    }
    PrimaryRowEntry* inc(PrimaryRowEntry* ptr)
    {
        return ptr + this->rowOffset;
    }
    PrimaryRowEntry* dec(PrimaryRowEntry* ptr)
    {
        return ptr - this->rowOffset;
    }

    PrimaryRowEntry* lowerBound(uint64_t value);
    PrimaryRowEntry* upperBound(uint64_t value);

    int64_t count(PrimaryRowEntry* from, PrimaryRowEntry* to);

    PrimaryRowEntry* begin;
    PrimaryRowEntry* end;

    uint64_t* init;
    uint64_t* mem;
    PrimaryRowEntry* data;

    int rowSizeBytes;
    int rowOffset;
};
