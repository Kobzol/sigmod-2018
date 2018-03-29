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

struct IndexGroup
{
public:
    uint64_t startValue;
    uint64_t endValue;
    PrimaryRowEntry* start;
    PrimaryRowEntry* end;
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
    static int64_t count(ColumnRelation& relation, PrimaryRowEntry* from, PrimaryRowEntry* to);

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

    template <int N>
    PrimaryRowEntry* findLowerBound(uint64_t* mem, int64_t rows, uint64_t value, uint32_t column);

    template <int N>
    PrimaryRowEntry* findUpperBound(uint64_t* mem, int64_t rows, uint64_t value, uint32_t column);

    int64_t count(PrimaryRowEntry* from, PrimaryRowEntry* to);

    uint32_t groupValue(uint64_t value)
    {
        return static_cast<uint32_t>(((value - this->minValue) / (double) (this->diff)) * this->groupCount);
    }

    PrimaryRowEntry* begin;
    PrimaryRowEntry* end;

    uint64_t* init;
    uint64_t* mem;
    PrimaryRowEntry* data;

    std::vector<IndexGroup> groups;
    uint64_t diff;

    int groupCount;

    int rowSizeBytes;
    int rowOffset;
};
