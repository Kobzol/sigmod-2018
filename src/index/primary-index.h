#pragma once

#include <atomic>
#include <limits>
#include <vector>
#include <array>
#include <functional>

#include "index.h"
#include "../settings.h"

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

struct PrimaryRowTarget
{
    uint32_t group;
    uint32_t index;
};

template <int N>
struct Row
{
public:
    std::array<uint64_t, N> row;
};

template <int N>
struct RadixTraitsRow
{
    static const int nBytes = 8;

    explicit RadixTraitsRow(int column, uint64_t minValue): minValue(minValue), column(column)
    {

    }

    int kth_byte(const Row<N> &x, int k) {
        return ((x.row[this->column] - this->minValue) >> (k * 8)) & 0xFF;
    }
    bool compare(const Row<N> &x, const Row<N> &y) {
        return x.row[this->column] < y.row[this->column];
    }

    uint64_t minValue;
    int column;
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

    bool build() final
    {
        return this->build(4);
    }
    bool build(uint32_t threads);

    void initMemory();
    void initGroups(int threads = 1);
    void finalize();
    void sortDirect();
    void sortGroups(int threads = 1);

    template <int N>
    void sortGroupsParallel(int threads);

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

    template <int N>
    void sort(PrimaryRowEntry* mem, PrimaryRowEntry* end, uint32_t column, uint64_t minValue, uint64_t maxValue);

    PrimaryRowEntry* begin;
    PrimaryRowEntry* end;

    uint64_t* init;
    uint64_t* mem;
    PrimaryRowEntry* data;
    std::vector<IndexGroup> indexGroups;
    uint64_t diff;

    PrimaryRowTarget* rowTargets;
    std::vector<uint32_t> computeOffsets;
    std::vector<uint32_t> counts;
    std::vector<uint32_t> starts;
    std::vector<uint32_t> unique;

    PrimaryRowEntry* (PrimaryIndex::*lowerBoundFn)(uint64_t* mem, int64_t rows, uint64_t value, uint32_t column);

    int groupCount;
    int rowSizeBytes;
    int rowOffset;
};
