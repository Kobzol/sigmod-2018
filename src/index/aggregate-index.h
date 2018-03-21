#pragma once

#include <cstddef>
#include "index.h"
#include "sort-index.h"

struct AggregateRow
{
public:
    AggregateRow() = default;
    explicit AggregateRow(uint64_t value): value(value)
    {

    }

    uint64_t value;
    std::vector<uint64_t> sums;
    size_t count = 0;
};

class AggregateIndex: public Index
{
public:
    AggregateIndex(ColumnRelation& relation, uint32_t column, SortIndex& index);

    bool build() final;

    std::vector<AggregateRow> data;
    SortIndex& index;
};
