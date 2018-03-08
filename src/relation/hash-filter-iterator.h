#pragma once

#include "../query.h"
#include "../util.h"
#include "column-relation.h"
#include "../index/hash-index.h"
#include "filter-iterator.h"

class HashFilterIterator: public FilterIterator
{
public:
    explicit HashFilterIterator(ColumnRelation* relation, uint32_t binding,
                                std::vector<Filter> filters, int equalsIndex);

    bool getNext() override;

    HashIndex* index;
    std::vector<uint64_t>* activeRow = nullptr;
    int activeRowIndex = -1;

    Filter hashFilter;
};
