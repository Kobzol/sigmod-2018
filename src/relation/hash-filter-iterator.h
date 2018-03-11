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
                                const std::vector<Filter>& filters, int equalsIndex = -1);

    bool getNext() final;
    bool skipSameValue() final;

    void prepareIndexedAccess() final;
    void iterateValue(const Selection& selection, uint64_t value) final;

    HashIndex* index;
    const std::vector<uint64_t>* activeRow = nullptr;
    int activeRowIndex = -1;
    int32_t rowCount;

    Filter hashFilter;
};
