#pragma once

#include "../query.h"
#include "../util.h"
#include "column-relation.h"
#include "../index/hash-index.h"
#include "filter-iterator.h"

/**
 * Filter that works if there is at least one = in WHERE.
 * It looks up the = column in a hash index and then checks the rest of the predicates for every row.
 */
class HashFilterIterator: public FilterIterator
{
public:
    explicit HashFilterIterator(ColumnRelation* relation, uint32_t binding,
                                const std::vector<Filter>& filters, int equalsIndex = -1);

    bool getNext() final;
    bool skipSameValue(const Selection& selection) final;

    void prepareIndexedAccess(const Selection& selection) final;
    void iterateValue(const Selection& selection, uint64_t value) final;

    int64_t predictSize() final;

    std::string getFilterName() final
    {
        return "HFI";
    }

    HashIndex* index;
    const std::vector<uint64_t>* activeRow = nullptr;
    int activeRowIndex = -1;
    int32_t rowCount;

    Filter hashFilter;
};
