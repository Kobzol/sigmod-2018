#pragma once

#include "filter-iterator.h"
#include "../index/sort-index.h"

class SortFilterIterator: public FilterIterator
{
public:
    SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters);

    bool getNext() override;
    bool skipSameValue() override;

    RowEntry* toPtr(const std::vector<RowEntry>::iterator& iterator) const;

    SortIndex* index;
    Filter sortFilter;

    RowEntry* start;
    RowEntry* end;
};
