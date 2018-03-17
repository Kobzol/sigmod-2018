#pragma once

#include "filter-iterator.h"
#include "../index/sort-index.h"
#include "index-iterator.h"

/**
 * Filter that looks up values in a sort index. Works for <, > and = filters.
 */
class SortIndexIterator: public IndexIterator<RowEntry, SortIndex, SortIndexIterator>
{
public:
    SortIndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters);
    SortIndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters,
    RowEntry* start, RowEntry* end);

    SortIndex* getIndex(uint32_t relation, uint32_t column) final;

    RowEntry* lowerBound(uint64_t value) final;
    RowEntry* upperBound(uint64_t value) final;

    bool getNext() final;
    bool skipSameValue(const Selection& selection) final;

    bool passesFilters() final;

    void restore() final
    {
        this->start = this->startSaved;
        this->rowIndex = this->start->row;
    }

    std::string getFilterName() final
    {
        return "SFI";
    }
};
