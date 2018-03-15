#pragma once

#include "filter-iterator.h"
#include "../index/sort-index.h"

/**
 * Filter that looks up values in a sort index. Works for <, > and = filters.
 */
class SortFilterIterator: public FilterIterator
{
public:
    SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters);

    bool getNext() final;
    bool skipSameValue(const Selection& selection) final;

    void prepareIndexedAccess() final;
    void prepareSortedAccess(const Selection& selection) final;
    bool isSortedOn(const Selection& selection) final
    {
        return selection == this->sortSelection;
    }

    void iterateValue(const Selection& selection, uint64_t value) final;

    int64_t predictSize() final;

    void save() final;
    void restore() final;

    std::string getFilterName() final
    {
        return "SFI";
    }

    RowEntry* toPtr(const std::vector<RowEntry>::iterator& iterator) const;

    SortIndex* index;
    Filter sortFilter;
    Selection sortSelection;

    RowEntry* start = nullptr;
    RowEntry* end = nullptr;

    RowEntry* startSaved;
    RowEntry* originalStart;
};
