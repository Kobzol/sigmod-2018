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
    RowEntry* start, RowEntry* end, Selection iteratedSelection, int startFilterIndex);

    SortIndex* getIndex(uint32_t relation, uint32_t column) final;

    bool getNext() final;
    bool skipSameValue(const Selection& selection) final;
    bool skipTo(const Selection& selection, uint64_t value) final;

    RowEntry* findNextValue(RowEntry* iter, RowEntry* end, const Selection& selection, uint64_t value) final;
    int64_t count(RowEntry* from, RowEntry* to) final;
    uint64_t getValueForIter(RowEntry* iter, const Selection& selection) final;

    bool passesFilters() final;

    void restore() final
    {
        this->start = this->startSaved;
        this->rowIndex = this->start->row;
    }

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) final;

    std::string getFilterName() final
    {
        return "SFI";
    }
};
