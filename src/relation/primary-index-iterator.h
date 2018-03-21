#pragma once

#include "filter-iterator.h"
#include "../index/primary-index.h"
#include "index-iterator.h"

/**
 * Filter that looks up values in a sort index. Works for <, > and = filters.
 */
class PrimaryIndexIterator: public IndexIterator<PrimaryRowEntry, PrimaryIndex, PrimaryIndexIterator>
{
public:
    PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters);
    PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters,
                       PrimaryRowEntry* start, PrimaryRowEntry* end);

    bool getNext() final;
    bool skipSameValue(const Selection& selection) final;

    bool passesFilters() final;

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;
    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    PrimaryIndex* getIndex(uint32_t relation, uint32_t column) final;

    PrimaryRowEntry* lowerBound(uint64_t value) final;
    PrimaryRowEntry* upperBound(uint64_t value) final;

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) final;

    std::string getFilterName() final
    {
        return "PFI";
    }
};
