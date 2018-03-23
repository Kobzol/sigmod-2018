#pragma once

#include "aggregated-iterator.h"
#include "../index/aggregate-index.h"

template <bool IS_GROUPBY_SUMMED>
class IndexedAggregatedIterator: public AggregatedIterator<IS_GROUPBY_SUMMED>
{
public:
    IndexedAggregatedIterator(Iterator* inner, const Selection& groupBy, const std::vector<Selection>& sumSelections);

    bool getNext() final;

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) final;

    void prepareIndexedAccess(const Selection& selection) final;
    void iterateValue(const Selection& selection, uint64_t value) final;

    void dumpPlan(std::ostream& ss) final;

    AggregateRow* start;
    AggregateRow* end;
    AggregateIndex* index;
    AggregateRow* originalStart;

    Selection iterateValueSelection{100, 100, 100};
    uint64_t iteratedValue;
};

template class IndexedAggregatedIterator<false>;
template class IndexedAggregatedIterator<true>;
