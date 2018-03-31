#include "filter-iterator.h"
#include "hash-filter-iterator.h"
#include "sort-index-iterator.h"
#include "../database.h"
#include "primary-index-iterator.h"

#include <cassert>

FilterIterator::FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters)
        : ColumnRelationIterator(relation, binding), filters(std::move(filters))
{
    this->filterSize = static_cast<int>(this->filters.size());
}

bool FilterIterator::getNext()
{
    this->rowIndex++;

    auto rowCount = this->relation->getRowCount();
    while (this->rowIndex < rowCount && !this->passesFilters())
    {
        this->rowIndex++;
    }

#ifdef COLLECT_JOIN_SIZE
    this->rowCount++;
#endif

    return this->rowIndex < rowCount;
}

bool FilterIterator::passesFilters()
{
    for (int i = this->startFilterIndex; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
#ifdef COMPILE_FILTERS
        if (!filter.evaluator(this->relation->getValue(filter.selection, this->rowIndex))) return false;
#else
        if (!passesFilter(filter, this->relation->getValue(filter.selection, this->rowIndex))) return false;
#endif
    }

    return true;
}

std::unique_ptr<Iterator> FilterIterator::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                                const Selection& selection)
{
    return database.createIndexedIterator(selection, this->filters);
}

int64_t FilterIterator::predictSize()
{
#ifdef USE_HISTOGRAM
    return database.histograms[this->filters[0].selection.relation].estimateResult(this->filters[0]);
#else
    return this->relation->getRowCount();
#endif
}

void FilterIterator::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                             const std::vector<Selection>& selections, size_t& count)
{
    auto rows = static_cast<int32_t>(this->getRowCount());
    size_t localCount = 0;
    int start = this->rowIndex;
    this->startFilterIndex = 0;

    for (int i = 0; i < static_cast<int32_t>(results.size()); i++)
    {
        this->rowIndex = start;
        this->rowIndex++;

        while (this->rowIndex < rows)
        {
            if (FilterIterator::passesFilters())
            {
                results[i] += FilterIterator::getColumn(columnIds[i]);
                localCount++;
            }

            this->rowIndex++;
        }

        if (localCount == 0) break;
        else if (i < results.size() - 1) localCount = 0;
    }

    count = localCount;
}
