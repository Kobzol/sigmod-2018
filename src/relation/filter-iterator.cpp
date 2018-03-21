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

std::unique_ptr<Iterator> FilterIterator::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container)
{
    return database.createIndexedIterator(*this->relation, this->binding, this->filters);
}

int64_t FilterIterator::predictSize()
{
#ifdef USE_HISTOGRAM
    return database.histograms[this->filters[0].selection.relation].estimateResult(this->filters[0]);
#else
    return this->relation->getRowCount();
#endif
}
