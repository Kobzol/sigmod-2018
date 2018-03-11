#include "filter-iterator.h"
#include "hash-filter-iterator.h"
#include "sort-filter-iterator.h"

#include <cassert>

static bool passesFilter(const Filter& filter, uint64_t value)
{
    switch (filter.oper)
    {
        case '=': return value == filter.value;
        case '<': return value < filter.value;
        case '>': return value > filter.value;
        default: assert(false);
    }

    return false;
}

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

    return this->rowIndex < rowCount;
}

bool FilterIterator::passesFilters()
{
    for (int i = this->startFilterIndex; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
        if (!passesFilter(filter, this->relation->getValue(filter.selection, this->rowIndex))) return false;
    }

    return true;
}

std::unique_ptr<Iterator> FilterIterator::createIndexedIterator()
{
    return std::make_unique<INDEXED_FILTER>(this->relation, this->binding, this->filters);
}
