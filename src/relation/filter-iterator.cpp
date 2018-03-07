#include "filter-iterator.h"

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

FilterIterator::FilterIterator(ColumnRelation* relation, uint32_t binding)
        : ColumnRelationIterator(relation, binding)
{

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
    for (auto& filter: this->filters)
    {
        if (!passesFilter(filter, this->relation->getValue(filter.selection, this->rowIndex))) return false;
    }

    return true;
}
