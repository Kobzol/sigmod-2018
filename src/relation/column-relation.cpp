#include "column-relation.h"
#include "hash-filter-iterator.h"
#include "sort-index-iterator.h"
#include "primary-index-iterator.h"

bool ColumnRelationIterator::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (selection.binding != this->binding) return false;
    value = this->getValue(selection);
    return true;
}

void ColumnRelationIterator::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
    for (auto& sel: selections)
    {
        *row++ = this->getValue(sel);
    }
}

int32_t ColumnRelationIterator::getRowCount()
{
    return static_cast<int32_t>(this->relation->getRowCount());
}

std::unique_ptr<Iterator> ColumnRelationIterator::createIndexedIterator()
{
    return std::make_unique<INDEXED_FILTER>(this->relation, this->binding, std::vector<Filter>());
}
