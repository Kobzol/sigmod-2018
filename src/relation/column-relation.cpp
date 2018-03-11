#include "column-relation.h"
#include "hash-filter-iterator.h"
#include "sort-filter-iterator.h"

bool ColumnRelationIterator::getNext()
{
    this->rowIndex++;
    return this->rowIndex < this->relation->getRowCount();
}

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
