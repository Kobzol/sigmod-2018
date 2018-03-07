#include "column-relation.h"

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

void ColumnRelationIterator::sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns)
{
    auto colSize = static_cast<int32_t>(columns.size());
    for (int i = 0; i < colSize; i++)
    {
        sums[i] += this->getColumn(columns[i]);
    }
}

void ColumnRelationIterator::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
    for (auto& sel: selections)
    {
        *row++ = this->getValue(sel);
    }
}
