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

bool ColumnRelationIterator::getBlock(std::vector<uint64_t*>& cols, size_t& rows)
{
    if (this->rowIndex >= this->relation->getRowCount()) return false;
    this->rowIndex++;

    cols.resize(this->blockSelections.size());

    rows = (size_t) std::min(this->relation->getRowCount() - this->rowIndex, static_cast<int64_t>(BLOCK_SIZE));
    for (int i = 0; i < static_cast<int32_t>(this->blockSelections.size()); i++)
    {
        cols[i] = this->relation->data + (this->blockSelections[i].column * this->rowIndex);
    }

    this->rowIndex += rows - 1;
    return true;
}
