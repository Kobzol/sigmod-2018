#include "column-relation.h"
#include "../database.h"

#include <cmath>

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

std::unique_ptr<Iterator> ColumnRelationIterator::createIndexedIterator(
        std::vector<std::unique_ptr<Iterator>>& container, const Selection& selection)
{
    return database.createIndexedIterator(selection, {});
}

static void vectorSum(uint64_t* __restrict__ target, const uint64_t* __restrict__ source, int rows)
{
    for (int i = 0; i < rows; i++)
    {
        *target += source[i];
    }
}

void ColumnRelationIterator::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                                     const std::vector<Selection>& selections, size_t& count)
{
    int rows = (this->end - (this->rowIndex + 1));
    auto totalRows = static_cast<int>(this->relation->getRowCount());

    for (int i = 0; i < static_cast<int32_t>(results.size()); i++)
    {
        int offset = this->rowIndex + 1;
        vectorSum(results.data() + i, this->relation->data + columnIds[i] * totalRows + offset, rows);
    }
    this->rowIndex = this->end;
    count = static_cast<size_t>(rows);
}

void ColumnRelationIterator::split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                                   size_t count)
{
    auto size = this->relation->getRowCount();
    auto chunkSize = static_cast<int>(std::ceil(size / (double) count));

    int left = size;
    int start = this->rowIndex + 1;
    while (left > 0)
    {
        int chunk = std::min(chunkSize, left);
        left -= chunk;

        int end = start + chunk;

        container.push_back(this->createForRange(start - 1, end));
        groups.push_back(container.back().get());

        start = end;
    }
}
