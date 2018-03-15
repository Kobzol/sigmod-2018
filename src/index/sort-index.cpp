#include "sort-index.h"
#include "../relation/column-relation.h"

#include <cstring>
#include <algorithm>

SortIndex::SortIndex(ColumnRelation& relation, uint32_t column)
        : relation(relation), column(column), data(static_cast<size_t>(relation.getRowCount()))
{

}

void SortIndex::build()
{
    if (this->buildStarted.test_and_set()) return;

    auto rows = static_cast<int32_t>(this->data.size());
    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        this->data[i].value = value;
        this->data[i].row = static_cast<uint32_t>(i);
        this->maxValue = std::max(this->maxValue, value);
        this->minValue = std::min(this->minValue, value);
    }
    std::sort(this->data.begin(), this->data.end());

    this->buildCompleted = true;
}
