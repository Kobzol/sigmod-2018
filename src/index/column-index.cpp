#include "column-index.h"
#include "../relation/column-relation.h"

#include <cstring>
#include <algorithm>

ColumnIndex::ColumnIndex(ColumnRelation& relation, uint32_t column): relation(relation), column(column)
{

}

void ColumnIndex::create()
{
    if (this->buildStarted.test_and_set()) return;

    std::vector<uint64_t> data(this->relation.tupleCount);
    std::memcpy(data.data(), this->relation.data + (this->relation.tupleCount * this->column),
                this->relation.tupleCount * sizeof(uint64_t));

    std::sort(data.begin(), data.end());

    this->buildCompleted = true;
}
