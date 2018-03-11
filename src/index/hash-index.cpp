#include "hash-index.h"
#include "../relation/column-relation.h"

HashIndex::HashIndex(ColumnRelation& relation, uint32_t column): relation(relation), column(column)
{

}

void HashIndex::build()
{
    if (this->buildStarted.test_and_set()) return;

    auto count = static_cast<int32_t>(this->relation.tupleCount);
    for (int i = 0; i < count; i++)
    {
        uint64_t value = this->relation.getValue(static_cast<size_t>(i), this->column);
        this->hashTable[value].push_back(static_cast<size_t>(i));
    }

    this->buildCompleted = true;
}