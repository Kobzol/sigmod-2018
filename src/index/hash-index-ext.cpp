#include "hash-index-ext.h"
#include "../relation/column-relation.h"

HashIndexExt::HashIndexExt(ColumnRelation& relation, uint32_t column): relation(relation), column(column)
{

}

void HashIndexExt::build()
{
    if (this->buildStarted.test_and_set()) return;

    auto count = static_cast<int32_t>(this->relation.tupleCount);
	auto columnCount = static_cast<int32_t>(this->relation.columnCount);

    for (int i = 0; i < count; i++)
    {
        uint64_t value = this->relation.getValue(static_cast<size_t>(i), this->column);

		HashIndexEntry* entry = &this->hashTable[value];

		entry->rowIDs.push_back(static_cast<size_t>(i));
		entry->sum.resize(columnCount);

		for (int j = 0; j < columnCount; j++)
		{
			uint64_t sumVal = this->relation.getValue(static_cast<size_t>(i), j);
			entry->sum[j] = entry->sum[j] + sumVal;
		}
    }

    this->buildCompleted = true;
}
