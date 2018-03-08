#include "hash-filter-iterator.h"
#include "../database.h"

// TODO: for more equal filters, pick the one with lowest amount of rows
HashFilterIterator::HashFilterIterator(ColumnRelation* relation, uint32_t binding,
                                       std::vector<Filter> filters, int equalsIndex)
        : FilterIterator(relation, binding, std::move(filters))
{
    this->hashFilter = this->filters[equalsIndex];
    this->filters.erase(this->filters.begin() + equalsIndex);
    this->index = &database.getHashIndex(this->hashFilter.selection.relation, this->hashFilter.selection.column);

    auto it = this->index->hashTable.find(this->hashFilter.value);
    if (it != this->index->hashTable.end())
    {
        this->activeRow = &it->second;
    }
}

bool HashFilterIterator::getNext()
{
    if (this->activeRow == nullptr) return false;

    auto rowCount = static_cast<int32_t>(this->activeRow->size());
    this->activeRowIndex++;

    for (; this->activeRowIndex < rowCount; this->activeRowIndex++)
    {
        this->rowIndex = static_cast<int32_t>((*this->activeRow)[this->activeRowIndex]);
        if (passesFilters())
        {
            return true;
        }
    }

    return false;
}
