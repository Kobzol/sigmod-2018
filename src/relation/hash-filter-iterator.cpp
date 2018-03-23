#include "hash-filter-iterator.h"
#include "../database.h"

// TODO: for more equal filters, pick the one with lowest amount of rows
HashFilterIterator::HashFilterIterator(ColumnRelation* relation, uint32_t binding,
                                       const std::vector<Filter>& filters, int equalsIndex)
        : FilterIterator(relation, binding, filters)
{
    if (equalsIndex != -1)
    {
        this->hashFilter = this->filters[equalsIndex];
        std::swap(this->filters[0], this->filters[equalsIndex]);
        this->startFilterIndex = 1;
        this->index = database.getHashIndex(this->hashFilter.selection.relation, this->hashFilter.selection.column);

        auto it = this->index->hashTable.find(this->hashFilter.value);
        if (it != this->index->hashTable.end())
        {
            this->activeRow = &it->second;
            this->rowCount = static_cast<int32_t>(it->second.size());
        }
    }
}

bool HashFilterIterator::getNext()
{
    if (this->activeRow == nullptr) return false;

    this->activeRowIndex++;

    for (; this->activeRowIndex < rowCount; this->activeRowIndex++)
    {
        this->rowIndex = static_cast<int32_t>((*this->activeRow)[this->activeRowIndex]);
        if (passesFilters())
        {
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
            return true;
        }
    }

    return false;
}

bool HashFilterIterator::skipSameValue(const Selection& selection)
{
    if (selection == this->hashFilter.selection)
    {
        this->activeRow = nullptr;
        return false;
    }
    return this->getNext();
}

void HashFilterIterator::iterateValue(const Selection& selection, uint64_t value)
{
    this->index = database.getHashIndex(selection.relation, selection.column);
    auto it = this->index->hashTable.find(value);
    if (it != this->index->hashTable.end())
    {
        this->activeRow = &it->second;
        this->rowCount = static_cast<int32_t>(it->second.size());
        this->activeRowIndex = -1;
    }
}

void HashFilterIterator::prepareIndexedAccess(const Selection& selection)
{
    this->activeRow = nullptr;
    this->startFilterIndex = 0;
}

int64_t HashFilterIterator::predictSize()
{
    return this->rowCount;
}
