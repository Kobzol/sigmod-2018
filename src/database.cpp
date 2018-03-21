#include "database.h"
#include "relation/primary-index-iterator.h"
#include "relation/sort-index-iterator.h"

template <typename T>
T* Database::getIndex(const std::vector<std::unique_ptr<T>>& indices, uint32_t relation, uint32_t column)
{
    auto& index = indices[this->getGlobalColumnId(relation, column)];
    if (index->buildCompleted)
    {
        return index.get();
    }
    return nullptr;
}

uint32_t Database::getGlobalColumnId(uint32_t relation, uint32_t column)
{
    return this->relations[relation].cumulativeColumnId + column;
}

HashIndex* Database::getHashIndex(uint32_t relation, uint32_t column)
{
    return this->getIndex(this->hashIndices, relation, column);
}
SortIndex* Database::getSortIndex(uint32_t relation, uint32_t column)
{
    return this->getIndex(this->sortIndices, relation, column);
}
PrimaryIndex* Database::getPrimaryIndex(uint32_t relation, uint32_t column)
{
    return this->getIndex(this->primaryIndices, relation, column);
}
AggregateIndex* Database::getAggregateIndex(uint32_t relation, uint32_t column)
{
    return this->getIndex(this->aggregateIndices, relation, column);
}

void Database::addJoinSize(const Join& join, int64_t size)
{
    auto key = this->createJoinKey(join);

    std::unique_lock<decltype(this->joinMapMutex)> lock(this->joinMapMutex);
    this->joinSizeMap[key] = size;
}

std::string Database::createJoinKey(const Join& join)
{
    std::stringstream key;
    for (auto& predicate : join)
    {
        int first = std::min(predicate.selections[0].relation, predicate.selections[1].relation) ==
                    predicate.selections[0].relation ? 0 : 1;
        int second = 1 - first;
        key << predicate.selections[first].relation << '.' << predicate.selections[first].column;
        key << '=';
        key << predicate.selections[second].relation << '.' << predicate.selections[second].column;
    }
    return key.str();
}

std::unique_ptr<Iterator> Database::createIndexedIterator(ColumnRelation& relation, uint32_t binding,
                                                          const std::vector<Filter>& filters)
{
    auto primary = this->getPrimaryIndex(relation.id, 0);
    if (primary != nullptr) return std::make_unique<PrimaryIndexIterator>(&relation, binding, filters);
    return std::make_unique<SortIndexIterator>(&relation, binding, filters);
}

std::unique_ptr<Iterator> Database::createFilteredIterator(ColumnRelation& relation, uint32_t binding,
                                                           const std::vector<Filter>& filters)
{
#ifdef USE_SEQUENTIAL_FILTER
    return std::make_unique<FilterIterator>(&relation, binding, filters);
#endif
    return this->createIndexedIterator(relation, binding, filters);
}
