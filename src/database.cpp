#include "database.h"

uint32_t Database::getGlobalColumnId(uint32_t relation, uint32_t column)
{
    return this->relations[relation].cumulativeColumnId + column;
}

HashIndex& Database::getHashIndex(uint32_t relation, uint32_t column)
{
    return *this->hashIndices[this->getGlobalColumnId(relation, column)];
}

SortIndex& Database::getSortIndex(uint32_t relation, uint32_t column)
{
    return *this->sortIndices[this->getGlobalColumnId(relation, column)];
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
