#include "database.h"

HashIndex& Database::getHashIndex(uint32_t relation, uint32_t column)
{
    return *this->hashIndices[this->getGlobalColumnId(relation, column)];
}

uint32_t Database::getGlobalColumnId(uint32_t relation, uint32_t column)
{
    return this->relations[relation].cumulativeColumnId + column;
}
