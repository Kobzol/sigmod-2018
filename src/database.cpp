#include "database.h"

HashIndex& Database::getHashIndex(uint32_t relation, uint32_t column)
{
    Selection selection(relation, 0, column);
    auto id = selection.getId();

    auto it = this->hashIndices.find(id);
    if (it == this->hashIndices.end())
    {
        it = this->hashIndices.insert(std::make_pair(id, std::make_unique<HashIndex>(
                this->relations[relation],
                column
        ))).first;
        it->second->build();
    }

    return *it->second;
}
