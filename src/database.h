#pragma once

#include <vector>

#include "relation/column-relation.h"
#include "index/hash-index.h"
#include "index/sort-index.h"

class Database
{
public:
    Database() = default;
    DISABLE_COPY(Database);

    uint32_t getGlobalColumnId(uint32_t relation, uint32_t column);

    HashIndex& getHashIndex(uint32_t relation, uint32_t column);
    SortIndex& getSortIndex(uint32_t relation, uint32_t column);

    std::vector<ColumnRelation> relations;

    std::vector<std::unique_ptr<HashIndex>> hashIndices;
    std::vector<std::unique_ptr<SortIndex>> sortIndices;
};

extern Database database;
