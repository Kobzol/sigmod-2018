#pragma once

#include <vector>

#include "relation/column-relation.h"
#include "index/hash-index.h"

class Database
{
public:
    Database() = default;
    DISABLE_COPY(Database);

    std::vector<ColumnRelation> relations;

    uint32_t getGlobalColumnId(uint32_t relation, uint32_t column);

    HashIndex& getHashIndex(uint32_t relation, uint32_t column);

    std::vector<std::unique_ptr<HashIndex>> hashIndices;
};

extern Database database;
