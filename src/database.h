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

    HashIndex& getHashIndex(uint32_t relation, uint32_t column);

    std::unordered_map<SelectionId, std::unique_ptr<HashIndex>> hashIndices;
};

extern Database database;
