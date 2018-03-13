#pragma once

#include <vector>

#include "relation/column-relation.h"
#include "index/hash-index.h"
#include "index/sort-index.h"
#include "relation/maxdiff_histogram.h"

class Database
{
public:
    Database() = default;
    DISABLE_COPY(Database);

    uint32_t getGlobalColumnId(uint32_t relation, uint32_t column);

    HashIndex& getHashIndex(uint32_t relation, uint32_t column);
    SortIndex& getSortIndex(uint32_t relation, uint32_t column);

    std::vector<ColumnRelation> relations;
    std::vector<MaxdiffHistogram> histograms;

    std::vector<std::unique_ptr<HashIndex>> hashIndices;
    std::vector<std::unique_ptr<SortIndex>> sortIndices;

    // TODO: look up join result cache
    int64_t predictSize(Join& join)
    {
        return this->relations[join[0].selections[0].relation].getRowCount() *
                this->relations[join[0].selections[1].relation].getRowCount();
    }
};

extern Database database;
