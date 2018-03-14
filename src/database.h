#pragma once

#include <functional> 
#include <algorithm>
#include <vector>
#include <mutex>

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

    void addJoinSize(const Join& join, int64_t size);

    std::string createJoinKey(const Join& join);

    std::vector<ColumnRelation> relations;
    std::vector<MaxdiffHistogram> histograms;

    std::vector<std::unique_ptr<HashIndex>> hashIndices;
    std::vector<std::unique_ptr<SortIndex>> sortIndices;

    std::unordered_map<std::string, int64_t> joinSizeMap;
    std::mutex joinMapMutex;
};

extern Database database;
