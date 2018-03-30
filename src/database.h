#pragma once

#include <vector>
#include <mutex>

#include "relation/column-relation.h"
#include "index/hash-index.h"
#include "index/sort-index.h"
#include "relation/maxdiff_histogram.h"
#include "index/primary-index.h"
#include "index/aggregate-index.h"

class Database
{
public:
    Database() = default;
    DISABLE_COPY(Database);

    uint32_t getGlobalColumnId(uint32_t relation, uint32_t column);

    HashIndex* getHashIndex(uint32_t relation, uint32_t column);
    SortIndex* getSortIndex(uint32_t relation, uint32_t column);
    PrimaryIndex* getPrimaryIndex(uint32_t relation, uint32_t column);
    AggregateIndex* getAggregateIndex(uint32_t relation, uint32_t column);

    bool hasIndexedIterator(const Selection& selection);
    std::unique_ptr<Iterator> createIndexedIterator(const Selection& selection, const std::vector<Filter>& filters);
    std::unique_ptr<Iterator> createFilteredIterator(const Selection& selection, const std::vector<Filter>& filters);

    void addJoinSize(const Join& join, int64_t size);

    std::string createJoinKey(const Join& join);

    uint64_t getMinValue(uint32_t relation, uint32_t column);
    uint64_t getMaxValue(uint32_t relation, uint32_t column);

    bool isUnique(const Selection& selection);

    bool isPkFk(const Selection& primary, const Selection& foreign);

    std::vector<ColumnRelation> relations;
#ifdef USE_HISTOGRAM
    std::vector<MaxdiffHistogram> histograms;
#endif

    std::vector<std::unique_ptr<HashIndex>> hashIndices;
    std::vector<std::unique_ptr<SortIndex>> sortIndices;
    std::vector<std::unique_ptr<PrimaryIndex>> primaryIndices;
    std::vector<std::unique_ptr<AggregateIndex>> aggregateIndices;
    std::vector<uint32_t> unique;

    std::unordered_map<std::string, int64_t> joinSizeMap;
    std::mutex joinMapMutex;

private:
    std::unordered_map<std::string, bool> pkFkPairs;

    template<typename T>
    T* getIndex(const std::vector<std::unique_ptr<T>>& indices, uint32_t relation, uint32_t column);
};

extern Database database;
