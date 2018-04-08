#include <iostream>
#include <cstring>
#include <cstddef>
#include <unordered_set>
#include <limits>
#include <unordered_map>
#include <memory>
#include <sstream>
#include <algorithm>

#include "settings.h"
#include "database.h"
#include "executor.h"
#include "io.h"
#include "stats.h"
#include "timer.h"
#include "join/hash-joiner.h"
#include "index/index-thread-pool.h"
#include "foreign-key/foreign-key-checker.h"
#include "rewrite/rewrite.h"
#include "index/index-builder.h"

Database database;
int THREAD_COUNT;

static void buildIndices(std::vector<Query>& queries)
{
#ifdef STATISTICS
    Timer indexBuildTimer;
#endif

#ifdef USE_PRIMARY_INDEX
    std::unordered_set<uint32_t> needIndices;

    for (auto& query: queries)
    {
        for (auto& join: query.joins)
        {
            for (auto& pred: join)
            {
                for (auto& sel: pred.selections)
                {
                    if (!database.hasIndexedIterator(sel))
                    {
                        needIndices.insert(database.getGlobalColumnId(sel.relation, sel.column));
                    }
                }
            }
        }
    }

    std::vector<uint32_t> indices(needIndices.begin(), needIndices.end());
    auto count = static_cast<int32_t>(indices.size());

    if (count > 0)
    {
        int innerThreads = 10;
        if (count < 4)
        {
            innerThreads = THREAD_COUNT / count;
        }

        count = std::min(count, 6);

        IndexBuilder builder;
        builder.buildIndices(indices, count, innerThreads);
    }
#endif

#ifdef STATISTICS
    indexBuildTime += indexBuildTimer.get();
    Timer queryRewriteTimer;
#endif

    for (auto& query: queries)
    {
        rewriteQuery(query);
    }

#ifdef STATISTICS
    queryRewriteTime += queryRewriteTimer.get();
#endif
}

int main(int argc, char** argv)
{
    std::ios::sync_with_stdio(false);
    THREAD_COUNT = std::thread::hardware_concurrency();

#ifdef LOAD_FROM_FILE
    freopen(LOAD_FROM_FILE, "r", stdin);
#else
    if (getenv("VTUNE") != nullptr)
    {
        freopen(INPUT_FILE, "r", stdin);
    }
#endif

#ifdef STATISTICS
    Timer loadTimer;
#endif
    loadDatabase(database);
#ifdef STATISTICS
    std::cerr << "Relation load time: " << relationLoadTime << std::endl;
    std::cerr << "Transpose time: " << transposeTime << std::endl;
    std::cerr << "Indices init time: " << indicesInitTime << std::endl;
    std::cerr << "Start index time: " << startIndexTime << std::endl;
    std::cerr << "Histogram time: " << histogramTime << std::endl;
    std::cerr << "Relation load total time: " << loadTimer.get() << std::endl;
#endif

#ifndef REAL_RUN
    std::cout << "Ready" << std::endl;
#endif

#ifdef STATISTICS
    Timer queryLoadTimer;
    std::vector<Query> allQueries;
    std::unordered_map<std::string, uint32_t> cachedJoins;
    size_t aggregatableQueries = 0;
#endif

    std::unordered_set<uint32_t> joinColumns;

    std::string line;
    std::vector<Query> queries;
    while (std::getline(std::cin, line))
    {
        if (NOEXPECT(line[0] == 'F'))
        {
            auto queryCount = static_cast<int32_t>(queries.size());
            buildIndices(queries);

            std::vector<Executor> executors;
            for (int i = 0; i < static_cast<int32_t>(queries.size()); i++)
            {
                executors.emplace_back(queries[i]);
            }

            #pragma omp parallel for
            for (int i = 0; i < static_cast<int32_t>(executors.size()); i++)
            {
                executors[i].buildPlan(database, PARALLEL_JOIN_SPLIT);
            }

            std::vector<MultiWrapperIterator*> iterators;
            for (auto& executor: executors)
            {
                iterators.insert(iterators.end(), executor.roots.begin(), executor.roots.end());
            }

            auto iteratorCount = static_cast<int>(iterators.size());
#ifdef USE_THREADS
            #pragma omp parallel for schedule(dynamic)
            for (int i = 0; i < iteratorCount; i++)
#else
            for (int i = 0; i < iteratorCount; i++)
#endif
            {
                iterators[i]->execute();
            }

            for (int i = 0; i < static_cast<int32_t>(executors.size()); i++)
            {
                executors[i].finalizeQuery();
            }

            for (auto& q: queries)
            {
                std::cout << q.result;
            }

#ifdef STATISTICS
            batchCount++;
            allQueries.insert(allQueries.end(), queries.begin(), queries.end());
#endif

            std::cout << std::flush;
            queries.clear();
        }
        else
        {
#ifdef STATISTICS
            queryLoadTimer.reset();
#endif
            queries.emplace_back();
            loadQuery(queries.back(), line);

#ifdef STATISTICS
            queryLoadTime += queryLoadTimer.get();

            auto& query = queries.back();
            query.id = queryCount;
            queryCount++;
            joinCount += query.joins.size();
            filterCount += query.filters.size();

            for (auto& join : query.joins)
            {
                columnsPerJoin += join.size();
                if (join.size() > 1)
                {
                    multipleColumnsPerRelJoins++;
                }

                if (join[0].selections[0].column == 0 || join[0].selections[1].column == 0)
                {
                    joinsOnFirstColumn++;
                }

                bool cacheable = true;
                for (auto& predicate : join)
                {
                    for (auto& filter : query.filters)
                    {
                        if (filter.selection == predicate.selections[0] ||
                            filter.selection == predicate.selections[1])
                        {
                            cacheable = false;
                            break;
                        }
                    }

                    auto& l = predicate.selections[0];
                    auto& r = predicate.selections[1];

                    joinColumns.insert(l.relation * 100 + l.column);
                    joinColumns.insert(r.relation * 100 + r.column);
                }

                if (cacheable)
                {
                    cachedJoins[database.createJoinKey(join)]++;
                }
            }

            selfJoinCount += query.selfJoins.size();

            for (auto& filter: query.filters)
            {
                if (filter.selection.column == 0)
                {
                    filtersOnFirstColumn++;
                }
                if (filter.oper == '=')
                {
                    filterEqualsCount++;
                }
            }

            if (query.isAggregable()) aggregatableQueries++;
#endif
        }
    }

#ifdef USE_INDEX_THREADPOOL
    threadIndexPool.stop();
#endif

#ifdef STATISTICS
    for (auto& query: allQueries)
    {
        if (query.impossible) skippedJoins++;
        for (auto& filter: query.filters)
        {
            if (filter.isSkippable())
            {
                filtersSkippedByHistogram++;
            }
        }
    }

    std::cerr << "Index min max time: " << indexMinMaxTime << std::endl;
    std::cerr << "Index group count time: " << indexGroupCountTime << std::endl;
    std::cerr << "Index copy to buckets time: " << indexCopyToBucketsTime << std::endl;
    std::cerr << "Index sort time: " << indexSortTime << std::endl;
    std::cerr << "Index finalize time: " << indexFinalizeTime << std::endl;
    std::cerr << "Index build time: " << indexBuildTime << std::endl;
    std::cerr << "Query rewrite time: " << queryRewriteTime << std::endl;

    std::cerr << "Skipped plans: " << plansSkipped << std::endl;
    std::cerr << "Skippable by FK: " << skippableFK << std::endl;
    /*std::cerr << "Skipped joins: " << skippedJoins << std::endl;
    std::cerr << "Filters skippable by histogram: " << filtersSkippedByHistogram << std::endl;
    std::cerr << "Join columns: " << joinColumns.size() << std::endl;
    std::cerr << "Joins filtered by min max: " << joinsFilteredByMinMax << std::endl;
    std::cerr << "Aggregatable queries: " << aggregatableQueries << std::endl;
    std::cerr << "Filter equals joined: " << filterEqualsJoined << std::endl;
    std::cerr << "Join one unique: " << joinOneUnique << std::endl;
    std::cerr << "Join both unique: " << joinBothUnique << std::endl;*/

    std::sort(allQueries.begin(), allQueries.end(), [](const Query& a, const Query& b) {
        return a.time > b.time;
    });

    for (int i = 0; i < std::min(static_cast<int32_t>(allQueries.size()), 4); i++)
    {
        std::cerr << allQueries[i].time << "ms, " << allQueries[i].input << ' ';
        std::cerr << allQueries[i].plan << ' ' << allQueries[i].result;
        /*for (auto& j: allQueries[i].joins)
        {
            for (auto& p: j)
            {
                for (int k = 0; k < 2; k++)
                {
                    auto& s = p.selections[k];
                    auto min = database.getMinValue(s.relation, s.column);
                    auto max = database.getMaxValue(s.relation, s.column);
                    std::cerr << s.binding << '.' << s.column << "[" << min << ',' << max << "]";
                    if (k == 0) std::cerr << '=';
                }
                std::cerr << "  ";
            }
        }
        std::cerr << std::endl;
        for (auto& f: allQueries[i].filters)
        {
            std::cerr << f.selection.binding << "." << f.selection.column << f.oper << f.value << " (" << f.valueMax << ") ";
        }
        std::cerr << std::endl;*/
    }

    /*std::vector<std::pair<std::string, uint32_t>> cachedList;
    for (auto& kv: cachedJoins)
    {
        cachedList.emplace_back(kv.first, kv.second);
    }

    std::sort(cachedList.begin(), cachedList.end(), [](auto& a, auto& b) {
        return a.second > b.second;
    });
    for (auto& kv: cachedList)
    {
        std::cerr << kv.first << ' ' << kv.second << "x";

        auto key = kv.first;
        auto size = database.joinSizeMap.find(key);
        if (size != database.joinSizeMap.end())
        {
            std::cerr << " (" << size->second << ")";
        }
        std::cerr << std::endl;
    }*/

    /*size_t relationCount = database.relations.size();
    std::cerr << "Query load time: " << queryLoadTime << std::endl;
    std::cerr << "ColumnRelation count: " << relationCount << std::endl;
    std::cerr << "Min tuple count: " << minTuples << std::endl;
    std::cerr << "Max tuple count: " << maxTuples << std::endl;
    std::cerr << "Avg tuple count: " << tupleCount / (double) relationCount << std::endl;
    std::cerr << "Min column count: " << minColumns << std::endl;
    std::cerr << "Max column count: " << maxColumns << std::endl;
    std::cerr << "Avg column count: " << columnCount / (double) relationCount << std::endl;
    std::cerr << "Query count: " << queryCount << std::endl;
    std::cerr << "Join count: " << joinCount << std::endl;
    std::cerr << "Filter count: " << filterCount << std::endl;
    std::cerr << "Equals filter count: " << filterEqualsCount << std::endl;
    std::cerr << "Batch count: " << batchCount << std::endl;
    std::cerr << "Multiple-columns for joins: " << multipleColumnsPerRelJoins << std::endl;
    std::cerr << "Avg columns for join: " << columnsPerJoin / (double) joinCount << std::endl;
    std::cerr << "Sorted on first column: " << sortedOnFirstColumn << std::endl;
    std::cerr << "Joins on first column: " << joinsOnFirstColumn << std::endl;
    std::cerr << "Filters on first column: " << filtersOnFirstColumn << std::endl;
    std::cerr << "Self-join count: " << selfJoinCount << std::endl;
    std::cerr << std::endl;*/

    std::cerr << std::flush;
#endif

    std::quick_exit(0);
}
