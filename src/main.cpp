#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cstring>
#include <cstddef>
#include <unordered_set>
#include <limits>
#include <unordered_map>
#include <memory>
#include <sstream>
#include <algorithm>
#include <cassert>

#include "settings.h"
#include "database.h"
#include "executor.h"
#include "io.h"
#include "stats.h"

int main(int argc, char** argv)
{
    std::ios::sync_with_stdio(false);

#ifdef LOAD_FROM_FILE
    freopen(LOAD_FROM_FILE, "r", stdin);
#else
    if (getenv("VTUNE") != nullptr)
    {
        freopen(INPUT_FILE, "r", stdin);
    }
#endif

    Database database;
    loadDatabase(database);

#ifndef REAL_RUN
    std::cout << "Ready" << std::endl;
#endif

    Executor executor;
    std::string line;
    std::vector<Query> queries;
    while (std::getline(std::cin, line))
    {
        if (line[0] == 'F')
        {
            auto queryCount = static_cast<int32_t>(queries.size());

            #pragma omp parallel for
            for (int i = 0; i < queryCount; i++)
            {
                executor.executeQuery(database, queries[i]);
            }
            for (int i = 0; i < queryCount; i++)
            {
                std::cout << queries[i].result;
            }

            std::cout << std::flush;
            queries.clear();
#ifdef STATISTICS
            batchCount++;
#endif
        }
        else
        {
            queries.emplace_back();
            loadQuery(queries.back(), line);

#ifdef STATISTICS
            auto& query = queries.back();
            queryCount++;
            joinCount += query.joins.size();
            filterCount += query.filters.size();

            std::unordered_set<std::string> pairs;
            std::unordered_set<int> joinedRelations;
            size_t multipleColumns = 1;

            for (auto& join : query.joins)
            {
                auto smaller = std::min(join[0].left.relation, join[0].right.relation);
                auto larger = std::max(join[0].left.relation, join[0].right.relation);
                auto key = std::to_string(smaller) + "." + std::to_string(larger);
                if (pairs.find(key) != pairs.end())
                {
                    multipleColumns++;
                }
                pairs.insert(key);

                joinedRelations.insert(smaller);
                joinedRelations.insert(larger);

                if (join[0].left.column == 0 || join[0].right.column == 0)
                {
                    joinsOnFirstColumn++;
                }
            }

            for (auto& filter: query.filters)
            {
                if (filter.selection.column == 0)
                {
                    filtersOnFirstColumn++;
                }
            }

            columnsPerJoin += multipleColumns;
            if (multipleColumns > 1)
            {
                multipleColumnsPerRelJoins++;
            }
#endif
        }
    }

#ifdef STATISTICS
    size_t relationCount = database.relations.size();
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
    std::cerr << "Batch count: " << batchCount << std::endl;
    std::cerr << "Multiple-columns for joins: " << multipleColumnsPerRelJoins << std::endl;
    std::cerr << "Avg columns for join: " << columnsPerJoin / (double) queryCount << std::endl;
    std::cerr << "Sorted on first column: " << sortedOnFirstColumn << std::endl;
    std::cerr << "Joins on first column: " << joinsOnFirstColumn << std::endl;
    std::cerr << "Filters on first column: " << filtersOnFirstColumn << std::endl;
#endif

    std::quick_exit(0);
}
