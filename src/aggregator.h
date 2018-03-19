#pragma once

#include <string>
#include <unordered_map>
#include <sstream>
#include <cassert>
#include <algorithm>
#include <xmmintrin.h>

#include "query.h"
#include "database.h"
#include "settings.h"

static void sum(uint64_t* __restrict__ target, const uint64_t* __restrict__ column, int rows)
{
    for (int i = 0; i < rows; i++)
    {
        *target += column[i];
    }
}

class Aggregator
{
public:
    void aggregate(Database& database, Query& query, Iterator* root)
    {
        std::unordered_map<SelectionId, Selection> selectionMap;
        for (auto& sel: query.selections)
        {
            selectionMap[sel.getId()] = sel;
        }

        root->requireSelections(selectionMap);

        std::vector<Selection> selections;
        std::unordered_map<uint32_t, uint32_t> backMap;
        for (auto& sel: selectionMap)
        {
            backMap[sel.second.getId()] = static_cast<unsigned int>(selections.size());
            selections.push_back(sel.second);
        }

        size_t count = 0;
        std::vector<uint64_t> results(static_cast<size_t>(selectionMap.size()));

        size_t rows = 0;
        auto columnSize = static_cast<int>(selections.size());
        std::vector<uint64_t*> columns(selections.size());

        root->prepareBlockAccess(selections);
        while (root->getBlock(columns, rows))
        {
            for (int i = 0; i < columnSize; i++)
            {
                sum(&results[i], columns[i], static_cast<int32_t>(rows));
            }
            count += rows;
            rows = 0;
        }

#ifdef STATISTICS
        std::stringstream planStream;
        root->dumpPlan(planStream);
        query.plan = planStream.str();
#endif

#ifdef COLLECT_JOIN_SIZE
        root->assignJoinSize(database);
#endif

        std::stringstream ss;
        for (auto& sel: query.selections)
        {
            if (count == 0)
            {
                ss << "NULL ";
            }
            else
            {
                uint64_t result = results[backMap[sel.getId()]];
                ss << std::to_string(result) << ' ';
            }
        }

        query.result = ss.str();
    }
};
