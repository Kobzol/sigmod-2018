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

        auto map = selectionMap;
        root->requireSelections(selectionMap);

        std::vector<uint32_t> columnIds;
        std::unordered_map<uint32_t, uint32_t> backMap;
        for (auto& sel: map)
        {
            backMap[sel.second.getId()] = static_cast<unsigned int>(columnIds.size());
            columnIds.push_back(root->getColumnForSelection(sel.second));
        }

        size_t count = 0;
        std::vector<uint64_t> results(static_cast<size_t>(selectionMap.size()));
        _mm_prefetch(results.data(), _MM_HINT_T0);

        root->sumRows(results, columnIds, count);

#ifdef STATISTICS
        std::stringstream planStream;
        root->dumpPlan(planStream);
        query.plan = planStream.str();
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
