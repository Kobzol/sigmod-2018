#pragma once

#include <string>
#include <unordered_map>
#include <sstream>
#include <cassert>
#include <algorithm>

#include "query.h"
#include "database.h"

class Aggregator
{
public:
    void aggregate(Database& database, Query& query, Iterator* root)
    {
        auto selectionSize = (int) query.selections.size();

        std::vector<uint64_t> results(static_cast<size_t>(selectionSize));
        size_t count = 0;

        std::vector<uint32_t> columnIds;
        for (auto& selection: query.selections)
        {
            columnIds.push_back(root->getColumnForSelection(selection));
        }

        root->reset();
        while (root->getNext())
        {
            root->sumRow(results, columnIds);
            count++;
        }

        std::stringstream ss;
        for (auto& result: results)
        {
            if (count == 0)
            {
                ss << "NULL ";
            }
            else
            {
                ss << std::to_string(result) << ' ';
            }
        }

        query.result = ss.str();
    }
};
