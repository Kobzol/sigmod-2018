#pragma once

#include <string>
#include <unordered_map>
#include <sstream>
#include <cassert>
#include <algorithm>

#include "relation/view.h"
#include "query.h"
#include "database.h"

class Aggregator
{
public:
    std::string aggregate(Database& database, const Query& query, View* root)
    {
        auto selectionSize = (int) query.selections.size();

        std::vector<uint64_t> results(static_cast<size_t>(selectionSize));
        size_t count = 0;

        auto iterator = root->createIterator(); // has to be joined
        while (iterator->getNext())
        {
            for (int i = 0; i < selectionSize; i++)
            {
                auto& select = query.selections[i];
                results[i] += iterator->getValue(select);
            }
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

        return ss.str();
    }
};
