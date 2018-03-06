#pragma once

#include <string>
#include <unordered_map>
#include <sstream>
#include <cassert>
#include <algorithm>

#include "relation/view.h"
#include "query.h"
#include "database.h"

struct SumData
{
public:
    SumData() = default;
    SumData(View* view, uint64_t sum, size_t count): view(view), sum(sum), count(count)
    {

    }

    View* view;
    uint64_t sum;
    size_t count;
};

class Aggregator
{
public:
    // TODO: aggregate results from same root on multiple columns
    std::string aggregate(Database& database, const Query& query,
                          std::unordered_map<uint32_t, View*>& views)
    {
        std::unordered_map<uint32_t, SumData> cache;
        std::vector<View*> roots;
        for (auto& kv: views)
        {
            if (std::find(roots.begin(), roots.end(), kv.second) == roots.end())
            {
                roots.push_back(kv.second);
            }
        }

        std::vector<SumData> results(query.selections.size());
        int i = 0;
        for (auto& select : query.selections)
        {
            auto it = views.find(select.relation);
            assert(it != views.end());

            auto key = select.getId();
            auto cacheIter = cache.find(key);
            if (cacheIter == cache.end())
            {
                View* view = it->second;
                results[i] = this->sum(view, select);
                cache.insert({ key, results[i] });
            }
            else results[i] = cacheIter->second;
            i++;
        }

        std::stringstream ss;
        for (auto& result: results)
        {
            if (result.count == 0)
            {
                ss << "NULL ";
            }
            else
            {
                size_t multiplier = 1;
                for (auto& root : roots)
                {
                    if (root != result.view)
                    {
                        multiplier *= this->rootMap[root];
                    }
                }

                ss << std::to_string(multiplier * result.sum) << ' ';
            }
        }

        return ss.str();
    }

    SumData sum(View* view, const Selection& selection)
    {
        auto iterator = view->createIterator();
        size_t sum = 0;
        size_t count = 0;

        while (iterator->getNext())
        {
            sum += iterator->getValue(selection);
            count++;
        }

        this->rootMap[view] = count;
        return SumData{view, sum, count};
    }

    std::unordered_map<View*, size_t> rootMap;
};
