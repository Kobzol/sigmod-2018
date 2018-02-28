#include "executor.h"
#include "relation/filter-view.h"
#include "join/hash-joiner.h"
#include "aggregator.h"

#include <unordered_map>
#include <memory>
#include <algorithm>
#include <sstream>

std::string Executor::executeQuery(Database& database, Query& query)
{
    std::unordered_map<uint32_t, View*> views;
    std::vector<std::unique_ptr<View>> container;

    this->createFilterViews(database, query, views, container);
    this->join(database, query, views, container);

    return this->aggregate(database, query, views);

}

void Executor::join(Database& database, Query& query, std::unordered_map<uint32_t, View*>& views,
                    std::vector<std::unique_ptr<View>>& container)
{
    HashJoiner joiner;
    sort(query.joins.begin(), query.joins.end(), [](const Join& a, const Join& b) {
        if (a.selections[0].relation < b.selections[0].relation) return true;
        if (a.selections[0].relation > b.selections[0].relation) return false;
        return a.selections[1].relation <= b.selections[1].relation;
    });

    std::vector<Join> joins;
    size_t index = 0;
    size_t joinSize = query.joins.size();
    while (index < joinSize)
    {
        joins.push_back(query.joins[index++]);
        uint32_t lowerRelation = joins.back().selections[0].relation;
        uint32_t higherRelation = joins.back().selections[1].relation;

        while (index < joinSize)
        {
            if (query.joins[index].selections[0].relation == lowerRelation &&
                query.joins[index].selections[1].relation == higherRelation)
            {
                joins.push_back(query.joins[index]);
                index++;
            }
            else break;
        }

        if (views.find(lowerRelation) == views.end()) views.insert({ lowerRelation, &database.relations[lowerRelation] });
        if (views.find(higherRelation) == views.end()) views.insert({ higherRelation, &database.relations[higherRelation] });

        container.push_back(joiner.join(
                *views[lowerRelation],
                *views[higherRelation],
                joins
        ));
        views[lowerRelation] = container.back().get();
        views[higherRelation] = container.back().get();
        joins.clear();
    }
}

std::string Executor::aggregate(Database& database, const Query& query,
                                std::unordered_map<uint32_t, View*>& views)
{
    std::unordered_map<std::string, std::string> cache;

    std::stringstream ss;
    for (auto& select : query.selections)
    {
        auto it = views.find(select.relation);
        if (it == views.end())
        {
            it = views.insert({ select.relation, &database.relations[select.relation] }).first;
        }

        auto key = std::to_string(select.relation) + '/' + std::to_string(select.column);
        auto cacheIter = cache.find(key);
        std::string result;

        if (cacheIter == cache.end())
        {
            Aggregator aggregator;
            result = aggregator.aggregate(select, *it->second);
            cache.insert({ key, result }).first;
        }
        else result = cacheIter->second;

        ss << result << ' ';
    }

    return ss.str();
}

void Executor::createFilterViews(Database& database,
                                 const Query& query,
                                 std::unordered_map<uint32_t, View*>& views,
                                 std::vector<std::unique_ptr<View>>& container)
{
    for (auto& filter: query.filters)
    {
        auto relation = filter.selection.relation;
        auto it = views.find(relation);
        if (it == views.end())
        {
            container.push_back(std::move(std::make_unique<FilterView>(&database.relations[relation])));
            it = views.insert({ relation, container.back().get() }).first;
        }
        dynamic_cast<FilterView*>(it->second)->filters.push_back(filter);
    }
}
