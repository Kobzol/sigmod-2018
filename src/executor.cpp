#include "executor.h"
#include "relation/filter-view.h"
#include "join/hash-joiner.h"
#include "aggregator.h"
#include "join/nested-joiner.h"

std::string Executor::executeQuery(Database& database, Query& query)
{
    std::unordered_map<uint32_t, View*> views;
    std::vector<std::unique_ptr<View>> container;

    this->createViews(database, query, views, container);
    auto root = this->createRootView(database, query, views, container);

    Aggregator aggregator;
    return aggregator.aggregate(database, query, root);
}

void Executor::createViews(Database& database,
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
    for (auto& relation: query.relations)
    {
        auto it = views.find(relation);
        if (it == views.end())
        {
            views.insert({ relation, &database.relations[relation] });
        }
    }
}

View* Executor::createRootView(Database& database, Query& query,
                               std::unordered_map<uint32_t, View*>& views,
                               std::vector<std::unique_ptr<View>>& container)
{
    if (query.joins.size() > 1)
    {
        sort(query.joins.begin(), query.joins.end(), [](const Join& a, const Join& b) {
            if (a.selections[0].relation < b.selections[0].relation) return true;
            if (a.selections[0].relation > b.selections[0].relation) return false;
            return a.selections[1].relation <= b.selections[1].relation;
        });
    }

    std::vector<Join> joins;
    size_t index = 0;
    size_t joinSize = query.joins.size();
    View* root = nullptr;

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

        auto joiner = std::make_unique<HashJoiner>(
                views[lowerRelation],
                views[higherRelation],
                joins
        );
        std::vector<uint32_t> ids;
        joiner->fillRelationIds(ids);
        for (auto id: ids)
        {
            views[id] = joiner.get();
        }

        root = joiner.get();
        container.push_back(std::move(joiner));
        joins.clear();
    }

    return root;
}
