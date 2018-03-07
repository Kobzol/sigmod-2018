#include "executor.h"
#include "relation/filter-iterator.h"
#include "join/hash-joiner.h"
#include "aggregator.h"
#include "join/nested-joiner.h"

void Executor::executeQuery(Database& database, Query& query)
{
    std::unordered_map<uint32_t, Iterator*> views;
    std::vector<std::unique_ptr<Iterator>> container;

    this->createViews(database, query, views, container);
    auto root = this->createRootView(database, query, views, container);

    Aggregator aggregator;
    aggregator.aggregate(database, query, root);
    query.result[query.result.size() - 1] = '\n';
}

void Executor::createViews(Database& database,
                           const Query& query,
                           std::unordered_map<uint32_t, Iterator*>& views,
                           std::vector<std::unique_ptr<Iterator>>& container)
{
    for (auto& filter: query.filters)
    {
        auto binding = filter.selection.binding;
        auto it = views.find(binding);
        if (it == views.end())
        {
            container.push_back(std::move(std::make_unique<FilterIterator>(
                    &database.relations[filter.selection.relation],
                    binding
            )));
            it = views.insert({ binding, container.back().get() }).first;
        }
        dynamic_cast<FilterIterator*>(it->second)->filters.push_back(filter);
    }

    uint32_t binding = 0;
    for (auto& relation: query.relations)
    {
        auto it = views.find(binding);
        if (it == views.end())
        {
            container.push_back(std::move(std::make_unique<ColumnRelationIterator>(
                    &database.relations[relation],
                    binding
            )));
            views.insert({ binding, container.back().get() });
        }
        binding++;
    }
}

Iterator* Executor::createRootView(Database& database, Query& query,
                               std::unordered_map<uint32_t, Iterator*>& views,
                               std::vector<std::unique_ptr<Iterator>>& container)
{
    if (query.joins.size() > 1)
    {
        sort(query.joins.begin(), query.joins.end(), [](const Join& a, const Join& b) {
            if (a.selections[0].binding < b.selections[0].binding) return true;
            if (a.selections[0].binding > b.selections[0].binding) return false;
            return a.selections[1].binding <= b.selections[1].binding;
        });
    }

    std::vector<Join> joins;
    size_t index = 0;
    size_t joinSize = query.joins.size();
    Iterator* root = nullptr;

    while (index < joinSize)
    {
        joins.push_back(query.joins[index++]);
        uint32_t lowerRelation = joins.back().selections[0].binding;
        uint32_t higherRelation = joins.back().selections[1].binding;

        while (index < joinSize)
        {
            if (query.joins[index].selections[0].binding == lowerRelation &&
                query.joins[index].selections[1].binding == higherRelation)
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
        std::vector<uint32_t> bindings;
        joiner->fillBindings(bindings);
        for (auto binding: bindings)
        {
            views[binding] = joiner.get();
        }

        root = joiner.get();
        container.push_back(std::move(joiner));
        joins.clear();
    }

    return root;
}
