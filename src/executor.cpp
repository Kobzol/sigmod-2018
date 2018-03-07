#include "executor.h"

#include <unordered_set>
#include <algorithm>
#include <iostream>

#include "relation/filter-iterator.h"
#include "join/hash-joiner.h"
#include "aggregator.h"
#include "settings.h"
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
#ifdef SORT_JOINS_BY_SIZE
    std::sort(query.joins.begin(), query.joins.end(), [&database](const Join& a, const Join& b) {
        auto& relLeft = database.relations[a[0].selections[0].relation];
        auto& relRight = database.relations[b[0].selections[0].relation];
        return relLeft.getRowCount() <= relRight.getRowCount();
    });
#endif

    auto* join = &query.joins[0];

    auto leftBinding = (*join)[0].selections[0].binding;
    auto rightBinding = (*join)[0].selections[1].binding;
    assert(leftBinding <= rightBinding);

    container.push_back(std::move(std::make_unique<HashJoiner>(
            views[leftBinding],
            views[rightBinding],
            0,
            *join
    )));

    std::unordered_set<uint32_t> usedBindings = { leftBinding, rightBinding };
    Iterator* root = container.back().get();
    for (int i = 1; i < static_cast<int32_t>(query.joins.size()); i++)
    {
        join = &query.joins[i];
        leftBinding = (*join)[0].selections[0].binding;
        rightBinding = (*join)[0].selections[1].binding;

        auto usedLeft = usedBindings.find(leftBinding) != usedBindings.end();
        auto usedRight = usedBindings.find(rightBinding) != usedBindings.end();
        Iterator* left = root;
        Iterator* right;
        uint32_t leftIndex = 0;

        if (usedLeft)
        {
            right = views[rightBinding];
            usedBindings.insert(rightBinding);
        }
        else if (usedRight)
        {
            right = views[leftBinding];
            usedBindings.insert(leftBinding);
            leftIndex = 1;
        }
        else
        {
            query.joins.push_back(*join);
            continue;
        }

        std::unique_ptr<Iterator> newJoin = std::make_unique<HashJoiner>(
                left,
                right,
                leftIndex,
                *join
        );

        root = newJoin.get();
        container.push_back(std::move(newJoin));
    }

    return root;
}
