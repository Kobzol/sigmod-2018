#include "executor.h"

#include <unordered_set>
#include <algorithm>
#include <iostream>

#include "relation/filter-iterator.h"
#include "join/hash-joiner.h"
#include "aggregator.h"
#include "settings.h"
#include "join/nested-joiner.h"
#include "relation/hash-filter-iterator.h"
#include "relation/sort-filter-iterator.h"
#include "join/self-join.h"
#include "timer.h"
#include "join/index-joiner.h"
#include "join/merge-sort-joiner.h"

void Executor::executeQuery(Database& database, Query& query)
{
#ifdef STATISTICS
    Timer timer;
#endif
    std::unordered_map<uint32_t, Iterator*> views;
    std::vector<std::unique_ptr<Iterator>> container;

    this->createViews(database, query, views, container);
    auto root = this->createRootView(database, query, views, container);

    Aggregator aggregator;
    aggregator.aggregate(database, query, root);
    query.result[query.result.size() - 1] = '\n';

#ifdef STATISTICS
    query.time = timer.get();
#endif
}

void Executor::createViews(Database& database,
                           const Query& query,
                           std::unordered_map<uint32_t, Iterator*>& views,
                           std::vector<std::unique_ptr<Iterator>>& container)
{
    std::unordered_map<uint32_t, std::vector<Filter>> filtersByBindings;

    // group filters by binding
    for (auto& filter: query.filters)
    {
        filtersByBindings[filter.selection.binding].push_back(filter);
    }

    // assign a filter iterator for filtered bindings
    for (auto& filterGroup: filtersByBindings)
    {
#ifdef USE_HASH_INDEX
        int equalsIndex = -1;
        for (int i = 0; i < static_cast<int32_t>(filterGroup.second.size()); i++)
        {
            if (filterGroup.second[i].oper == '=')
            {
                equalsIndex = i;
                break;
            }
        }
#endif

        auto binding = filterGroup.first;
        auto relation = &database.relations[filterGroup.second[0].selection.relation];
#ifdef USE_HASH_INDEX
        std::unique_ptr<FilterIterator> filter;
        if (equalsIndex != -1)
        {
            filter = std::make_unique<HashFilterIterator>(relation,
                                                          binding,
                                                          filterGroup.second,
                                                          equalsIndex);
        }
        else filter = std::make_unique<FILTER_ITERATOR>(relation,
                                                        binding,
                                                        filterGroup.second);
#else
        auto filter = std::make_unique<FILTER_ITERATOR>(relation,
                                                        binding,
                                                        filterGroup.second);
#endif
        views.insert({ binding, filter.get() });
        container.push_back(std::move(filter));
    }

    // assign a simple relation iterator for bindings without a filter
    uint32_t binding = 0;
    for (auto& relation: query.relations)
    {
        auto it = views.find(binding);
        if (it == views.end())
        {
            container.push_back(std::make_unique<ColumnRelationIterator>(
                    &database.relations[relation],
                    binding
            ));
            views.insert({ binding, container.back().get() });
        }
        binding++;
    }

#ifdef USE_SELF_JOIN
    // assign self-joins
    for (auto& kv: query.selfJoins)
    {
        auto it = views.find(kv.first);
        container.push_back(std::make_unique<SelfJoin>(
                *it->second,
                kv.second
        ));
        views[binding] = container.back().get();
    }
#endif
}

void createHashJoin(Iterator* left,
                    Iterator* right,
                    uint32_t leftIndex,
                    std::vector<std::unique_ptr<Iterator>>& container,
                    Join* join,
                    bool last)
{
    if (last)
    {
        container.push_back(right->createIndexedIterator());
        right = container.back().get();
    }

    if (join->size() > 1)
    {
        container.push_back(std::make_unique<HashJoiner<true>>(
                left,
                right,
                leftIndex,
                *join
        ));
    }
    else container.push_back(std::make_unique<HashJoiner<false>>(
                left,
                right,
                leftIndex,
                *join
        ));
}
void createIndexJoin(Iterator* left,
                    Iterator* right,
                    uint32_t leftIndex,
                    std::vector<std::unique_ptr<Iterator>>& container,
                    Join* join)
{
    container.push_back(right->createIndexedIterator());
    container.push_back(std::make_unique<IndexJoiner>(
            left,
            container.back().get(),
            leftIndex,
            *join
    ));
}
void createMergesortJoin(Iterator* left,
                         Iterator* right,
                         uint32_t leftIndex,
                         std::vector<std::unique_ptr<Iterator>>& container,
                         Join* join)
{
    if (!left->isJoin())
    {
        container.push_back(left->createIndexedIterator());
        left = container.back().get();
    }

    container.push_back(right->createIndexedIterator());
    container.push_back(std::make_unique<MergeSortJoiner>(
            left,
            container.back().get(),
            leftIndex,
            *join
    ));
}

void createJoin(Iterator* left,
                Iterator* right,
                uint32_t leftIndex,
                std::vector<std::unique_ptr<Iterator>>& container,
                Join* join,
                bool first,
                bool last)
{
#ifndef USE_SORT_INDEX
    createHashJoin(left, right, leftIndex, container, join, false);
    return;
#endif

    if (first || left->isSortedOn((*join)[0].selections[leftIndex]))
    {
        createMergesortJoin(left, right, leftIndex, container, join);
    }
    else
    {
        if (left->predictSize() < 20000)
        {
            createIndexJoin(left, right, leftIndex, container, join);
        }
        else createHashJoin(left, right, leftIndex, container, join, last);
    }
}

Iterator* Executor::createRootView(Database& database, Query& query,
                                   std::unordered_map<uint32_t, Iterator*>& views,
                                   std::vector<std::unique_ptr<Iterator>>& container)
{
#ifdef SORT_JOINS_BY_SIZE
    std::sort(query.joins.begin(), query.joins.end(), [&database, &views](const Join& a, const Join& b) {
        auto& aLeft = database.relations[a[0].selections[0].relation];
        auto& aRight = database.relations[a[0].selections[1].relation];
        auto& bLeft = database.relations[b[0].selections[0].relation];
        auto& bRight = database.relations[b[0].selections[1].relation];
        return (aLeft.getRowCount() + aRight.getRowCount()) > (bLeft.getRowCount() + bRight.getRowCount());
    });
#endif

    auto* join = &query.joins[0];

    auto leftBinding = (*join)[0].selections[0].binding;
    auto rightBinding = (*join)[0].selections[1].binding;

    createJoin(views[leftBinding], views[rightBinding], 0, container, join, true, query.joins.size() == 1);

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

        createJoin(left, right, leftIndex, container, join, false, i == (static_cast<int32_t>(query.joins.size()) - 1));
        root = container.back().get();
    }

    return root;
}
