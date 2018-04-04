#include "executor.h"

#include <unordered_set>
#include <algorithm>
#include <iostream>

#include "relation/filter-iterator.h"
#include "join/hash-joiner.h"
#include "aggregation/aggregator.h"
#include "settings.h"
#include "join/nested-joiner.h"
#include "relation/hash-filter-iterator.h"
#include "relation/sort-index-iterator.h"
#include "relation/primary-index-iterator.h"
#include "join/self-join.h"
#include "timer.h"
#include "join/index-joiner.h"
#include "join/merge-sort-joiner.h"
#include "aggregation/aggregated-iterator.h"
#include "aggregation/indexed-aggregated-iterator.h"
#include "stats.h"

void Executor::buildPlan(Database& database, int split)
{
    bool aggregable;
    this->createViews(database, this->query, this->views, this->container, aggregable);
    auto root = this->createRootView(database, this->query, this->views, this->container, aggregable);
    this->prepareRoots(database, this->query, root, aggregable, split);
}

void Executor::createViews(Database& database, const Query& query, std::unordered_map<uint32_t, Iterator*>& views,
                           std::vector<std::unique_ptr<Iterator>>& container, bool& aggregable)
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
        std::sort(filterGroup.second.begin(), filterGroup.second.end());

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
#ifdef USE_HASH_INDEX
        std::unique_ptr<FilterIterator> filter;
        if (equalsIndex != -1)
        {
            TODO: add check for created index
            filter = std::make_unique<HashFilterIterator>(relation,
                                                          binding,
                                                          filterGroup.second,
                                                          equalsIndex);
        }
        else filter = database.createFilteredIterator(filterGroup.second[0].selection, filterGroup.second);
#else
        auto filter = database.createFilteredIterator(filterGroup.second[0].selection, filterGroup.second);
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

    if (query.isAggregable())
    {
        aggregable = true;
        this->createAggregatedViews(query, views, container);
    }
    else aggregable = false;

#ifdef USE_SELF_JOIN
    // assign self-joins
    for (auto& kv: query.selfJoins)
    {
        auto it = views.find(kv.first);
        container.push_back(std::make_unique<SelfJoin>(
                it->second,
                kv.second
        ));
        views[kv.first] = container.back().get();
    }
#endif
}

template <template<bool> typename T>
void createTemplatedJoin(Iterator* left,
                                Iterator* right,
                                uint32_t leftIndex,
                                Join* join,
                                std::vector<std::unique_ptr<Iterator>>& container)
{
    if (join->size() > 1)
    {
        container.push_back(std::make_unique<T<true>>(
                left,
                right,
                leftIndex,
                *join
        ));
    }
    else
    {
        container.push_back(std::make_unique<T<false>>(
                left,
                right,
                leftIndex,
                *join
        ));
    }
}

static void createHashJoin(Iterator* left,
                    Iterator* right,
                    uint32_t leftIndex,
                    std::vector<std::unique_ptr<Iterator>>& container,
                    Join* join,
                    bool last)
{
    if (last && database.hasIndexedIterator((*join)[0].selections[1 - leftIndex]))
    {
        container.push_back(right->createIndexedIterator(container, (*join)[0].selections[1 - leftIndex]));
        right = container.back().get();
    }
    else last = false;

    if (join->size() > 1)
    {
        container.push_back(std::make_unique<HashJoiner<true>>(
                left,
                right,
                leftIndex,
                *join,
                last
        ));
    }
    else
    {
        container.push_back(std::make_unique<HashJoiner<false>>(
                left,
                right,
                leftIndex,
                *join,
                last
        ));
    }
}
static void createIndexJoin(Iterator* left,
                    Iterator* right,
                    uint32_t leftIndex,
                    std::vector<std::unique_ptr<Iterator>>& container,
                    Join* join,
                    bool hasLeftIndex)
{
    if (hasLeftIndex && !left->isJoin())
    {
        container.push_back(left->createIndexedIterator(container, (*join)[0].selections[leftIndex]));
        left = container.back().get();
    }
    else hasLeftIndex = false;

    container.push_back(right->createIndexedIterator(container, (*join)[0].selections[1 - leftIndex]));
    right = container.back().get();

    if (join->size() > 1)
    {
        container.push_back(std::make_unique<IndexJoiner<true>>(
                left,
                right,
                leftIndex,
                *join,
                hasLeftIndex
        ));
    }
    else
    {
        container.push_back(std::make_unique<IndexJoiner<false>>(
                left,
                right,
                leftIndex,
                *join,
                hasLeftIndex
        ));
    }
}
static void createMergesortJoin(Iterator* left,
                                Iterator* right,
                                uint32_t leftIndex,
                                std::vector<std::unique_ptr<Iterator>>& container,
                                Join* join,
                                bool leftSorted)
{
    if (!left->isJoin() && !leftSorted)
    {
        container.push_back(left->createIndexedIterator(container, (*join)[0].selections[leftIndex]));
        left = container.back().get();
    }

    container.push_back(right->createIndexedIterator(container, (*join)[0].selections[1 - leftIndex]));

    createTemplatedJoin<MergeSortJoiner>(left, container.back().get(), leftIndex, join, container);
}

static void createJoin(Iterator* left,
                       Iterator* right,
                       uint32_t leftIndex,
                       std::vector<std::unique_ptr<Iterator>>& container,
                       Join* join,
                       const Query& query,
                       bool first,
                       bool last,
                       bool aggregable)
{
    int index = 0;
    for (; index < static_cast<int32_t>(join->size()); index++)
    {
        if (left->isSortedOn((*join)[index].selections[leftIndex]))
        {
            break;
        }
    }

    if (index < static_cast<int32_t>(join->size()))
    {
        std::swap((*join)[0], (*join)[index]);
    }

    bool hasLeftIndex = database.hasIndexedIterator((*join)[0].selections[leftIndex]);
    bool hasRightIndex = database.hasIndexedIterator((*join)[0].selections[1 - leftIndex]);
    bool leftSorted = left->isSortedOn((*join)[0].selections[leftIndex]);
    bool leftSortable = leftSorted || (hasLeftIndex && !left->isJoin());

    if (leftSortable && hasRightIndex && (first || leftSorted))
    {
        createMergesortJoin(left, right, leftIndex, container, join, leftSorted);
    }
    else
    {
        if (hasRightIndex)
        {
            createIndexJoin(left, right, leftIndex, container, join, hasLeftIndex);
        }
        else createHashJoin(left, right, leftIndex, container, join, last);
    }
}

static uint64_t getJoinRange(const JoinPredicate& predicate)
{
    auto left = predicate.selections[0];
    auto right = predicate.selections[1];

    auto leftMin = database.getMinValue(left.relation, left.column);
    auto leftMax = database.getMaxValue(left.relation, left.column);
    auto rightMin = database.getMinValue(right.relation, right.column);
    auto rightMax = database.getMaxValue(right.relation, right.column);

    auto start = std::max(leftMin, rightMin);
    auto end = std::min(leftMax, rightMax);

    if (start >= end) return 0;
    return end - start;
}

Iterator* Executor::createRootView(Database& database, Query& query,
                                   std::unordered_map<uint32_t, Iterator*>& views,
                                   std::vector<std::unique_ptr<Iterator>>& container,
                                   bool aggregable)
{
    if (query.joins.empty())
    {
        std::vector<uint32_t> bindings;
        query.fillBindings(bindings);
        if (bindings.size() > 1)
        {
#ifdef STATISTICS
            std::cerr << query.input << std::endl;
            query.dump(std::cerr);
#endif
            throw "EXC";
        }

        return views[bindings[0]];
    }

    std::sort(query.joins.begin(), query.joins.end(), [](const Join& a, const Join& b) {
        return a.size() > b.size();
    });

#ifdef SORT_JOINS_BY_SIZE
    std::sort(query.joins.begin(), query.joins.end(), [&database, &views](const Join& a, const Join& b) {
        return getJoinRange(a[0]) < getJoinRange(b[0]);

        /*auto& aLeft = views[a[0].selections[0].binding];
        auto& aRight = views[a[0].selections[1].binding];
        auto& bLeft = views[b[0].selections[0].binding];
        auto& bRight = views[b[0].selections[1].binding];
        return (aLeft->predictSize() * aRight->predictSize()) < (bLeft->predictSize() * bRight->predictSize());*/
    });
#endif
    auto* join = &query.joins[0];

    auto leftBinding = (*join)[0].selections[0].binding;
    auto rightBinding = (*join)[0].selections[1].binding;

    createJoin(views[leftBinding], views[rightBinding], 0, container, join,
               query, true, query.joins.size() == 1, aggregable);

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

        createJoin(left, right, leftIndex, container, join, query,
                   false, i == (static_cast<int32_t>(query.joins.size()) - 1), aggregable);
        root = container.back().get();
    }

    if (aggregable)
    {
        container.push_back(std::make_unique<Aggregator>(root, query));
        root = container.back().get();
    }

    return root;
}

template <template<bool> typename T, bool GROUPBY_SUM>
static void createTemplatedAggregate(const Selection& groupBy,
                                     const std::vector<Selection>& sumSelections,
                                     std::vector<std::unique_ptr<Iterator>>& container)
{
    container.push_back(std::make_unique<T<GROUPBY_SUM>>(container.back().get(), groupBy, sumSelections));
}
void Executor::createAggregatedViews(const Query& query, std::unordered_map<uint32_t, Iterator*>& views,
                                     std::vector<std::unique_ptr<Iterator>>& container)
{
    for (auto& kv: views)
    {
        Selection groupBy{};
        for (auto& join: query.joins)
        {
            for (auto selection : join[0].selections)
            {
                if (selection.binding == kv.first)
                {
                    groupBy = selection;
                    break;
                }
            }
        }

        bool joinSummed = false;
        std::vector<Selection> sumSelections;
        for (auto& sel: query.selections)
        {
            if (sel.binding == kv.first)
            {
                sumSelections.push_back(sel);
            }
            if (sel == groupBy) joinSummed = true;
        }

        container.push_back(kv.second->createIndexedIterator(container, groupBy));

#ifdef USE_AGGREGATE_INDEX
        bool canUseIndex = true;
        for (auto& filter: query.filters)
        {
            if (filter.selection.binding == kv.first)
            {
                canUseIndex = false;
                break;
            }
        }


        if (canUseIndex)
        {
            if (joinSummed)
            {
                createTemplatedAggregate<IndexedAggregatedIterator, true>(groupBy, sumSelections, container);
            }
            else createTemplatedAggregate<IndexedAggregatedIterator, false>(groupBy, sumSelections, container);
        }
        else
#endif
        {
            if (joinSummed)
            {
                createTemplatedAggregate<AggregatedIterator, true>(groupBy, sumSelections, container);
            }
            else createTemplatedAggregate<AggregatedIterator, false>(groupBy, sumSelections, container);
        }
        kv.second = container.back().get();
    }
}

void Executor::prepareRoots(Database& database, Query& query, Iterator* root, bool aggregable, int split)
{
    for (auto& sel: query.selections)
    {
        this->selectionMap[sel.getId()] = sel;
    }

    if (aggregable)
    {
        auto map = selectionMap;
        std::vector<uint32_t> bindings;
        root->fillBindings(bindings);

        for (auto binding: bindings)
        {
            Selection sel = Selection::aggregatedCount(binding);
            map[sel.getId()] = sel;
        }

        root->requireSelections(map);
    }
    else root->requireSelections(this->selectionMap);

    std::vector<Iterator*> groups;

#ifdef STATISTICS
    this->query.predicted = static_cast<uint64_t>(root->predictSize());
#endif

    if (root->isImpossible())
    {
        this->query.fillImpossible();
#ifdef STATISTICS
        plansSkipped++;
#endif
    }
    else
    {
        root->split(this->container, groups, static_cast<size_t>(split));
        for (int i = 0; i < static_cast<int32_t>(groups.size()); i++)
        {
            auto iter = std::make_unique<MultiWrapperIterator>(groups, i);
            this->roots.push_back(iter.get());
            this->container.push_back(std::move(iter));
        }
    }

    if (!this->roots.empty())
    {
        root = this->roots.back();
    }

    root->requireSelections(this->selectionMap);

    for (auto& sel: this->selectionMap)
    {
        this->backMap[sel.second.getId()] = static_cast<unsigned int>(columnIds.size());
        this->columnIds.push_back(root->getColumnForSelection(sel.second));
        this->selections.push_back(sel.second);
    }

    for (auto& root: this->roots)
    {
        root->setSelections(this->columnIds, this->selections);
    }
}

void Executor::finalizeQuery()
{
    size_t count = 0;
    std::vector<uint64_t> results(static_cast<size_t>(this->selectionMap.size()));

    if (!this->roots.empty())
    {
        for (auto& iter: this->roots)
        {
            size_t localCount = 0;
            iter->sumRows(results, this->columnIds, this->selections, localCount);
            count += localCount;
        }

#ifdef STATISTICS
        std::stringstream planStream;
        this->roots[0]->dumpPlan(planStream);
        this->query.plan = planStream.str();

        double time = 0;
        for (auto& root: this->roots)
        {
            time += ((MultiWrapperIterator*)root)->time;
        }

        this->query.time = time / this->roots.size();
#endif

#ifdef COLLECT_JOIN_SIZE
        this->roots[0]->assignJoinSize(database);
#endif
    }

    std::stringstream ss;
    for (auto& sel: this->query.selections)
    {
        if (count == 0)
        {
            ss << "NULL ";
        }
        else
        {
            uint64_t result = results[this->backMap[sel.getId()]];
            ss << std::to_string(result) << ' ';
        }
    }

    this->query.result = ss.str();
    this->query.result[this->query.result.size() - 1] = '\n';
}
