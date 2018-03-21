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
#include "aggregation/aggregate.h"
#include "aggregation/aggregateSort.h"
#include "aggregation/compute.h"
#include "relation/sort-index-iterator.h"
#include "relation/primary-index-iterator.h"
#include "join/self-join.h"
#include "timer.h"
#include "join/index-joiner.h"
#include "join/merge-sort-joiner.h"

#include "common/cTimer.h"

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
	// Pro kazdou tabulku seznam filtru.
	std::unordered_map<uint32_t, std::vector<Filter>> filtersByBindings;

	// Nastavi hash mapu tabulka -> seznam filtru.
	for (auto& filter : query.filters)
	{
		filtersByBindings[filter.selection.binding].push_back(filter);
	}

    // assign a filter iterator for filtered bindings
    for (auto& filterGroup: filtersByBindings)
    {
        std::sort(filterGroup.second.begin(), filterGroup.second.end(), [](const Filter& lhs, const Filter& rhs)
        {
            return lhs.oper == '=';
        });
    }

	// Pro kazdou dvojici (tabulka -> seznam filtru)
	for (auto& filterGroup : filtersByBindings)
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

		// Vytvorime bud hashovany filtr nebo iteracni filtr.
		if (equalsIndex != -1)
		{
			filter = std::make_unique<HashFilterIterator>(relation,
				binding,
				filterGroup.second,
				equalsIndex);
		}
		else
		{
			filter = std::make_unique<FILTER_ITERATOR>(relation,
				binding,
				filterGroup.second);
		}
#else
		auto filter = std::make_unique<FILTER_ITERATOR>(relation,
			binding,
			filterGroup.second);
#endif

		views.insert({ binding, filter.get() });
		container.push_back(std::move(filter));
	}

	// Zbytek tabulek, pro ktere nemame zadne filtry.
	uint32_t binding = 0;
	for (auto& relation : query.relations)
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
	for (auto& kv : query.selfJoins)
	{
		auto it = views.find(kv.first);
		container.push_back(std::make_unique<SelfJoin>(
			*it->second,
			kv.second
			));
		views[binding] = container.back().get();
	}
    // assign self-joins
    for (auto& kv: query.selfJoins)
    {
        auto it = views.find(kv.first);
        container.push_back(std::make_unique<SelfJoin>(
                *it->second,
                kv.second
        ));
        views[kv.first] = container.back().get();
    }
#endif
}

static void createHashJoin(Iterator* left,
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
    else
    {
        container.push_back(std::make_unique<HashJoiner<false>>(
                left,
                right,
                leftIndex,
                *join
        ));
    }
}
static void createIndexJoin(Iterator* left,
                    Iterator* right,
                    uint32_t leftIndex,
                    std::vector<std::unique_ptr<Iterator>>& container,
                    Join* join)
{
    container.push_back(right->createIndexedIterator());

    if (join->size() > 1)
    {
        container.push_back(std::make_unique<IndexJoiner<true>>(
                left,
                container.back().get(),
                leftIndex,
                *join
        ));
    }
    else
    {
        container.push_back(std::make_unique<IndexJoiner<false>>(
                left,
                container.back().get(),
                leftIndex,
                *join
        ));
    }
}
static void createMergesortJoin(Iterator* left,
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

    if (join->size() > 1)
    {
        container.push_back(std::make_unique<MergeSortJoiner<true>>(
                left,
                container.back().get(),
                leftIndex,
                *join
        ));
    }
    else
    {
        container.push_back(std::make_unique<MergeSortJoiner<false>>(
                left,
                container.back().get(),
                leftIndex,
                *join
        ));
    }
}

static void createJoin(Iterator* left,
                Iterator* right,
                uint32_t leftIndex,
                std::vector<std::unique_ptr<Iterator>>& container,
                Join* join,
                const Query& query,
                bool first,
                bool last)
{
#ifndef INDEX_AVAILABLE
    createHashJoin(left, right, leftIndex, container, join, false);
    return;
#endif

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

    if (first || left->isSortedOn((*join)[0].selections[leftIndex]))
    {
        createMergesortJoin(left, right, leftIndex, container, join);
    }
    else
    {
        // TODO: left now thinks it's empty when it's a SortIndexIterator without filters
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

    createJoin(views[leftBinding], views[rightBinding], 0, container, join,
               query, true, query.joins.size() == 1);

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
                   false, i == (static_cast<int32_t>(query.joins.size()) - 1));
        root = container.back().get();
    }

    return root;
}


void Executor::remapJoin(Join* join, std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>>& map)
{
	for (auto & pred : *join)
	{
		pred.selections[0].column = map[pred.selections[0].binding][pred.selections[0].column];
		pred.selections[1].column = map[pred.selections[1].binding][pred.selections[1].column];
	}
}

void createJoinNew(Iterator* left,
	Iterator* right,
	uint32_t leftIndex,
	std::vector<std::unique_ptr<Iterator>>& container,
	Join* join)
{
	if (right->supportsIterateValue())
	{
        if (join->size() > 1)
        {
            container.push_back(std::make_unique<IndexJoiner<true>>(
                    left,
                    right,
                    leftIndex,
                    *join));
        }
        else
        {
            container.push_back(std::make_unique<IndexJoiner<false>>(
                    left,
                    right,
                    leftIndex,
                    *join));
        }

		right->prepareIterateValue();
	}
	else
	{
		if (join->size() > 1)
		{
			container.push_back(std::make_unique<HashJoiner<true>>(
				right,
				left,
				1 - leftIndex,
				*join));
		}
		else
		{
			container.push_back(std::make_unique<HashJoiner<false>>(
				right,
				left,
				1 - leftIndex,
				*join));
		}
	}
}

void Executor::executeNew(Database & database, Query & query)
{
#ifdef STATISTICS
	Timer timer;
#endif

	std::vector<std::unique_ptr<Iterator>> container;
	std::unordered_map<uint32_t, AggregateAbstract*> aggregates;
	std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>> columnMappingSum;
	std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>> columnMappingJoin;

	// Nejprve sestaveni agregatoru.
	uint32_t binding = 0;
	for (auto &relation : query.relations)
	{
		Iterator* iterator = NULL;

		// Filtry.
		std::vector<Filter> filters;
		for (auto &filter : query.filters)
		{
			if (filter.selection.binding == binding)
			{
				filters.push_back(filter);
			}
		}

		// Pokud existuje filtr, vytvorime FilterIterator.
		if (filters.size() > 0)
		{
			//container.push_back(std::make_unique<FilterIterator>(&database.relations[relation], binding, filters));
			//iterator = container.back().get();
			container.push_back(std::make_unique<SortIndexIterator>(&database.relations[relation], binding, filters));
			iterator = container.back().get();

		}
		// Jinak vytvorim standardni iterator nad tabulkou.
		else
		{
			container.push_back(std::make_unique<ColumnRelationIterator>(&database.relations[relation], binding));
			iterator = container.back().get();
		}

		// Zjistim, podle ceho je potreba relace seskupovat.
		std::vector<Selection> groupBy;
		for (auto &join : query.joins)
		{
			for (auto &predicate : join)
			{
				// Obe strany predikatu.
				for (unsigned int i = 0; i < 2; i++)
				{
					if (predicate.selections[i].binding == binding)
					{
						// Projdu, jestli nahodou uz dana selekce neni obsazena.
						bool b = false;
						for (auto selection : groupBy)
						{
							if (selection == predicate.selections[i])
							{
								b = true;
								break;
							}
						}

						if (!b)
						{
							// Premapovani sloupce.
							columnMappingJoin[binding][predicate.selections[i].column] = groupBy.size();

							groupBy.push_back(predicate.selections[i]);
						}
					}
				}
			}
		}

		// Nakonec, podle ceho je potreba sumovat.
		std::vector<Selection> sum;
		for (auto &selection : query.selections)
		{
			if (selection.binding == binding)
			{
				// Premapovani sloupce.
				columnMappingSum[binding][selection.column] = groupBy.size() + sum.size() + 1;

				sum.push_back(selection);
			}
		}

		if (groupBy.size() == 1 && filters.size() == 0)
		{
			container.push_back(std::make_unique<AggregateSort>(&database.relations[relation], groupBy[0], sum, binding));
		}
		else
		{
			// Podpora max. 5 sloupcu pro groupovani.
			switch (groupBy.size())
			{
			case 1:
				container.push_back(std::make_unique<Aggregate<1>>(iterator, groupBy, sum, binding));
				break;
			case 2:
				container.push_back(std::make_unique<Aggregate<2>>(iterator, groupBy, sum, binding));
				break;
			case 3:
				container.push_back(std::make_unique<Aggregate<3>>(iterator, groupBy, sum, binding));
				break;
			case 4:
				container.push_back(std::make_unique<Aggregate<4>>(iterator, groupBy, sum, binding));
				break;
			case 5:
				container.push_back(std::make_unique<Aggregate<5>>(iterator, groupBy, sum, binding));
				break;
			default:
				// TODO.
				assert(false);
				break;
			}
		}

		aggregates[binding] = (AggregateAbstract*)container.back().get();

		binding++;
	}

	// Sestaveni planu.

	auto* join = &query.joins[0];
	remapJoin(join, columnMappingJoin);


	auto leftBinding = (*join)[0].selections[0].binding;
	auto rightBinding = (*join)[0].selections[1].binding;
	assert(leftBinding <= rightBinding);

	//if (join->size() > 1)
	//{
	//	container.push_back(std::make_unique<HashJoiner<true>>(
	//		aggregates[leftBinding],
	//		aggregates[rightBinding],
	//		0,
	//		*join
	//		));
	//}
	//else
	//{
	//	container.push_back(std::make_unique<HashJoiner<false>>(
	//		aggregates[leftBinding],
	//		aggregates[rightBinding],
	//		0,
	//		*join
	//		));
	//}
	createJoinNew(aggregates[leftBinding], aggregates[rightBinding], 0, container, join);

	std::unordered_set<uint32_t> usedBindings = { leftBinding, rightBinding };
	Iterator* root = container.back().get();
	for (int i = 1; i < static_cast<int32_t>(query.joins.size()); i++)
	{
		join = &query.joins[i];
		leftBinding = (*join)[0].selections[0].binding;
		rightBinding = (*join)[0].selections[1].binding;

		remapJoin(join, columnMappingJoin);

		auto usedLeft = usedBindings.find(leftBinding) != usedBindings.end();
		auto usedRight = usedBindings.find(rightBinding) != usedBindings.end();
		Iterator* left = root;
		Iterator* right;
		uint32_t leftIndex = 0;

		if (usedLeft)
		{
			right = aggregates[rightBinding];
			usedBindings.insert(rightBinding);
		}
		else if (usedRight)
		{
			right = aggregates[leftBinding];
			usedBindings.insert(leftBinding);
			leftIndex = 1;
		}
		else
		{
			query.joins.push_back(*join);
			continue;
		}

		//if (join->size() > 1)
		//{
		//	container.push_back(std::make_unique<HashJoiner<true>>(
		//		left,
		//		right,
		//		leftIndex,
		//		*join
		//		));
		//}
		//else
		//{
		//	container.push_back(std::make_unique<HashJoiner<false>>(
		//		left,
		//		right,
		//		leftIndex,
		//		*join
		//		));
		//}

		createJoinNew(left, right, leftIndex, container, join);

		root = container.back().get();
	}

	const unsigned int computeBinding = 1000;


	std::vector<std::vector<Selection>> computeExprs;
	std::vector<Selection> newQuerySelections;
	uint32_t selIndex = 0;
	for (auto selection : query.selections)
	{
		std::vector<Selection> expr;

		// Premapovany sloupec pro soucet.

		Selection newSelection;
		newSelection.binding = selection.binding;
		newSelection.column = columnMappingSum[selection.binding][selection.column];
		newSelection.relation = selection.relation;

		expr.push_back(newSelection);

		// Sloupce pro pocty - vsechny ostatni relace mimo tu, nad kterou se provadi suma.
		for (uint32_t binding = 0; binding < query.relations.size(); binding++)
		{
			if (binding != selection.binding)
			{
				Selection newSelection2;
				newSelection2.binding = binding;
				newSelection2.column = aggregates[binding]->getCountColumnIndex();
				newSelection2.relation = query.relations[binding];

				expr.push_back(newSelection2);
			}
		}

		computeExprs.push_back(expr);

		Selection newQuerySelection;
		newQuerySelection.binding = computeBinding;
		newQuerySelection.column = selIndex++;
		newQuerySelection.relation = 0;

		newQuerySelections.push_back(newQuerySelection);
	}

	//Aggregator aggTest;
	//aggTest.printResult(database, query, root);
	//exit(0);


	// std::cout << computeExprs.size() << std::endl;

	container.push_back(std::make_unique<Compute>(
		root, computeExprs, computeBinding));

	root = container.back().get();

	//root->printPlan(0);


	query.selections = newQuerySelections;

	//common::utils::cTimer timer;
	//timer.Start();

	Aggregator aggregator;
	aggregator.aggregate(database, query, root);
	query.result[query.result.size() - 1] = '\n';

	//timer.Stop();
	//timer.Print("\n");

	//exit(0);
#ifdef STATISTICS
	query.time = timer.get();
#endif
}
