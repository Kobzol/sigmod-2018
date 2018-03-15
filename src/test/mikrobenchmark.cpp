#include <iostream>
#include <string>
#include <memory>
#include <unordered_map>

#include "../relation/column-relation.h"
#include "../io.h"
#include "../join/hash-joiner.h"
#include "../join/merge-sort-joiner.h"
#include "../executor.h"
#include "../common/cTimer.h"
#include "../database.h"
#include "../query.h"
#include "../aggregator.h"

static void setRelation(ColumnRelation& relation, const std::vector<uint64_t>& data)
{
    uint64_t cols = relation.columnCount;

    for (int i = 0; i < (int) data.size(); i++)
    {
        uint64_t row = i / cols;
        uint64_t col = i % cols;

        relation.getValueMutable(row, col) = data[i];
    }
}


static ColumnRelation createRelation(int tupleCount, int columnCount, int offset = 0, int repeat = 1)
{
	ColumnRelation relation{};
	relation.tupleCount = static_cast<uint64_t>(tupleCount);
	relation.columnCount = static_cast<uint32_t>(columnCount);
	relation.data = new uint64_t[relation.tupleCount * relation.columnCount];

	for (int i = 0; i < (int)(relation.tupleCount * relation.columnCount); i++)
	{
		relation.data[i] = static_cast<uint64_t>(i) + offset;
	}
	return relation;
}


Database database;

#define REL_COUNT 2
#define COL_COUNT 2
#define SMALL_TUPLE_COUNT 10
#define LARGE_TUPLE_COUNT 20

int main()
{
	Query query;
	std::string line = "0 1|0.0=1.0|0.1 1.1";
	//std::string line = "2 2|0.0=1.1|1.2";
	loadQuery(query, line);



	database.relations.push_back(createRelation(SMALL_TUPLE_COUNT, COL_COUNT));
	database.relations.back().cumulativeColumnId = 0;
	database.relations.push_back(createRelation(LARGE_TUPLE_COUNT, COL_COUNT));
	database.relations.back().cumulativeColumnId = 2;

	//database.relations[0].print();
	//database.relations[1].print();

	database.hashIndices.resize(4);
	database.sortIndices.resize(4);
	for (int i = 0; i < REL_COUNT; i++)
	{
		for (int j = 0; j < COL_COUNT; j++)
		{
			database.hashIndices[i * REL_COUNT + j] = std::make_unique<HashIndex>(database.relations[i], j);
			database.hashIndices[i * REL_COUNT + j]->build();
			database.sortIndices[i * REL_COUNT + j] = std::make_unique<SortIndex>(database.relations[i], j);
			database.sortIndices[i * REL_COUNT + j]->build();
		}
	}

	Executor executor;

	std::unordered_map<uint32_t, Iterator*> views;
	std::vector<std::unique_ptr<Iterator>> container;

	executor.createViews(database, query, views, container);
	auto* join = &query.joins[0];

	auto leftBinding = (*join)[0].selections[0].binding;
	auto rightBinding = (*join)[0].selections[1].binding;

	//createJoin(views[leftBinding], views[rightBinding], 0, container, join, true, query.joins.size() == 1);

	Iterator* left = views[leftBinding];
	Iterator* right = views[rightBinding];

	container.push_back(left->createIndexedIterator());
	left = container.back().get();

	container.push_back(right->createIndexedIterator());
	right = container.back().get();

	// dat containeru
	auto root = std::make_unique<MergeSortJoiner>(
		left,
		right,
		0,
		*join
		);
	container.push_back(std::move(root));

	//std::vector<uint64_t> results(static_cast<size_t>(2));
	//std::vector<uint32_t> columnIds;
	//size_t count = 0;

	//root->sumRows(results, columnIds, count);

	Aggregator aggregator;
	aggregator.aggregate(database, query, root.get());


	//query.result[query.result.size() - 1] = '\n';
	//std::cout << results[0] << ' ' << results[1] << std::endl << std::flush;

	return 0;
}
