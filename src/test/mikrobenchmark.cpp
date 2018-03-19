#include <iostream>
#include <string>
#include <memory>
#include <unordered_map>

#include "../relation/column-relation.h"
#include "../io.h"
#include "../join/hash-joiner.h"
#include "../join/index-joiner.h"
#include "../join/merge-sort-joiner.h"
#include "../executor.h"
#include "../common/cTimer.h"
#include "../database.h"
#include "../query.h"
#include "../aggregator.h"
#include "../timer.h"

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


static ColumnRelation createRelation(int tupleCount, int columnCount, int offset = 0, int repeat = 1, int step = 1)
{
	ColumnRelation relation{};
	relation.tupleCount = static_cast<uint64_t>(tupleCount);
	relation.columnCount = static_cast<uint32_t>(columnCount);
	relation.data = new uint64_t[relation.tupleCount * relation.columnCount];

	for (int i = 0, j = 0; i < (int)(relation.tupleCount * relation.columnCount); j+= step)
	{
		for (int k = 0; k < repeat; k++, i++)
		relation.data[i] = static_cast<uint64_t>(j) + offset;
	}
	return relation;
}


static ColumnRelation createRelation2(int tupleCount, int min, int max, int step)
{
	int columnCount = 2;
	int value;
	ColumnRelation relation{};
	relation.tupleCount = static_cast<uint64_t>(tupleCount);
	relation.columnCount = static_cast<uint32_t>(columnCount);
	relation.data = new uint64_t[relation.tupleCount * relation.columnCount];

	// column1
	value = min;
	for (int i = 0; i < (int)(relation.tupleCount); i++)
	{
		relation.data[i] = static_cast<uint64_t>(value);
		value += step;
		if (value > max) value = min;
	}
	return relation;

	// column2
	value = 0;
	for (int i = relation.tupleCount; i < (int)(relation.tupleCount * relation.columnCount); i++)
	{
		relation.data[i] = static_cast<uint64_t>(value++);
	}
	return relation;
}


Database database;

#define REL_COUNT 2
#define COL_COUNT 2

//#define TEST_HISTOGRAM

#define MAX_TUPLE_COUNT 5000000
#define MIN_TUPLE_COUNT 10000
#define MAX_JOIN_VALUE 10000000
#define MAX_STEP 50
#define NUM_OF_RUNS 100

// debug
//#define PRINT
//#define MAX_TUPLE_COUNT 100
//#define NUM_OF_RUNS 1

#define REPEAT 1
#define LARGE_STEP 1

void prepare_iterators(Executor& executor,
	const Query& query,
	std::unordered_map<uint32_t, Iterator*>& views,
	std::vector<std::unique_ptr<Iterator>>& container,
	Iterator** left,
	Iterator** right,
	bool left_indexed,
	bool right_indexed);

int main()
{

	int runCount = 0;
	int runMax = NUM_OF_RUNS;
	int selectivity;

	double* qtimes_nlj = new double[runMax];
	double* qtimes_ms = new double[runMax];
	double* qtimes_hj = new double[runMax];
	int* right_relation_size = new int[runMax];
	int* left_relation_size = new int[runMax];

	srand(100);

	do
	{
		selectivity = rand() % 100 + 1;
		Query query;
#ifdef JUST_EXECUTE
		std::string line = "0 1|0.0=1.0&0.1<7|0.1 1.1";
#else
		int select = MAX_TUPLE_COUNT * selectivity / 100;
		std::string line = (std::string)("0 1|0.0=1.0&0.1<") + std::to_string(select) + "|0.1 1.1";
#endif
		loadQuery(query, line);


		database.relations.clear();
		database.histograms.clear();
		database.sortIndices.clear();

		common::utils::cTimer queryTimer;

		// create relation 1
		int tuple_count = rand() % MAX_TUPLE_COUNT + MIN_TUPLE_COUNT;
		int min = 0;
		int max = rand() % MAX_JOIN_VALUE + 10000;
		int step = ((float)1 / (rand() % 500)) * MAX_STEP + 1;
		database.relations.push_back(createRelation2(tuple_count, min, max, step));
		database.relations.back().cumulativeColumnId = 0;

		// create relation 2
		int tuple_count = rand() % MAX_TUPLE_COUNT + MIN_TUPLE_COUNT;
		int min = 0;
		int max = rand() % MAX_JOIN_VALUE + 10000;
		int step = ((float)1 / (rand() % 500)) * MAX_STEP + 1;
		database.relations.push_back(createRelation2(tuple_count, min, max, step));
		database.relations.back().cumulativeColumnId = 2;

#ifdef TEST_HISTOGRAM
		database.histograms.emplace_back();
		database.histograms.emplace_back();

		Timer th;
		th.start();
		int i = 0;
		for (auto& h : database.histograms)
		{
			h.loadRelation(database.relations[i++]);
		}
		std::cout << "Histogram time load: " << th.get() << std::endl;
		database.print();
#endif

#ifdef PRINT
		std::cout << line << std::endl;
		database.relations[0].print();
		database.relations[1].print();
#endif

		database.sortIndices.resize(4);
		for (int i = 0; i < REL_COUNT; i++)
		{
			for (int j = 0; j < COL_COUNT; j++)
			{
				//database.hashIndices[i * REL_COUNT + j] = std::make_unique<HashIndex>(database.relations[i], j);
				//database.hashIndices[i * REL_COUNT + j]->build();
				database.sortIndices[i * REL_COUNT + j] = std::make_unique<SortIndex>(database.relations[i], j);
				database.sortIndices[i * REL_COUNT + j]->build();
			}
		}

		{
			Executor executor;

			std::unordered_map<uint32_t, Iterator*> views;
			std::vector<std::unique_ptr<Iterator>> container;
			Iterator* left = 0;
			Iterator* right = 0;

			// NLJ
			prepare_iterators(executor, query, views, container, &left, &right, true, true);
			auto* join = &query.joins[0];
			auto root_nlj = std::make_unique<IndexJoiner>(left, right, 0, *join);

			Aggregator aggregator;
			Timer timer;
			timer.start();
			aggregator.aggregate(database, query, root_nlj.get());
			qtimes_nlj[runCount] = timer.get();

			query.result[query.result.size() - 1] = '\n';
			std::cout << query.result << std::flush;
		}

		{
			//MS
			Executor executor;

			std::unordered_map<uint32_t, Iterator*> views;
			std::vector<std::unique_ptr<Iterator>> container;
			Iterator* left = 0;
			Iterator* right = 0;

			prepare_iterators(executor, query, views, container, &left, &right, true, true);

			auto root_ms = std::make_unique<MergeSortJoiner>(left, right, 0, query.joins[0]);

			Aggregator aggregator;
			Timer timer;
			timer.start();
			aggregator.aggregate(database, query, root_ms.get());
			qtimes_ms[runCount] = timer.get();

			query.result[query.result.size() - 1] = '\n';
			std::cout << query.result << std::flush;
		}

		{
			//HJ
			Executor executor;

			std::unordered_map<uint32_t, Iterator*> views;
			std::vector<std::unique_ptr<Iterator>> container;
			Iterator* left = 0;
			Iterator* right = 0;

			prepare_iterators(executor, query, views, container, &left, &right, true, false);

			auto root_hj = std::make_unique<HashJoiner<false>>(left, right, 0, query.joins[0]);

			Aggregator aggregator;
			Timer timer;
			timer.start();
			aggregator.aggregate(database, query, root_hj.get());
			qtimes_hj[runCount] = timer.get();

			query.result[query.result.size() - 1] = '\n';
			std::cout << query.result << std::flush;
		}

		delete[] database.relations[0].data;
		delete[] database.relations[1].data;

	} while (++runCount < runMax);

	std::cout << "NLJ query times:" << std::endl;
	for (int i = 0; i < runMax; i++)
	{
		std::cout << qtimes_nlj[i] << std::endl;
	}

	std::cout << "MS query times:" << std::endl;
	for (int i = 0; i < runMax; i++)
	{
		std::cout << qtimes_ms[i] << std::endl;
	}


	std::cout << "HJ query times:" << std::endl;
	for (int i = 0; i < runMax; i++)
	{
		std::cout << qtimes_hj[i] << std::endl;
	}
	return 0;
}

void prepare_iterators(Executor& executor, 
	const Query& query,
	std::unordered_map<uint32_t, Iterator*>& views,
	std::vector<std::unique_ptr<Iterator>>& container,
	Iterator** left,
	Iterator** right,
	bool left_indexed,
	bool right_indexed)
{
	executor.createViews(database, query, views, container);

	auto leftBinding = (query.joins[0])[0].selections[0].binding;
	auto rightBinding = (query.joins[0])[0].selections[1].binding;

	*left = views[leftBinding];
	*right = views[rightBinding];

	if (right_indexed)
	{
		container.push_back((*right)->createIndexedIterator());
		*right = container.back().get();
	}

	if (left_indexed)
	{
		container.push_back((*left)->createIndexedIterator());
		*left = container.back().get();
	}
}