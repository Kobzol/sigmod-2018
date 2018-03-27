#include <iostream>
#include <memory>
#include "../relation/column-relation.h"
#include "../io.h"
#include "../join/hash-joiner.h"
#include "../executor.h"
#include "../emit/filter-compiler.h"

static ColumnRelation createRelation(int tupleCount, int columnCount, int offset = 0)
{
    ColumnRelation relation{};
    relation.tupleCount = static_cast<uint64_t>(tupleCount);
    relation.columnCount = static_cast<uint32_t>(columnCount);
    relation.data = new uint64_t[relation.tupleCount * relation.columnCount];
    for (int i = 0; i < (int) (relation.tupleCount * relation.columnCount); i++)
    {
        relation.data[i] = static_cast<uint64_t>(i) + offset;
    }
    return relation;
}
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

Database database;

int main()
{
    /*std::vector<Filter> filters = {
        Filter(Selection(0, 0, 0), 13, nullptr, '>'),
        Filter(Selection(0, 0, 0), 28, nullptr, '<')
    };

    FilterCompiler compiler;
    auto fn = compiler.compile(filters);
    std::cerr << fn(25600000000) << std::endl;
    std::cerr << fn(0) << std::endl;
    std::cerr << fn(13) << std::endl;
    std::cerr << fn(28) << std::endl;
    std::cerr << fn(14) << std::endl;*/

    database.relations.push_back(createRelation(4, 3));
    database.relations.back().cumulativeColumnId = 0;
    database.relations.push_back(createRelation(4, 3));
    database.relations.push_back(createRelation(3, 3));

    setRelation(database.relations[0], {
            1, 2, 3,
            2, 2, 4,
            4, 2, 6,
            3, 4, 2
    });
    setRelation(database.relations[1], {
            1, 1, 4,
            2, 1, 3,
            2, 4, 2,
            3, 3, 4
    });
    setRelation(database.relations[2], {
            1, 2, 2,
            4, 4, 8,
            4, 8, 2
    });

    int columnId = 0;
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
    {
        database.relations[i].cumulativeColumnId = static_cast<uint32_t>(columnId);
        columnId += database.relations[i].columnCount;
    }
    database.unique.resize(database.relations.back().cumulativeColumnId + database.relations.back().columnCount);

    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
    {
        for (int j = 0; j < static_cast<int32_t>(database.relations[i].columnCount); j++)
        {
            database.primaryIndices.push_back(std::make_unique<PrimaryIndex>(database.relations[i], j,
                                                                 nullptr));
            database.sortIndices.push_back(std::make_unique<SortIndex>(database.relations[i], j));
            database.aggregateIndices.push_back(std::make_unique<AggregateIndex>(database.relations[i], j,
                                                                                 *database.sortIndices.back()));

            database.sortIndices.back()->build();
            database.aggregateIndices.back()->build();
        }
    }

    Query query;
    //std::string line = "0 1 2|0.0=1.0&2.0=0.0|0.2 1.1 1.2 2.0";
    //std::string line = "0 1 2|0.1=1.0&1.0=2.2&0.0>2|1.0 0.0 0.2";
    //std::string line = "0 1|0.2=1.0&0.2=2|1.1 0.2 1.0";
    std::string line = "0 1|0.0=1.1|1.1 1.2";
    loadQuery(query, line);

    Executor executor;
    executor.executeQuery(database, query);
    std::cout << query.result << std::flush;

    return 0;
}
