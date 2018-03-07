#include <iostream>
#include <memory>
#include "../relation/column-relation.h"
#include "../io.h"
#include "../join/hash-joiner.h"
#include "../executor.h"

static ColumnRelation createRelation(int tupleCount, int columnCount, int offset = 0)
{
    static int id = 0;

    ColumnRelation relation;
    relation.tupleCount = static_cast<uint64_t>(tupleCount);
    relation.columnCount = static_cast<uint64_t>(columnCount);
    relation.data = new uint64_t[relation.tupleCount * relation.columnCount];
    relation.id = static_cast<uint32_t>(id++);
    for (int i = 0; i < (int) (relation.tupleCount * relation.columnCount); i++)
    {
        relation.data[i] = static_cast<uint64_t>(i) + offset;
    }
    return relation;
}
static void setRelation(ColumnRelation& relation, const std::vector<uint64_t> data)
{
    uint64_t cols = relation.columnCount;

    for (int i = 0; i < (int) data.size(); i++)
    {
        uint64_t row = i / cols;
        uint64_t col = i % cols;

        relation.getValueMutable(row, col) = data[i];
    }
}

int main()
{
    Query query;
    //std::string line = "0 1 2|0.1=1.0&1.1=2.0|0.2 1.1 2.0";
    std::string line = "2 2|0.0=1.1|1.2";
    loadQuery(query, line);

    Database database{};
    database.relations.push_back(createRelation(3, 3));
    database.relations.push_back(createRelation(3, 3));
    database.relations.push_back(createRelation(3, 3));

    setRelation(database.relations[0], {
            1, 2, 3,
            4, 2, 6,
            3, 4, 2
    });
    setRelation(database.relations[1], {
            2, 1, 0,
            4, 4, 2,
            3, 8, 4
    });
    setRelation(database.relations[2], {
            1, 2, 1,
            4, 4, 8,
            4, 8, 3
    });

    Executor executor;
    executor.executeQuery(database, query);
    std::cout << query.result << std::flush;

    return 0;
}
