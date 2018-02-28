#include <iostream>
#include <memory>
#include "../relation/column-relation.h"
#include "../relation/filter-view.h"
#include "../io.h"
#include "../join/hash-joiner.h"
#include "../executor.h"

static ColumnRelation createRelation(int tupleCount, int columnCount, int offset = 0)
{
    ColumnRelation relation;
    relation.tupleCount = static_cast<uint64_t>(tupleCount);
    relation.columnCount = static_cast<uint64_t>(columnCount);
    relation.data = new uint64_t[relation.tupleCount * relation.columnCount];
    for (int i = 0; i < (int) (relation.tupleCount * relation.columnCount); i++)
    {
        relation.data[i] = static_cast<uint64_t>(i) + offset;
    }
    return relation;
}

int main()
{
    Query query;
    std::string line = "0 1 2|0.1=1.0&1.0=2.2&0.0>0|1.0 0.3 0.4";
    loadQuery(query, line);

    Database database;
    database.relations.push_back(createRelation(3, 5, 1));
    database.relations.push_back(createRelation(3, 3, 1));
    database.relations.push_back(createRelation(3, 3, 2));

    database.relations[0].getValueMutable(0, 1) = 5;
    database.relations[1].getValueMutable(0, 0) = 5;
    database.relations[1].getValueMutable(0, 0) = 5;
    database.relations[2].getValueMutable(0, 2) = 5;

    database.relations[0].dump();
    database.relations[1].dump();
    database.relations[2].dump();

    Executor executor;
    std::cout << executor.executeQuery(database, query) << std::endl;

    return 0;
}
