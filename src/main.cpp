#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cstring>
#include <cstddef>
#include <unordered_set>
#include <limits>
#include <unordered_map>
#include <memory>
#include <sstream>
#include <algorithm>
#include <cassert>

#include "settings.h"
#include "database.h"
#include "query.h"
#include "relation/view.h"
#include "relation/filter-view.h"
#include "join/hash-joiner.h"
#include "aggregator.h"
#include "executor.h"

#ifdef STATISTICS
    static size_t tupleCount = 0;
    static size_t columnCount = 0;
    static size_t minColumns = std::numeric_limits<size_t>::max();
    static size_t maxColumns = 0;
    static size_t minTuples = std::numeric_limits<size_t>::max();
    static size_t maxTuples = 0;
    static size_t queryCount = 0;
    static size_t joinCount = 0;
    static size_t filterCount = 0;
    static size_t batchCount = 0;
    static size_t multipleColumnsPerRelJoins = 0;
    static size_t relationsMissingInJoins = 0;
#endif

static void loadDatabase(Database& database)
{
    std::string line;
    struct stat stats{};

    while (std::getline(std::cin, line))
    {
        if (line == "Done") break;

        int fd = open(line.c_str(), O_RDONLY);
        fstat(fd, &stats);

        auto length = (size_t) stats.st_size;
        auto* addr = static_cast<char*>(mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, 0));
#ifdef CHECK_ERRORS
        if (addr < 0) perror("mmap");
#endif

        database.relations.emplace_back();
        ColumnRelation& rel = database.relations.back();

        rel.tupleCount = *reinterpret_cast<uint64_t*>(addr);
        addr += sizeof(uint64_t);
        rel.columnCount = *reinterpret_cast<uint64_t*>(addr);
        addr += sizeof(uint64_t);
        rel.data = new uint64_t[rel.tupleCount * rel.columnCount];
        rel.id = database.relations.size() - 1;
        std::memcpy(rel.data, addr, rel.tupleCount * rel.columnCount * sizeof(uint64_t));
        munmap(addr, length);
        close(fd);

#ifdef STATISTICS
        tupleCount += rel.tupleCount;
        columnCount += rel.columnCount;
        minTuples = std::min(minTuples, rel.tupleCount);
        maxTuples = std::max(maxTuples, rel.tupleCount);
        minColumns = std::min(minColumns, rel.columnCount);
        maxColumns = std::max(maxColumns, rel.columnCount);
#endif
    }
}
static uint64_t readInt(std::string& str, int& index)
{
    uint64_t value = 0;
    while (isdigit(str[index]))
    {
        value *= 10;
        value += str[index++] - '0';
    }

    return value;
}
static void loadQuery(Query& query, std::string& line)
{
    line += '|';

    int index = 0;

    // load relations
    while (true)
    {
        query.relations.push_back(readInt(line, index));
        if (line[index++] == '|') break;
    }

    // load predicates
    while (true)
    {
        uint32_t relation = query.relations[readInt(line, index)];
        index++;
        Selection selection(relation, (uint32_t) readInt(line, index));
        char oper = line[index++];

        uint64_t value = readInt(line, index);
        if (EXPECT(line[index] == '.')) // parse join
        {
            query.joins.emplace_back();
            Join& join = query.joins.back();
            join.selections[0] = selection;

            join.selections[1].relation = query.relations[value];
            index++;
            join.selections[1].column = (uint32_t) readInt(line, index);

            assert(join.selections[0].relation != join.selections[1].relation);
            if (join.selections[0].relation > join.selections[1].relation)
            {
                std::swap(join.selections[0], join.selections[1]);
            }
        }
        else // parse filter
        {
            query.filters.emplace_back(selection, oper, value);
        }

        if (line[index++] == '|') break;
    }

    // load selections
    while (true)
    {
        auto relation = query.relations[readInt(line, index)];
        index++;
        query.selections.emplace_back(relation, readInt(line, index));

        if (line[index++] == '|') break;
    }
}

int main(int argc, char** argv)
{
    std::ios::sync_with_stdio(false);

    Database database;
    loadDatabase(database);

    std::cout << "Ready" << std::endl;

    Executor executor;
    std::string line;
    while (std::getline(std::cin, line))
    {
        if (line[0] == 'F')
        {
            std::cout << std::flush;
#ifdef STATISTICS
            batchCount++;
#endif
        }
        else
        {
            Query query;
            loadQuery(query, line);
            auto result = executor.executeQuery(database, query);
            result[result.size() - 1] = '\n';
            std::cout << result;

#ifdef STATISTICS
            queryCount++;
            joinCount += query.joins.size();
            filterCount += query.filters.size();

            std::unordered_set<std::string> pairs;
            std::unordered_set<int> joinedRelations;
            for (auto& join : query.joins)
            {
                auto smaller = std::min(join.selections[0].relation, join.selections[1].relation);
                auto larger = std::max(join.selections[0].relation, join.selections[1].relation);
                auto key = std::to_string(smaller) + "." + std::to_string(larger);
                if (pairs.find(key) != pairs.end())
                {
                    multipleColumnsPerRelJoins++;
                    break;
                }
                pairs.insert(key);

                joinedRelations.insert(smaller);
                joinedRelations.insert(larger);
            }

            if (joinedRelations.size() < query.relations.size())
            {
                relationsMissingInJoins++;
            }
#endif
        }
    }

#ifdef STATISTICS
    size_t relationCount = database.relations.size();
    std::cerr << "ColumnRelation count: " << relationCount << std::endl;
    std::cerr << "Min tuple count: " << minTuples << std::endl;
    std::cerr << "Max tuple count: " << maxTuples << std::endl;
    std::cerr << "Avg tuple count: " << tupleCount / (double) relationCount << std::endl;
    std::cerr << "Min column count: " << minColumns << std::endl;
    std::cerr << "Max column count: " << maxColumns << std::endl;
    std::cerr << "Avg column count: " << columnCount / (double) relationCount << std::endl;
    std::cerr << "Query count: " << queryCount << std::endl;
    std::cerr << "Join count: " << joinCount << std::endl;
    std::cerr << "Filter count: " << filterCount << std::endl;
    std::cerr << "Batch count: " << batchCount << std::endl;
    std::cerr << "Multiple-columns per relation joins: " << multipleColumnsPerRelJoins << std::endl;
    std::cerr << "Relations missing on joins: " << relationsMissingInJoins << std::endl;
#endif

    std::quick_exit(0);
}
