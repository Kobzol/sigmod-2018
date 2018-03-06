#include "io.h"

#include <sys/stat.h>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <cstring>
#include <unistd.h>

#include "settings.h"
#include "stats.h"

void loadDatabase(Database& database)
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

        database.relations.emplace_back();
        ColumnRelation& rel = database.relations.back();

        rel.tupleCount = *reinterpret_cast<uint64_t*>(addr);
        addr += sizeof(uint64_t);
        rel.columnCount = *reinterpret_cast<uint64_t*>(addr);
        addr += sizeof(uint64_t);
        rel.data = new uint64_t[rel.tupleCount * rel.columnCount];
        rel.id = static_cast<uint32_t>(database.relations.size() - 1);
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

        bool sorted = true;
        for (size_t i = 1; i < rel.tupleCount; i++)
        {
            if (rel.getValue(i, 0) <= rel.getValue(i - 1, 0))
            {
                sorted = false;
                break;
            }
        }

        if (sorted)
        {
            sortedOnFirstColumn++;
        }
#endif
    }
}
uint64_t readInt(std::string& str, int& index)
{
    uint64_t value = 0;
    while (isdigit(str[index]))
    {
        value *= 10;
        value += str[index++] - '0';
    }

    return value;
}
void loadQuery(Query& query, std::string& line)
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
