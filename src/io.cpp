#include "io.h"

#include <sys/stat.h>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <cstring>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

#include "settings.h"
#include "stats.h"
#include "query.h"

void loadDatabase(Database& database)
{
    std::string line;
    struct stat stats{};

    uint32_t columnId = 0;
    while (std::getline(std::cin, line))
    {
        if (line == "Done") break;

        int fd = open(line.c_str(), O_RDONLY);
        fstat(fd, &stats);

        auto length = (size_t) stats.st_size;
        auto* addr = static_cast<char*>(mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, 0));

        database.relations.emplace_back();
        ColumnRelation& rel = database.relations.back();

        rel.cumulativeColumnId = columnId;
        rel.tupleCount = *reinterpret_cast<uint64_t*>(addr);
        addr += sizeof(uint64_t);
        rel.columnCount = static_cast<uint32_t>(*reinterpret_cast<uint64_t*>(addr));
        addr += sizeof(uint64_t);
        rel.data = new uint64_t[rel.tupleCount * rel.columnCount];
        columnId += rel.columnCount;

#ifdef TRANSPOSE_RELATIONS
        auto data = new uint64_t[rel.tupleCount * rel.columnCount];
        std::memcpy(data, addr, rel.tupleCount * rel.columnCount * sizeof(uint64_t));

        for (int c = 0; c < static_cast<int32_t>(rel.columnCount); c++)
        {
            for (int r = 0; r < static_cast<int32_t>(rel.tupleCount); r++)
            {
                rel.data[r * rel.columnCount + c] = data[c * rel.tupleCount + r];
            }
        }

        delete[] data;
#else
        std::memcpy(rel.data, addr, rel.tupleCount * rel.columnCount * sizeof(uint64_t));
#endif

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

    for (int r = 0; r < static_cast<int32_t>(database.relations.size()); r++)
    {
        for (int i = 0; i < static_cast<int32_t>(database.relations[r].columnCount); i++)
        {
#ifdef USE_HASH_INDEX
            database.hashIndices.push_back(std::make_unique<HashIndex>(database.relations[r], i));
#endif
#ifdef USE_SORT_INDEX
            database.sortIndices.push_back(std::make_unique<SortIndex>(database.relations[r], i));
#endif
        }

        database.histograms.emplace_back();
    }

#ifdef REAL_RUN
    #pragma omp parallel for
    for (int i = 0; i < static_cast<int32_t>(columnId); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(columnId); i++)
#endif
    {
#ifdef USE_HASH_INDEX
        database.hashIndices[i]->build();
#endif
#ifdef USE_SORT_INDEX
        database.sortIndices[i]->build();
#endif
    }

#ifdef USE_HISTOGRAM
#ifdef REAL_RUN
    #pragma omp parallel for
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#endif
    {
        database.histograms[i].loadRelation(database.relations[i]);
    }
#endif
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

static SelectionId getJoinId(uint32_t bindingA, uint32_t bindingB)
{
    uint32_t first = std::min(bindingA, bindingB);
    uint32_t second = std::max(bindingA, bindingB);

    return second * 1000 + first;
}

void createComponents(Query& query)
{
    std::unordered_map<uint32_t, std::unique_ptr<UnionFind>> components;

    for (auto& join: query.joins)
    {
        for (auto& pred: join)
        {
            auto left = components.find(pred.selections[0].getId());
            if (left == components.end())
            {
                left = components.insert(std::make_pair(pred.selections[0].getId(),
                                                        std::make_unique<UnionFind>(pred.selections[0]))).first;
            }
            auto right = components.find(pred.selections[1].getId());
            if (right == components.end())
            {
                right = components.insert(std::make_pair(pred.selections[1].getId(),
                                                         std::make_unique<UnionFind>(pred.selections[1]))).first;
            }

            left->second->join(right->second.get());
        }
    }


    std::unordered_map<UnionFind*, std::vector<Selection>> groups; // components of join graph
    for (auto& kv: components)
    {
        auto parent = kv.second->findParent();
        auto it = groups.find(parent);
        if (it == groups.end())
        {
            it = groups.insert({ parent, {} }).first;
        }
        it->second.push_back(kv.second->selection);
    }

    // iterate every component
    for (auto& kv: groups)
    {
        std::unordered_map<uint32_t, std::vector<Selection>> byBinding;
        for (auto& sel: kv.second)
        {
            auto it = byBinding.find(sel.binding);
            if (it == byBinding.end())
            {
                it = byBinding.insert({ sel.binding, {} }).first;
            }
            it->second.push_back(sel);
        }

        for (auto& bind: byBinding)
        {
            if (bind.second.size() > 1)
            {
                for (auto& s: bind.second)
                {
                    auto& v = query.selfJoins[bind.first];
                    if (std::find(v.begin(), v.end(), s) == v.end())
                    {
                        v.push_back(s);
                    }
                }
            }
        }
    }

    // rewrite sum selections
    std::vector<Selection> usedSelections;
    for (int i = 0; i < static_cast<int32_t>(query.selections.size()); i++)
    {
        auto& sel = query.selections[i];
        for (auto& used: usedSelections)
        {
            auto usedIt = components.find(used.getId());
            auto selIt = components.find(sel.getId());
            if (usedIt != components.end() && selIt != components.end())
            {
                if (usedIt->second->findParent() == selIt->second->findParent())
                {
                    query.selections[i] = used;
                    break;
                }
            }
        }
        usedSelections.push_back(query.selections[i]);
    }
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

    std::unordered_map<SelectionId, uint32_t> joinMap;

    // load predicates
    while (true)
    {
        auto binding = static_cast<uint32_t>(readInt(line, index)); // index to query relations
        uint32_t relation = query.relations[binding];
        index++;
        Selection selection(relation, binding, (uint32_t) readInt(line, index));
        char oper = line[index++];

        uint64_t value = readInt(line, index);
        if (EXPECT(line[index] == '.')) // parse join
        {
            auto joinId = getJoinId(binding, static_cast<uint32_t>(value));
            auto it = joinMap.find(joinId);
            Join* join;

            if (it == joinMap.end())
            {
                query.joins.emplace_back();
                join = &query.joins.back();
                joinMap.insert({ joinId, query.joins.size() - 1 });
            }
            else join = &query.joins[it->second];

            join->emplace_back();
            auto& predicate = join->back();

            predicate.selections[0] = selection;
            predicate.selections[1].relation = query.relations[value];
            predicate.selections[1].binding = static_cast<uint32_t>(value);
            index++;
            predicate.selections[1].column = (uint32_t) readInt(line, index);

            // normalize the order of bindings in multiple-column joins
            if (predicate.selections[0].binding > predicate.selections[1].binding)
            {
                std::swap(predicate.selections[0], predicate.selections[1]);
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
        auto binding = readInt(line, index);
        auto relation = query.relations[binding];
        index++;
        query.selections.emplace_back(relation, binding, readInt(line, index));

        if (line[index++] == '|') break;
    }

    createComponents(query);

#ifdef STATISTICS
    query.input = line;
#endif
}
