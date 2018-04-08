#include <omp.h>
#include "io.h"
#include "index/index-thread-pool.h"
#include "timer.h"
#include "foreign-key/foreign-key-checker.h"
#include "rewrite/rewrite.h"
#include "index/index-builder.h"

void loadDatabase(Database& database)
{
    std::string line;
    struct stat stats{};

    Timer loadTimer;
    uint32_t columnId = 0;
    while (std::getline(std::cin, line))
    {
        if (line == "Done") break;

#ifdef __linux__		
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
        rel.data = reinterpret_cast<uint64_t*>(addr);//new uint64_t[rel.tupleCount * rel.columnCount];
        rel.id = static_cast<uint32_t>(database.relations.size() - 1);
        columnId += rel.columnCount;

        //std::memcpy(rel.data, addr, rel.tupleCount * rel.columnCount * sizeof(uint64_t));

        //munmap(addr, length);
        close(fd);

#else
		database.relations.emplace_back();
		ColumnRelation& rel = database.relations.back();
		rel.cumulativeColumnId = columnId;		

		std::ifstream is(line, std::ifstream::binary);

		if (is)
		{
			uint64_t rowCount = 0, columnCount = 0;
			is.read((char*)&rowCount, sizeof(uint64_t));
			is.read((char*)&columnCount, sizeof(uint64_t));

			rel.tupleCount = rowCount;
			rel.columnCount = columnCount;

			rel.data = new uint64_t[rel.tupleCount * rel.columnCount];

			is.read((char*)rel.data, rel.tupleCount * rel.columnCount * sizeof(uint64_t));

			is.close();

			columnId += rel.columnCount;
		}
		else
		{
			std::cerr << "cannot open " << line << std::endl;
		}
#endif

#ifdef STATISTICS
        tupleCount += rel.tupleCount;
        columnCount += rel.columnCount;
#ifdef __linux__
		minTuples = std::min(minTuples, rel.tupleCount);
		maxTuples = std::max(maxTuples, rel.tupleCount);
		minColumns = std::min(minColumns, rel.columnCount);
		maxColumns = std::max(maxColumns, rel.columnCount);
#else
		minTuples = min(minTuples, rel.tupleCount);
		maxTuples = max(maxTuples, rel.tupleCount);
		minColumns = min(minColumns, rel.columnCount);
		maxColumns = max(maxColumns, rel.columnCount);
#endif
#endif
    }

#ifdef STATISTICS
    relationLoadTime = loadTimer.get();
#endif

    database.unique.resize(columnId);

    Timer transposeTimer;
    std::vector<uint64_t*> relationData(database.relations.size());

#ifdef USE_PRIMARY_INDEX
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
    {
        if (database.relations[i].getColumnCount() == 1)
        {
            relationData[i] = database.relations[i].transposed = database.relations[i].data;
        }
        else if (PrimaryIndex::canBuild(database.relations[i]))
        {
            relationData[i] = static_cast<uint64_t*>(malloc(sizeof(uint64_t) *
                                                            PrimaryIndex::rowSize(database.relations[i]) *
                                                            database.relations[i].getRowCount()));
            database.relations[i].transposed = relationData[i];
        }
    }

    for (int i = 0; i < static_cast<int32_t>(relationData.size()); i++)
    {
        if (PrimaryIndex::canBuild(database.relations[i]) && database.relations[i].getColumnCount() > 1)
        {
            auto rows = static_cast<int32_t>(database.relations[i].getRowCount());

#pragma omp parallel for num_threads(20)
            for (int r = 0; r < rows; r++)
            {
                int columns = database.relations[i].getColumnCount();
                for (int c = 0; c < static_cast<int32_t>(columns); c++)
                {
                    relationData[i][r * columns + c] = database.relations[i].getValue(static_cast<size_t>(r),
                                                                                      static_cast<size_t>(c));
                }
            }
        }
    }
#endif

#ifdef STATISTICS
    transposeTime = transposeTimer.get();
    Timer indicesInitTimer;
#endif

    for (int r = 0; r < static_cast<int32_t>(database.relations.size()); r++)
    {
        for (int i = 0; i < static_cast<int32_t>(database.relations[r].columnCount); i++)
        {
            database.hashIndices.push_back(std::make_unique<HashIndex>(database.relations[r], i));
            database.sortIndices.push_back(std::make_unique<SortIndex>(database.relations[r], i));
            database.aggregateIndices.push_back(std::make_unique<AggregateIndex>(database.relations[r], i,
                                                                                 *database.sortIndices.back()));
            database.primaryIndices.push_back(std::make_unique<PrimaryIndex>(database.relations[r], i,
                                                                             relationData[r]));
        }

#ifdef USE_HISTOGRAM
        database.histograms.emplace_back();
#endif
    }

#ifdef STATISTICS
    indicesInitTime = indicesInitTimer.get();
#endif

    Timer startIndexTimer;
#ifdef USE_INDEX_THREADPOOL
#ifdef USE_SORT_INDEX
    int start = 0;
#ifdef USE_PRIMARY_INDEX
    start = 1;
#endif

#pragma omp parallel for
    for (int r = 0; r < static_cast<int32_t>(database.relations.size()); r++)
    {
        if (startIndexTimer.get() > INDEX_THREAD_BAILOUT) continue;
        for (int c = start; c < static_cast<int32_t>(database.relations[r].columnCount); c++)
        {
            if (startIndexTimer.get() > INDEX_THREAD_BAILOUT)
            {
                break;
            }
            auto& index = database.primaryIndices[database.getGlobalColumnId(static_cast<uint32_t>(r), 0)];
            if (index->take())
            {
                index->build();
            }
            auto& index = database.sortIndices[database.getGlobalColumnId(static_cast<uint32_t>(r),
                                                                          static_cast<uint32_t>(c))];
            if (index->take())
            {
                index->build();

#ifdef USE_AGGREGATE_INDEX
                auto& aggregated = database.aggregateIndices[database.getGlobalColumnId(static_cast<uint32_t>(r),
                                                                                        static_cast<uint32_t>(c))];
                if (aggregated->take())
                {
                    aggregated->build();
                }
#endif
            }
        }
    }
#endif
    threadIndexPool.start();
#else
    std::vector<uint32_t> indices;
    for (int i = 0; i < static_cast<int32_t>(columnId); i++)
    {
        if (database.primaryIndices[i]->column < PREBUILD_PRIMARY_COLUMNS)
        {
            indices.push_back(i);
        }
    }

    IndexBuilder builder;
    builder.buildIndices(indices, 10, 4);
/*#ifdef USE_THREADS
    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < static_cast<int32_t>(columnId); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(columnId); i++)
#endif
    {
#ifdef USE_HASH_INDEX
        if (database.hashIndices[i]->take())
        {
            database.hashIndices[i]->build();
        }
#endif
        bool built = false;
#ifdef USE_SORT_INDEX
        if (!built && database.sortIndices[i]->column < PREBUILD_PRIMARY_COLUMNS && database.sortIndices[i]->take())
        {
            database.sortIndices[i]->build(4);

#ifdef USE_AGGREGATE_INDEX
            if (database.aggregateIndices[i]->take())
            {
                database.aggregateIndices[i]->build();
            }
#endif
        }
#endif
    }
#endif*/
#endif

#ifdef STATISTICS
    startIndexTime = startIndexTimer.get();
#endif
#ifdef USE_HISTOGRAM
    Timer histogramTimer;

#ifdef USE_THREADS
    //#pragma omp parallel for
    std::vector<uint32_t*> hist_aux;
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#endif
    {
        database.histograms[i].loadRelation(database.relations[i], hist_aux);
    }
#ifdef STATISTICS
    histogramTime = histogramTimer.get();
#endif
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
#ifdef __linux__
    uint32_t first = std::min(bindingA, bindingB);
    uint32_t second = std::max(bindingA, bindingB);
#else
	uint32_t first = min(bindingA, bindingB);
	uint32_t second = max(bindingA, bindingB);
#endif

    return second * 1000 + first;
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

#ifdef STATISTICS
            auto leftUnique = database.unique[database.getGlobalColumnId(predicate.selections[0].relation, predicate.selections[0].column)];
            auto rightUnique = database.unique[database.getGlobalColumnId(predicate.selections[1].relation, predicate.selections[1].column)];
            if (leftUnique || rightUnique) joinOneUnique++;
            if (leftUnique && rightUnique) joinBothUnique++;
#endif

            // normalize the order of bindings in multiple-column joins
            if (predicate.selections[0].binding < predicate.selections[1].binding)
            {
                std::swap(predicate.selections[0], predicate.selections[1]);
            }

            bool unique = true;
            for (int i = 0; i < static_cast<int32_t>(join->size()) - 1; i++)
            {
                auto& j = (*join)[i];
                if (j.selections[0] == predicate.selections[0] && j.selections[1] == predicate.selections[1])
                {
                    unique = false;
                    break;
                }
            }

            if (!unique) join->pop_back();
        }
        else // parse filter
        {
            query.filters.emplace_back(selection, value, nullptr, oper);

            bool unique = true;
            for (int i = 0; i < static_cast<int32_t>(query.filters.size()) - 1; i++)
            {
                if (query.filters[i].selection == query.filters.back().selection)
                {
                    if (query.filters[i].value == query.filters.back().value &&
                            query.filters[i].oper == query.filters.back().oper)
                    {
                        unique = false;
                        break;
                    }
                }
            }

            if (!unique) query.filters.pop_back();
        }

        if (line[index++] == '|') break;
    }

#ifdef STATISTICS
    for (auto& join: query.joins)
    {
        for (auto& j: join)
        {
            for (auto& s: j.selections)
            {
                for (auto& f: query.filters)
                {
                    if (f.selection == s && f.oper == '=')
                    {
                        filterEqualsJoined++;
                    }
                }
            }
        }
    }
#endif

    // load selections
    while (true)
    {
        auto binding = readInt(line, index);
        auto relation = query.relations[binding];
        index++;
        query.selections.emplace_back(relation, binding, readInt(line, index));

        if (line[index++] == '|') break;
    }

#ifdef STATISTICS
    query.input = line;
#endif
}
