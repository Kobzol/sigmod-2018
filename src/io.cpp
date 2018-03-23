#include "io.h"
#include "index/index-thread-pool.h"
#include "timer.h"

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
        //std::memcpy(rel.data, addr, rel.tupleCount * rel.columnCount * sizeof(uint64_t));
#endif

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

#ifdef TRANSPOSE_RELATIONS
			// TODO
#else
			is.read((char*)rel.data, rel.tupleCount * rel.columnCount * sizeof(uint64_t));
#endif

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

        /*for (int c = 0; c < rel.columnCount; c++)
        {
            bool sorted = true;
            for (size_t i = 1; i < rel.tupleCount; i++)
            {
                if (rel.getValue(i, c) <= rel.getValue(i - 1, c))
                {
                    sorted = false;
                    break;
                }
            }

            if (sorted)
            {
                sortedOnFirstColumn++;
            }
        }*/
#endif
    }

#ifdef STATISTICS
    relationLoadTime = loadTimer.get();
#endif

    Timer transposeTimer;
    std::vector<uint64_t*> relationData(database.relations.size());

#ifdef USE_PRIMARY_INDEX
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
    {
        relationData[i] = new uint64_t[PrimaryIndex::rowSize(database.relations[i]) *
                database.relations[i].getRowCount()];
    }

#ifdef USE_THREADS
#pragma omp parallel for
    for (int i = 0; i < static_cast<int32_t>(relationData.size()); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(relationData.size()); i++)
#endif
    {
        if (PrimaryIndex::canBuild(database.relations[i]))
        {
            auto rows = static_cast<int32_t>(database.relations[i].getRowCount());
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

#ifdef USE_PRIMARY_INDEX
        // free memory
        //relationData[r]->resize(0);
        //relationData[r]->shrink_to_fit();
#endif

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
#ifdef USE_THREADS
    #pragma omp parallel for
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
#ifdef USE_PRIMARY_INDEX
        if (database.primaryIndices[i]->column == 0 && database.primaryIndices[i]->take())
        {
            built = database.primaryIndices[i]->build();
        }
#endif
#ifdef USE_SORT_INDEX
        if (!built && database.sortIndices[i]->take())
        {
            database.sortIndices[i]->build();

#ifdef USE_AGGREGATE_INDEX
            if (database.aggregateIndices[i]->take())
            {
                database.aggregateIndices[i]->build();
            }
#endif
        }
#endif
    }
#endif

#ifdef STATISTICS
    startIndexTime = startIndexTimer.get();
#endif
#ifdef USE_HISTOGRAM
    Timer histogramTimer;

#ifdef USE_THREADS
    #pragma omp parallel for
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#else
    for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
#endif
    {
        database.histograms[i].loadRelation(database.relations[i]);
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

std::vector<Filter> getFilters(const std::vector<Filter>& filters, const Selection& selection)
{
    std::vector<Filter> selected;
    for (auto& f: filters)
    {
        if (f.selection == selection) selected.push_back(f);
    }
    return selected;
}
bool containsFilter(const std::vector<Filter>& filters, const Filter& filter)
{
    return std::find_if(filters.begin(), filters.end(), [&filter](const Filter& f) {
        return f == filter;
    }) != filters.end();
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

#ifdef EXPAND_FILTERS
        std::vector<Filter> componentFilters;
        for (auto& sel: kv.second)
        {
            auto filters = getFilters(query.filters, sel);
            for (auto& f: filters)
            {
                if (!containsFilter(componentFilters, f))
                {
                    componentFilters.push_back(f);
                }
            }
        }

        if (componentFilters.empty()) continue;

        bool impossible = false;
        std::unordered_set<uint64_t> equals;

        uint64_t minValue = 0;
        uint64_t maxValue = std::numeric_limits<uint64_t>::max();

        for (auto& f: componentFilters)
        {
            if (f.oper == '=')
            {
                equals.insert(f.value);
                minValue = f.value;
                maxValue = f.value;
                if (equals.size() > 1)
                {
                    impossible = true;
                    break;
                }
            }
            else if (f.oper == '<')
            {
                maxValue = std::min(f.value, maxValue);
            }
            else minValue = std::max(f.value, minValue);
        }

        if (minValue == maxValue)
        {
            componentFilters = {
                    Filter(Selection(0, 0, 0), minValue, nullptr, '=')
            };
        }
        else if (minValue > 0 && maxValue < std::numeric_limits<uint64_t>::max())
        {
            componentFilters = {
                    Filter(Selection(0, 0, 0), minValue, nullptr, '>'),
                    Filter(Selection(0, 0, 0), maxValue, nullptr, '<')
            };
        }

        query.impossible = impossible;

        for (auto& sel: kv.second)
        {
            for (auto& filter: componentFilters)
            {
                auto assigned = filter;
                assigned.selection = sel;
                if (!containsFilter(query.filters, assigned))
                {
                    query.filters.push_back(assigned);
                }
            }
        }
#endif
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
#ifdef COMPILE_FILTERS
            query.filters.back().evaluator = FilterCompiler().compile(std::vector<Filter>{ query.filters.back() });
#endif
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

    createComponents(query);

#ifdef STATISTICS
    query.input = line;
#endif
}
