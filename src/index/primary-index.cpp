#include "primary-index.h"
#include "../relation/column-relation.h"
#include "../thirdparty/kxsort.h"
#include "../database.h"
#include "../timer.h"
#include "../stats.h"
#include "sort.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <omp.h>

bool PrimaryIndex::canBuild(ColumnRelation& relation)
{
    return relation.getColumnCount() <= PRIMARY_INDEX_MAX_COLUMNS;
}
int PrimaryIndex::rowSize(ColumnRelation& relation)
{
    int columns = relation.getColumnCount();
    int perRow = 0;

    if (columns == 1) perRow = sizeof(Row<1>);
    if (columns == 2) perRow = sizeof(Row<2>);
    if (columns == 3) perRow = sizeof(Row<3>);
    if (columns == 4) perRow = sizeof(Row<4>);
    if (columns == 5) perRow = sizeof(Row<5>);
    if (columns == 6) perRow = sizeof(Row<6>);
    if (columns == 7) perRow = sizeof(Row<7>);
    if (columns == 8) perRow = sizeof(Row<8>);
    if (columns == 9) perRow = sizeof(Row<9>);

    return perRow;
}
PrimaryRowEntry* PrimaryIndex::move(ColumnRelation& relation, PrimaryRowEntry* entry, int offset)
{
    return entry + (offset * (PrimaryIndex::rowSize(relation) / sizeof(PrimaryRowEntry)));
}
int64_t PrimaryIndex::count(ColumnRelation& relation, PrimaryRowEntry* from, PrimaryRowEntry* to)
{
    return (to - from) / (PrimaryIndex::rowSize(relation) / sizeof(PrimaryRowEntry));
}

PrimaryIndex::PrimaryIndex(ColumnRelation& relation, uint32_t column, uint64_t* init)
    : Index(relation, column), init(init)
{

}

static std::pair<uint64_t, uint64_t> findMinMax(const uint64_t* __restrict__ column, int rows)
{
    uint64_t minValue = std::numeric_limits<uint64_t>::max();
    uint64_t maxValue = 0;

//#pragma omp parallel for reduction(min:minValue) reduction(max:maxValue) num_threads(4)
    for (int i = 0; i < rows; i++)
    {
        auto value = column[i];
        minValue = value < minValue ? value : minValue;
        maxValue = value > maxValue ? value : maxValue;
    }

    return std::make_pair(minValue, maxValue);
}
void PrimaryIndex::initMemory()
{
    assert(this->init != nullptr);

    auto rows = static_cast<int>(this->relation.getRowCount());

    this->rowSizeBytes = PrimaryIndex::rowSize(this->relation);
    this->rowOffset = this->rowSizeBytes / sizeof(PrimaryRowEntry);

    this->mem = new uint64_t[this->rowOffset * rows];
    this->data = reinterpret_cast<PrimaryRowEntry*>(this->mem);
    this->begin = this->data;
    this->end = this->move(this->data, rows);

#ifdef STATISTICS
    Timer timer;
#endif

    auto* columnPtr = this->relation.data + (this->column * rows);
    auto minMax = findMinMax(columnPtr, rows);

    this->minValue = minMax.first;
    this->maxValue = minMax.second;
    this->diff = (this->maxValue - this->minValue) + 1;

#ifdef STATISTICS
    indexMinMaxTime += timer.get();
#endif
}

void PrimaryIndex::initGroups(int threads)
{
#ifdef STATISTICS
    Timer timer;
#endif

    auto rows = static_cast<int32_t>(this->relation.getRowCount());

    const int TARGET_SIZE = 1024 * 512;
    double size = (rows * this->rowSizeBytes) / static_cast<double>(TARGET_SIZE);
    const auto GROUP_COUNT = static_cast<int>(std::min((this->maxValue - this->minValue) + 1,
                                                       static_cast<uint64_t>(std::ceil(size))));
    auto diff = std::max(((this->maxValue - this->minValue) + 1) / GROUP_COUNT + 1, 1UL);
    auto shift = static_cast<uint64_t>(std::ceil(std::log2(diff)));

//    this->groups.resize(static_cast<size_t>(GROUP_COUNT));
    this->rowTargets = static_cast<PrimaryRowTarget*>(
            malloc(static_cast<size_t>(rows) * sizeof(std::pair<uint32_t, uint32_t>))
    ); // group, index
    this->counts.resize(static_cast<size_t>(GROUP_COUNT));
    this->starts.resize(static_cast<size_t>(GROUP_COUNT));
    this->unique.resize(static_cast<size_t>(GROUP_COUNT));
    this->computeOffsets.resize(static_cast<size_t>(GROUP_COUNT * threads));

    auto minValue = this->minValue;
    auto* ptr = this->relation.data + (rows * this->column);

    std::vector<std::vector<uint32_t>> counts(static_cast<size_t>(threads));
    for (int i = 0; i < threads; i++)
    {
        counts[i].resize(static_cast<size_t>(GROUP_COUNT));
    }

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < rows; i++)
    {
        auto groupIndex = (ptr[i] - minValue) >> shift;
        this->rowTargets[i].group = static_cast<uint32_t>(groupIndex);
        this->rowTargets[i].index = static_cast<uint32_t>(counts[omp_get_thread_num()][groupIndex]++);
    }

    for (int t = 0; t < threads; t++)
    {
        this->computeOffsets[t * GROUP_COUNT] = this->counts[0];
        this->counts[0] += counts[t][0];
    }

    for (int g = 1; g < GROUP_COUNT; g++)
    {
        uint32_t count = 0;
        for (int t = 0; t < threads; t++)
        {
            this->computeOffsets[t * GROUP_COUNT + g] = count;
            count += counts[t][g];
        }
        this->counts[g] = count;
        this->starts[g] = this->starts[g - 1] + this->counts[g - 1];
    }

//    uint32_t minGroup = this->groups[0].count;
//    uint32_t maxGroup = this->groups[0].count;
//    int nonZero = minGroup == 0 ? 0 : 1;
//    for (int i = 1; i < GROUP_COUNT; i++)
//    {
//        this->groups[i].start = this->groups[i - 1].start + this->groups[i - 1].count;
//        minGroup = std::min(minGroup, this->groups[i].count);
//        maxGroup = std::max(maxGroup, this->groups[i].count);
//        if (this->groups[i].count > 0) nonZero++;
//    }

    /*std::vector<PrimaryGroup> sortGroups(this->groups.begin(), this->groups.end());
    std::sort(sortGroups.begin(), sortGroups.end(), [](const PrimaryGroup& a, const PrimaryGroup& b) {
        return a.count < b.count;
    });

    std::cerr << "(" << GROUP_COUNT << ", " << nonZero << ", " << minGroup << ", " << maxGroup;
    std::cerr << ", " << sortGroups[sortGroups.size() / 2].count << ") ";*/

#ifdef STATISTICS
    indexGroupCountTime += timer.get();
#endif
}

void PrimaryIndex::finalize()
{
    if (!canBuild(this->relation))
    {
        return;
    }

#ifdef STATISTICS
    Timer timer;
#endif

    bool unique = true;
    if (!this->unique.empty())
    {
        for (auto& u: this->unique)
        {
            if (!u)
            {
                unique = false;
                break;
            }
        }
    }
    else
    {
        auto rows = static_cast<int>(this->relation.getRowCount());
#ifdef USE_MULTILEVEL_INDEX
        this->groupCount = 128;
    this->indexGroups.resize(static_cast<size_t>(this->groupCount));

    int group = 0;
    this->indexGroups[0].startValue = this->minValue;
    this->indexGroups[0].start = this->begin;

    bool unique = true;
    uint64_t lastValue = this->begin->row[this->column];
    for (int i = 1; i < rows; i++)
    {
        uint64_t value = this->move(this->begin, i)->row[this->column];

        if (this->groupValue(value) > group && lastValue != value)
        {
            this->indexGroups[group].endValue = value;
            this->indexGroups[group].end = this->move(this->begin, i);
            this->indexGroups[group + 1].startValue = value;
            this->indexGroups[group + 1].start = this->groups[group].end;
            group++;
        }

        if (lastValue == value)
        {
            unique = false;
        }
        lastValue = value;
    }

    this->indexGroups[group].endValue = this->maxValue + 1;
    this->indexGroups[group].end = this->end;
    this->indexGroups.resize(group + 1);
#else
        uint64_t lastValue = this->begin->row[this->column];

        for (int i = 1; i < rows; i++)
        {
            uint64_t value = this->move(this->begin, i)->row[this->column];
            if (NOEXPECT(lastValue == value))
            {
                unique = false;
                break;
            }
            else lastValue = value;
        }
#endif
    }

    database.unique[database.getGlobalColumnId(this->relation.id, this->column)] = static_cast<unsigned int>(unique);

    if (this->columns == 1) this->lowerBoundFn = &PrimaryIndex::findLowerBound<1>;
    if (this->columns == 2) this->lowerBoundFn = &PrimaryIndex::findLowerBound<2>;
    if (this->columns == 3) this->lowerBoundFn = &PrimaryIndex::findLowerBound<3>;
    if (this->columns == 4) this->lowerBoundFn = &PrimaryIndex::findLowerBound<4>;
    if (this->columns == 5) this->lowerBoundFn = &PrimaryIndex::findLowerBound<5>;
    if (this->columns == 6) this->lowerBoundFn = &PrimaryIndex::findLowerBound<6>;
    if (this->columns == 7) this->lowerBoundFn = &PrimaryIndex::findLowerBound<7>;
    if (this->columns == 8) this->lowerBoundFn = &PrimaryIndex::findLowerBound<8>;
    if (this->columns == 9) this->lowerBoundFn = &PrimaryIndex::findLowerBound<9>;

    this->buildCompleted = true;

#ifdef STATISTICS
    indexFinalizeTime += timer.get();
#endif
}

bool PrimaryIndex::build(uint32_t threads)
{
    if (!canBuild(this->relation))
    {
        return false;
    }

    if (this->relation.getRowCount() > 100000)
    {
        this->initGroups(threads);
        this->sortGroups(threads);
    }
    else this->sortDirect();

    this->finalize();

    return true;
}

PrimaryRowEntry* PrimaryIndex::lowerBound(uint64_t value)
{
    return (this->*lowerBoundFn)(this->mem, this->relation.getRowCount(), value, this->column);
}

PrimaryRowEntry* PrimaryIndex::upperBound(uint64_t value)
{
    if (this->columns == 1) return findUpperBound<1>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 2) return findUpperBound<2>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 3) return findUpperBound<3>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 4) return findUpperBound<4>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 5) return findUpperBound<5>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 6) return findUpperBound<6>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 7) return findUpperBound<7>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 8) return findUpperBound<8>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 9) return findUpperBound<9>(this->mem, this->relation.getRowCount(), value, this->column);

    assert(false);
    return nullptr;
}

int64_t PrimaryIndex::count(PrimaryRowEntry* from, PrimaryRowEntry* to)
{
    return (to - from) / this->rowOffset;
}

template<int N>
PrimaryRowEntry* PrimaryIndex::findLowerBound(uint64_t* mem, int64_t rows, uint64_t value, uint32_t column)
{
    if (value < this->minValue) return this->begin;
    if (value > this->maxValue) return this->end;

#ifdef USE_MULTILEVEL_INDEX
    for (auto& group: this->indexGroups)
    {
        if (group.startValue <= value && value < group.endValue)
        {
            auto* ptr = reinterpret_cast<Row<N>*>(group.start);
            auto* end = reinterpret_cast<Row<N>*>(group.end);
            auto iter = std::lower_bound(ptr, end, value, [column](const Row<N>& entry, uint64_t val) {
                return entry.row[column] < val;
            });

            return reinterpret_cast<PrimaryRowEntry*>(iter);

        }
    }
#endif

    auto* ptr = reinterpret_cast<Row<N>*>(mem);
    auto iter = std::lower_bound(ptr, ptr + rows, value, [column](const Row<N>& entry, uint64_t val) {
        return entry.row[column] < val;
    });
    return reinterpret_cast<PrimaryRowEntry*>(ptr + (iter - ptr));
}

template<int N>
PrimaryRowEntry* PrimaryIndex::findUpperBound(uint64_t* mem, int64_t rows, uint64_t value, uint32_t column)
{
    auto* ptr = reinterpret_cast<Row<N>*>(mem);
    auto iter = std::upper_bound(ptr, ptr + rows, value, [column](uint64_t val, const Row<N>& entry) {
        return val < entry.row[column];
    });
    return reinterpret_cast<PrimaryRowEntry*>(ptr + (iter - ptr));
}

template<int N>
void PrimaryIndex::sort(PrimaryRowEntry* mem, PrimaryRowEntry* end, uint32_t column,
                        uint64_t minValue, uint64_t maxValue)
{
    uint64_t diff = maxValue - minValue + 1;

    auto* ptr = reinterpret_cast<Row<N>*>(mem);
    auto* stop = reinterpret_cast<Row<N>*>(end);
    kx::radix_sort(ptr, stop, RadixTraitsRow<N>(column, minValue), diff);
}

template <int N>
static void copyBuckets(uint64_t* __restrict__ dest, const uint64_t* __restrict__ src,
                        const uint32_t* __restrict__ starts,
                        const PrimaryRowTarget* __restrict__ rowTargets,
                        const uint32_t* __restrict__ computeOffsets,
                        int rows, int groupCount, int threads)
{
    auto* __restrict__ to = reinterpret_cast<Row<N>*>(dest);
    const auto* __restrict__ from = reinterpret_cast<const Row<N>*>(src);

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < rows; i++)
    {
        int group = rowTargets[i].group;
        auto row = starts[group] + rowTargets[i].index;
        row += (&computeOffsets[omp_get_thread_num() * groupCount])[group];
        to[row] = from[i];
    }
}

void PrimaryIndex::sortDirect()
{
    auto rows = static_cast<int32_t>(this->relation.getRowCount());
    std::memcpy(this->data, this->init, this->rowSizeBytes * rows);

    auto from = this->move(this->data, 0);
    auto to = this->move(this->data, rows);

    if (this->columns == 1) sort<1>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 2) sort<2>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 3) sort<3>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 4) sort<4>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 5) sort<5>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 6) sort<6>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 7) sort<7>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 8) sort<8>(from, to, this->column, this->minValue, this->maxValue);
    if (this->columns == 9) sort<9>(from, to, this->column, this->minValue, this->maxValue);
}

void PrimaryIndex::sortGroups(int threads)
{
    auto rows = static_cast<int>(this->relation.getRowCount());

#ifdef STATISTICS
    Timer timer;
#endif

    auto* target = reinterpret_cast<uint64_t*>(this->data);
    if (this->columns == 1) copyBuckets<1>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 2) copyBuckets<2>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 3) copyBuckets<3>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 4) copyBuckets<4>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 5) copyBuckets<5>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 6) copyBuckets<6>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 7) copyBuckets<7>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 8) copyBuckets<8>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);
    if (this->columns == 9) copyBuckets<9>(target, this->init, this->starts.data(), this->rowTargets,
                                           this->computeOffsets.data(), rows,
                                           static_cast<int>(this->starts.size()), threads);

#ifdef STATISTICS
    indexCopyToBucketsTime += timer.get();
    timer.reset();
#endif

    if (this->columns == 1) sortGroupsParallel<1>(threads);
    if (this->columns == 2) sortGroupsParallel<2>(threads);
    if (this->columns == 3) sortGroupsParallel<3>(threads);
    if (this->columns == 4) sortGroupsParallel<4>(threads);
    if (this->columns == 5) sortGroupsParallel<5>(threads);
    if (this->columns == 6) sortGroupsParallel<6>(threads);
    if (this->columns == 7) sortGroupsParallel<7>(threads);
    if (this->columns == 8) sortGroupsParallel<8>(threads);
    if (this->columns == 9) sortGroupsParallel<9>(threads);

#ifdef STATISTICS
    indexSortTime += timer.get();
#endif
}

template<int N>
void PrimaryIndex::sortGroupsParallel(int threads)
{
    auto groupCount = static_cast<int32_t>(this->starts.size());
#pragma omp parallel for num_threads(threads) schedule(dynamic, 4)
    for (int g = 0; g < groupCount; g++)
    {
        auto start = this->starts[g];
        auto end = start + this->counts[g];

        bool unique = true;
        if (start < end)
        {
            auto from = this->move(this->data, static_cast<int>(start));
            auto to = this->move(this->data, static_cast<int>(end));

            this->sort<N>(from, to, this->column, this->minValue, this->maxValue);

            uint64_t lastValue = this->move(this->data, start)->row[this->column];
            for (int i = start + 1; i < static_cast<int32_t>(end); i++)
            {
                auto value = this->move(this->data, i)->row[this->column];
                if (value == lastValue)
                {
                    unique = false;
                    break;
                }
                else lastValue = value;
            }
        }
        this->unique[g] = static_cast<unsigned int>(unique);
    }
}
