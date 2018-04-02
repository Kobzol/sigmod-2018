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

void PrimaryIndex::prepare()
{
    if (!canBuild(this->relation))
    {
        return;
    }

    this->initGroups();
    auto rows = static_cast<int>(this->relation.getRowCount());

    auto* src = reinterpret_cast<PrimaryRowEntry*>(this->init);
    const int BUCKET_SPLIT_COUNT = 20;
    auto chunk = static_cast<int>(std::ceil(rows / (double) BUCKET_SPLIT_COUNT));

    for (int b = 0; b < BUCKET_SPLIT_COUNT; b++)
    {
        int from = b * chunk;
        int to = std::min(rows, from + chunk);

        this->bucketJobs.emplace_back([this, src, from, to]() {
            for (int i = from; i < to; i++)
            {
                auto row = this->groups[this->rowTargets[i].first].start + this->rowTargets[i].second;
                auto* item = this->move(this->data, static_cast<int>(row));
                std::memcpy(item, this->move(src, i), static_cast<size_t>(this->rowSizeBytes));
            }
        });
    }

    int columns = this->relation.getColumnCount();
    for (int g = 0; g < static_cast<int32_t>(this->groups.size()); g++)
    {
        this->sortJobs.emplace_back([this, g, columns]() {
            auto start = this->groups[g].start;
            auto end = start + this->groups[g].count;

            auto from = this->move(this->data, static_cast<int>(start));
            auto to = this->move(this->data, static_cast<int>(end));

            if (columns == 1) sort<1>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 2) sort<2>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 3) sort<3>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 4) sort<4>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 5) sort<5>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 6) sort<6>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 7) sort<7>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 8) sort<8>(from, to, this->column, this->minValue, this->maxValue);
            if (columns == 9) sort<9>(from, to, this->column, this->minValue, this->maxValue);
        });
    }
}

void PrimaryIndex::finalize()
{
    if (!canBuild(this->relation))
    {
        return;
    }

    auto rows = static_cast<int>(this->relation.getRowCount());
#ifdef USE_MULTILEVEL_INDEX
    this->groupCount = 128;
    this->indexGroups.resize(static_cast<size_t>(this->groupCount));

    int group = 0;
    this->indexGroups[0].startValue = this->minValue;
    this->indexGroups[0].start = this->begin;
#endif

    bool unique = true;
    uint64_t lastValue = this->begin->row[this->column];
    for (int i = 1; i < rows; i++)
    {
        uint64_t value = this->move(this->begin, i)->row[this->column];

#ifdef USE_MULTILEVEL_INDEX
        if (this->groupValue(value) > group && lastValue != value)
        {
            this->indexGroups[group].endValue = value;
            this->indexGroups[group].end = this->move(this->begin, i);
            this->indexGroups[group + 1].startValue = value;
            this->indexGroups[group + 1].start = this->groups[group].end;
            group++;
        }
#endif

        if (lastValue == value)
        {
            unique = false;
#ifndef USE_MULTILEVEL_INDEX
            break;
#endif
        }
        lastValue = value;
    }

#ifdef USE_MULTILEVEL_INDEX
    this->indexGroups[group].endValue = this->maxValue + 1;
    this->indexGroups[group].end = this->end;
    this->indexGroups.resize(group + 1);
#endif

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

bool PrimaryIndex::build(uint32_t threads)
{
    if (!canBuild(this->relation))
    {
        return false;
    }

    this->initGroups();

    auto rows = static_cast<int>(this->relation.getRowCount());
    auto* src = reinterpret_cast<PrimaryRowEntry*>(this->init);

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < rows; i++)
    {
        auto row = this->groups[this->rowTargets[i].first].start + this->rowTargets[i].second;
        auto* item = this->move(this->data, static_cast<int>(row));
        std::memcpy(item, this->move(src, i), static_cast<size_t>(this->rowSizeBytes));
    }

    int columns = this->relation.getColumnCount();

#pragma omp parallel for num_threads(threads)
    for (int g = 0; g < static_cast<int32_t>(this->groups.size()); g++)
    {
        auto start = this->groups[g].start;
        auto end = start + this->groups[g].count;

        auto from = this->move(this->data, static_cast<int>(start));
        auto to = this->move(this->data, static_cast<int>(end));

        if (columns == 1) sort<1>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 2) sort<2>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 3) sort<3>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 4) sort<4>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 5) sort<5>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 6) sort<6>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 7) sort<7>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 8) sort<8>(from, to, this->column, this->minValue, this->maxValue);
        if (columns == 9) sort<9>(from, to, this->column, this->minValue, this->maxValue);
    }

    this->finalize();

    return false;
}

void PrimaryIndex::initGroups()
{
    assert(this->init != nullptr);

    auto rows = static_cast<int>(this->relation.getRowCount());

    this->rowSizeBytes = PrimaryIndex::rowSize(this->relation);
    this->rowOffset = this->rowSizeBytes / sizeof(PrimaryRowEntry);

    this->mem = new uint64_t[this->rowOffset * rows];
    this->data = reinterpret_cast<PrimaryRowEntry*>(this->mem);

    uint64_t minValue = std::numeric_limits<uint64_t>::max();
    uint64_t maxValue = 0;

#ifdef STATISTICS
    Timer timer;
#endif

    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        minValue = value < minValue ? value : minValue;
        maxValue = value > maxValue ? value : maxValue;
    }

    this->minValue = minValue;
    this->maxValue = maxValue;
    this->diff = (this->maxValue - this->minValue) + 1;

#ifdef STATISTICS
    indexMinMaxTime += timer.get();
#endif

    const int TARGET_SIZE = 1024 * 512;
    double size = (rows * this->rowSizeBytes) / static_cast<double>(TARGET_SIZE);
    const auto GROUP_COUNT = static_cast<int>(std::min((maxValue - minValue) + 1,
                                                       static_cast<uint64_t>(std::ceil(size))));
//    const int GROUP_COUNT = 128;
    auto diff = std::max(((maxValue - minValue) + 1) / GROUP_COUNT + 1, 1UL);
    auto shift = static_cast<uint64_t>(std::ceil(std::log2(diff)));

    this->groups.resize(static_cast<size_t>(GROUP_COUNT));
    this->rowTargets.resize(static_cast<size_t>(rows)); // group, index

    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        auto groupIndex = (value - minValue) >> shift;
        this->rowTargets[i].first = static_cast<uint32_t>(groupIndex);
        this->rowTargets[i].second = static_cast<uint32_t>(this->groups[groupIndex].count);
        this->groups[groupIndex].count++;
    }

    for (int i = 1; i < GROUP_COUNT; i++)
    {
        this->groups[i].start = this->groups[i - 1].start + this->groups[i - 1].count;
    }

    this->begin = this->data;
    this->end = this->move(this->data, rows);
}
