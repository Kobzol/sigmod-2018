#include "sort-index.h"
#include "../relation/column-relation.h"
#include "../thirdparty/kxsort.h"

#include <algorithm>
#include <cmath>
#include <cstring>

struct RadixTraitsRowEntry
{
    static const int nBytes = 8;
    int kth_byte(const RowEntry &x, int k) {
        return (x.value >> (k * 8)) & 0xFF;
    }
    bool compare(const RowEntry &x, const RowEntry &y) {
        return x < y;
    }
};

struct SortGroup
{
    size_t count = 0;
    size_t start = 0;
};

SortIndex::SortIndex(ColumnRelation& relation, uint32_t column)
        : Index(relation, column)
{

}

bool SortIndex::build(uint32_t threads)
{
    this->data.resize(static_cast<size_t>(this->relation.getRowCount()));
    auto rows = static_cast<int32_t>(this->data.size());

    uint64_t minValue = std::numeric_limits<uint64_t>::max();
    uint64_t maxValue = 0;

    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        minValue = value < minValue ? value : minValue;
        maxValue = value > maxValue ? value : maxValue;
        this->data[i].value = value;
        this->data[i].row = i;
    }

    auto diff = std::max(1UL, maxValue - minValue) + 1;

    const int TARGET_SIZE = 1024 * 1024;
    double size = (rows * sizeof(RowEntry)) / static_cast<double>(TARGET_SIZE);
    const auto GROUP_COUNT = static_cast<int>(std::ceil(size));
//    const int GROUP_COUNT = 64;

    std::vector<SortGroup> groups(GROUP_COUNT);
    std::vector<std::pair<uint32_t, uint32_t>> rowTargets(static_cast<size_t>(rows)); // group, index

    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        auto groupIndex = ((value - minValue) / (double) diff) * GROUP_COUNT;
        rowTargets[i].first = static_cast<uint32_t>(groupIndex);
        rowTargets[i].second = static_cast<uint32_t>(groups[groupIndex].count);
        groups[groupIndex].count++;
    }

    for (int i = 1; i < GROUP_COUNT; i++)
    {
        groups[i].start = groups[i - 1].start + groups[i - 1].count;
    }

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < rows; i++)
    {
        auto row = groups[rowTargets[i].first].start + rowTargets[i].second;
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        auto& item = this->data[row];
        item.value = value;
        item.row = static_cast<uint64_t>(i);
    }

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < GROUP_COUNT; i++)
    {
        auto start = groups[i].start;
        auto end = start + groups[i].count;
        kx::radix_sort(this->data.begin() + start, this->data.begin() + end, RadixTraitsRowEntry(), diff);
    }

    this->minValue = this->data[0].value;
    this->maxValue = this->data.back().value;

    this->begin = this->data.data();
    this->end = this->data.data() + this->data.size();

    this->buildCompleted = true;
    return true;
}

RowEntry* SortIndex::lowerBound(uint64_t value)
{
    return this->toPtr(std::lower_bound(this->data.begin(), this->data.end(), value,
                                        [](const RowEntry& entry, uint64_t val) {
                                            return entry.value < val;
                                        }));
}

RowEntry* SortIndex::upperBound(uint64_t value)
{
    return this->toPtr(std::upper_bound(this->data.begin(), this->data.end(), value,
                                        [](uint64_t val, const RowEntry& entry) {
                                            return val < entry.value;
                                        }));
}

int64_t SortIndex::count(RowEntry* from, RowEntry* to)
{
    return to - from;
}
