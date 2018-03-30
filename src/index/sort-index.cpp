#include "sort-index.h"
#include "../relation/column-relation.h"
#include "../thirdparty/kxsort.h"

#include <algorithm>
#include <cmath>
#include <cstring>

struct RadixTraitsRowEntry
{
    static const int nBytes = 12;
    int kth_byte(const RowEntry &x, int k) {
        if (k >= 8) return (x.value >> ((k - 8) * 8)) & 0xFF;
        return (x.row >> (k * 8)) & 0xFF;
    }
    bool compare(const RowEntry &x, const RowEntry &y) {
        return x < y;
    }
};

struct Group
{
    std::atomic<size_t> count{0};
    size_t start = 0;
    std::atomic<size_t> index{0};
};

SortIndex::SortIndex(ColumnRelation& relation, uint32_t column)
        : Index(relation, column)
{

}

#define SORT_BUILD_THREADS 4

bool SortIndex::build()
{
    this->data.resize(static_cast<size_t>(this->relation.getRowCount()));
    auto rows = static_cast<int32_t>(this->data.size());

    uint64_t minValue = std::numeric_limits<uint64_t>::max();
    uint64_t maxValue = 0;

#pragma omp parallel for reduction(min:minValue) reduction(max:maxValue) num_threads(SORT_BUILD_THREADS)
    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        minValue = value < minValue ? value : minValue;
        maxValue = value > maxValue ? value : maxValue;
    }

    /*const int TARGET_SIZE = 1024 * 1024;
    double size = (rows * sizeof(RowEntry)) / static_cast<double>(TARGET_SIZE);
    const auto GROUP_COUNT = static_cast<int>(std::ceil(size));*/
    const int GROUP_COUNT = 64;
    auto diff = std::max(1UL, maxValue - minValue) + 1;
    std::vector<Group> groups(GROUP_COUNT);

#pragma omp parallel for num_threads(SORT_BUILD_THREADS)
    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        auto groupIndex = ((value - minValue) / (double) diff) * GROUP_COUNT;
        groups[groupIndex].count++;
    }

    for (int i = 1; i < GROUP_COUNT; i++)
    {
        groups[i].start = groups[i - 1].index + groups[i - 1].count;
        groups[i].index = groups[i].start;
    }

#pragma omp parallel for num_threads(SORT_BUILD_THREADS)
    for (int i = 0; i < rows; i++)
    {
        auto value = this->relation.getValue(static_cast<size_t>(i), this->column);
        auto groupIndex = ((value - minValue) / (double) diff) * GROUP_COUNT;
        auto& item = this->data[groups[groupIndex].index++];
        item.value = value;
        item.row = static_cast<uint64_t>(i);
    }

#pragma omp parallel for num_threads(SORT_BUILD_THREADS)
    for (int i = 0; i < GROUP_COUNT; i++)
    {
        auto start = groups[i].start;
        auto end = start + groups[i].count;
        kx::radix_sort(this->data.begin() + start, this->data.begin() + end, RadixTraitsRowEntry());
    }

    //kx::radix_sort(this->data.begin(), this->data.end(), RadixTraitsRowEntry());

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
