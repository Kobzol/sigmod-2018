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

    auto* ptr = this->relation.data + (rows * this->column);
    for (int i = 0; i < rows; i++)
    {
        auto value = *ptr++;
        minValue = value < minValue ? value : minValue;
        maxValue = value > maxValue ? value : maxValue;
    }

    const int TARGET_SIZE = 1024 * 512;
    double size = (rows * sizeof(RowEntry)) / static_cast<double>(TARGET_SIZE);
    const auto GROUP_COUNT = static_cast<int>(std::min((maxValue - minValue) + 1,
                                                       static_cast<uint64_t>(std::ceil(size))));
    auto diff = std::max(((maxValue - minValue) + 1) / GROUP_COUNT + 1, 1UL);
    auto shift = static_cast<uint64_t>(std::ceil(std::log2(diff)));

    std::vector<SortGroup> groups(static_cast<size_t>(GROUP_COUNT));
    std::vector<std::pair<uint32_t, uint32_t>> rowTargets(static_cast<size_t>(rows)); // group, index

    ptr = this->relation.data + (rows * this->column);
    for (int i = 0; i < rows; i++)
    {
        auto groupIndex = (*ptr++ - minValue) >> shift;
        rowTargets[i].first = static_cast<uint32_t>(groupIndex);
        rowTargets[i].second = static_cast<uint32_t>(groups[groupIndex].count++);
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

    uint64_t difference = maxValue - minValue;
#pragma omp parallel for num_threads(threads) schedule(dynamic)
    for (int i = 0; i < GROUP_COUNT; i++)
    {
        auto start = groups[i].start;
        auto end = start + groups[i].count;
        kx::radix_sort(this->data.begin() + start, this->data.begin() + end, RadixTraitsRowEntry(), difference);
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
