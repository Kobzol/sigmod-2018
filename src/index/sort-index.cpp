#include "sort-index.h"
#include "../relation/column-relation.h"
#include "../thirdparty/kxsort.h"
#include "../database.h"

#include <cstring>
#include <algorithm>

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

SortIndex::SortIndex(ColumnRelation& relation, uint32_t column)
        : Index(relation, column)
{

}

bool SortIndex::build()
{
    this->data.resize(static_cast<size_t>(relation.getRowCount()));
    auto rows = static_cast<int32_t>(this->data.size());
    for (int i = 0; i < rows; i++)
    {
        this->data[i].value = this->relation.getValue(static_cast<size_t>(i), this->column);
        this->data[i].row = static_cast<uint32_t>(i);
    }
    kx::radix_sort(this->data.begin(), this->data.end(), RadixTraitsRowEntry());

    this->minValue = this->data[0].value;
    this->maxValue = this->data.back().value;

    this->begin = this->data.data();
    this->end = this->data.data() + this->data.size();

    bool unique = true;
    for (int i = 1; i < rows; i++)
    {
        if (this->begin[i - 1].value == this->begin[i].value)
        {
            unique = false;
            break;
        }
    }

    database.unique[database.getGlobalColumnId(this->relation.id, this->column)] = unique;

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
