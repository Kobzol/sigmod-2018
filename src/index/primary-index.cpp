#include "primary-index.h"
#include "../relation/column-relation.h"
#include "../thirdparty/kxsort.h"
#include "../database.h"

#include <algorithm>
#include <cstring>

template <int N>
struct Row
{
public:
    std::array<uint64_t, N> row;
};

template <int N>
struct RadixTraitsRow
{
    static const int nBytes = 8;

    explicit RadixTraitsRow(int column): column(column)
    {

    }

    int kth_byte(const Row<N> &x, int k) {
        return (x.row[column] >> (k * 8)) & 0xFF;
    }
    bool compare(const Row<N> &x, const Row<N> &y) {
        return x.row[column] < y.row[column];
    }

    int column;
};

template <int N>
void sort(uint64_t* mem, int rows, uint32_t column)
{
    auto* ptr = reinterpret_cast<Row<N>*>(mem);
    kx::radix_sort(ptr, ptr + rows, RadixTraitsRow<N>(column));
}

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

bool PrimaryIndex::build()
{
    if (!canBuild(this->relation))
    {
        return false;
    }

    assert(this->init != nullptr);

    auto rows = static_cast<int>(this->relation.getRowCount());
    int columns = this->relation.getColumnCount();
    uint32_t column = this->column;

    this->rowSizeBytes = PrimaryIndex::rowSize(this->relation);
    this->rowOffset = this->rowSizeBytes / sizeof(PrimaryRowEntry);

    this->mem = new uint64_t[this->rowSizeBytes * rows];
    this->data = reinterpret_cast<PrimaryRowEntry*>(this->mem);
    std::memcpy(this->mem, this->init, rows * this->rowSizeBytes);

    if (columns == 1) sort<1>(this->mem, rows, column);
    if (columns == 2) sort<2>(this->mem, rows, column);
    if (columns == 3) sort<3>(this->mem, rows, column);
    if (columns == 4) sort<4>(this->mem, rows, column);
    if (columns == 5) sort<5>(this->mem, rows, column);
    if (columns == 6) sort<6>(this->mem, rows, column);
    if (columns == 7) sort<7>(this->mem, rows, column);
    if (columns == 8) sort<8>(this->mem, rows, column);
    if (columns == 9) sort<9>(this->mem, rows, column);

    this->minValue = this->data[0].row[column];
    this->maxValue = this->move(this->data, rows - 1)->row[column];
    this->diff = (this->maxValue - this->minValue) + 1;

    this->begin = this->data;
    this->end = this->move(data, rows);

#ifdef USE_MULTILEVEL_INDEX
    this->groupCount = 32;
    this->groups.resize(static_cast<size_t>(this->groupCount));

    int group = 0;
    this->groups[0].startValue = this->minValue;
    this->groups[0].start = this->begin;
#endif

    bool unique = true;
    for (int i = 1; i < rows; i++)
    {
        uint64_t lastValue = this->move(this->begin, i - 1)->row[this->column];
        uint64_t value = this->move(this->begin, i)->row[this->column];

#ifdef USE_MULTILEVEL_INDEX
        if (NOEXPECT(this->groupValue(value) > group))
        {
            this->groups[group].endValue = value;
            this->groups[group].end = this->move(this->begin, i);
            group++;
            this->groups[group].startValue = value;
            this->groups[group].start = this->groups[group - 1].end;
        }
#endif

        if (lastValue == value)
        {
            unique = false;
#ifndef USE_MULTILEVEL_INDEX
            break;
#endif
        }
    }

#ifdef USE_MULTILEVEL_INDEX
    this->groups[group].endValue = this->maxValue + 1;
    this->groups[group].end = this->end;

    assert(group == this->groupCount - 1);
#endif

    database.unique[database.getGlobalColumnId(this->relation.id, this->column)] = static_cast<unsigned int>(unique);

    this->buildCompleted = true;
    return true;
}

PrimaryRowEntry* PrimaryIndex::lowerBound(uint64_t value)
{
    if (this->columns == 1) return findLowerBound<1>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 2) return findLowerBound<2>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 3) return findLowerBound<3>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 4) return findLowerBound<4>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 5) return findLowerBound<5>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 6) return findLowerBound<6>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 7) return findLowerBound<7>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 8) return findLowerBound<8>(this->mem, this->relation.getRowCount(), value, this->column);
    if (this->columns == 9) return findLowerBound<9>(this->mem, this->relation.getRowCount(), value, this->column);
    
    assert(false);
    return nullptr;
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
#ifdef USE_MULTILEVEL_INDEX
    for (auto& group: this->groups)
    {
        if (group.startValue <= value && value < group.endValue)
        {
            auto* ptr = reinterpret_cast<Row<N>*>(group.start);
            auto* end = reinterpret_cast<Row<N>*>(group.end);
            auto iter = std::lower_bound(ptr, end, value, [column](const Row<N>& entry, uint64_t val) {
                return entry.row[column] < val;
            });
            if (iter == end) return this->end;
            return reinterpret_cast<PrimaryRowEntry*>(ptr + (iter - ptr));
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
