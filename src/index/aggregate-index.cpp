#include "aggregate-index.h"
#include "../relation/column-relation.h"

AggregateIndex::AggregateIndex(ColumnRelation& relation, uint32_t column, SortIndex& index)
        : Index(relation, column), index(index)
{

}

bool AggregateIndex::build()
{
    this->data.emplace_back(this->index.data[0].value);
    AggregateRow* row = &this->data.back();
    row->sums.resize(this->relation.getColumnCount());

    for (auto& record : this->index.data)
    {
        uint64_t value = record.value;
        if (value != row->value)
        {
            assert(row->count > 0);
            this->data.emplace_back(value);
            row = &this->data.back();
            row->sums.resize(this->relation.getColumnCount());
        }

        int index = 0;
        for (int c = 0; c < static_cast<int32_t>(this->relation.getColumnCount()); c++)
        {
            row->sums[index++] += this->relation.getValue(record.row, static_cast<size_t>(c));
        }

        row->count++;
    }

    this->buildCompleted = true;
    return true;
}
