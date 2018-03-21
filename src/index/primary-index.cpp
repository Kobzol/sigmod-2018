#include "primary-index.h"
#include "../relation/column-relation.h"

#include <algorithm>

bool PrimaryIndex::canBuild(ColumnRelation& relation)
{
    return relation.getColumnCount() <= PRIMARY_INDEX_COLUMNS;
}

PrimaryIndex::PrimaryIndex(ColumnRelation& relation, uint32_t column, std::vector<PrimaryRowEntry>* init)
    : Index(relation, column), init(init)
{

}

bool PrimaryIndex::build()
{
    if (!canBuild(this->relation))
    {
        return false;
    }

    assert(this->column == 0);
    assert(this->init != nullptr);

    this->data = *this->init;

    uint32_t column = this->column;
    std::sort(this->data.begin(), this->data.end(), [column](const PrimaryRowEntry& lhs, const PrimaryRowEntry& rhs) {
        return lhs.row[column] < rhs.row[column];
    });

    this->minValue = this->data[0].row[column];
    this->maxValue = this->data.back().row[column];

    this->buildCompleted = true;
    return true;
}
