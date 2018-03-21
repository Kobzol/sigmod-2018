#include "primary-index.h"
#include "../relation/column-relation.h"

#include <algorithm>

bool PrimaryIndex::canBuild(ColumnRelation& relation)
{
    return relation.getColumnCount() <= PRIMARY_INDEX_COLUMNS;
}

PrimaryIndex::PrimaryIndex(ColumnRelation& relation, uint32_t column, std::vector<PrimaryRowEntry> data)
    : Index(relation, column), data(std::move(data))
{

}

void PrimaryIndex::build()
{
    if (!canBuild(this->relation))
    {
        return;
    }

    std::sort(this->data.begin(), this->data.end(), [this](const PrimaryRowEntry& lhs, const PrimaryRowEntry& rhs) {
        return lhs.row[this->column] < rhs.row[this->column];
    });

    this->buildCompleted = true;
}
