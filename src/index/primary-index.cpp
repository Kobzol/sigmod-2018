#include "primary-index.h"
#include <algorithm>

PrimaryIndex::PrimaryIndex(ColumnRelation& relation, uint32_t column, std::vector<PrimaryRowEntry> data)
    : Index(relation, column), data(std::move(data))
{

}

void PrimaryIndex::build()
{
    if (this->buildStarted.test_and_set()) return;

    std::sort(this->data.begin(), this->data.end(), [this](const PrimaryRowEntry& lhs, const PrimaryRowEntry& rhs) {
        return lhs.row[this->column] < rhs.row[this->column];
    });

    this->buildCompleted = true;
}
