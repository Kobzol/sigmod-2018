#pragma once

#include "../query.h"
#include "../util.h"
#include "column-relation.h"

class FilterIterator: public ColumnRelationIterator
{
public:
    explicit FilterIterator(ColumnRelation* relation, uint32_t binding);

    bool getNext() override;

    std::vector<Filter> filters;

private:
    bool passesFilters();
};
