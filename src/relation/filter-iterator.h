#pragma once

#include "../query.h"
#include "../util.h"
#include "column-relation.h"

class FilterIterator: public ColumnRelationIterator
{
public:
    explicit FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters);

    bool getNext() override;
    uint32_t getFilterReduction() override
    {
        uint32_t filterReduction = 0;

        for (auto& f: this->filters)
        {
            if (f.oper == '=') filterReduction += 5;
            else filterReduction += 1;
        }

        return filterReduction;
    }

    std::vector<Filter> filters;

    bool passesFilters();
};
