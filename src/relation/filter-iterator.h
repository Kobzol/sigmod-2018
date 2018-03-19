#pragma once

#include "../query.h"
#include "../util.h"
#include "column-relation.h"

/**
 * Iterator that filters rows according to a WHERE clause (list of Filters).
 */
class FilterIterator: public ColumnRelationIterator
{
public:
    explicit FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters);

    bool getNext() override;
    bool getBlock(std::vector<uint64_t*>& cols, size_t& rows) override;

    uint32_t getFilterReduction() final
    {
        uint32_t filterReduction = 0;

        for (auto& f: this->filters)
        {
            if (f.oper == '=') filterReduction += 5;
            else filterReduction += 1;
        }

        return filterReduction;
    }

    std::unique_ptr<Iterator> createIndexedIterator() override;

    int64_t predictSize() override;

    void dumpPlan(std::stringstream& ss) final
    {
        ss << this->getFilterName() << "(" << this->rowCount << ")";
    }

    virtual std::string getFilterName()
    {
        return "FI";
    }

    virtual bool passesFilters();

    std::vector<Filter> filters;
    int startFilterIndex = 0;
    int filterSize;

    size_t rowCount = 0;
};

inline bool passesFilter(const Filter& filter, uint64_t value)
{
    switch (filter.oper)
    {
        case '=': return value == filter.value;
        case '<': return value < filter.value;
        case '>': return value > filter.value;
        default: assert(false);
    }

    return false;
}
