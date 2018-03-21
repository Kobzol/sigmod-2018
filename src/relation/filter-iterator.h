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

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container) override;

    int64_t predictSize() override;

    void dumpPlan(std::ostream& ss) final
    {
        ss << this->getFilterName() << "(" << this->rowCount << ")";
    }

    virtual std::string getFilterName()
    {
        return "FI";
    }

    virtual bool passesFilters();

	void printPlan(unsigned int level);


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
