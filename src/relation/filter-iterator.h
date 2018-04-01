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
    FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters);
    FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters, int start, int end);

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

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                         const std::vector<Selection>& selections, size_t& count) override;

    int64_t predictSize() override;

    virtual std::unique_ptr<ColumnRelationIterator> createForRange(int start, int end) final
    {
        return std::make_unique<FilterIterator>(this->relation, this->binding, this->filters, start, end);
    }

    void dumpPlan(std::ostream& ss) final
    {
        ss << this->getFilterName() << "(" << this->rowCount << ")";
    }

    virtual std::string getFilterName()
    {
        return "FI";
    }

    virtual bool passesFilters();
    bool passesFiltersTransposed();

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
        case 'r': return value > filter.value && value < filter.valueMax;
        default: assert(false);
    }

    return false;
}
