#include <iostream>
#include "query.h"
#include "database.h"

bool Filter::isSkippable() const
{
#ifdef USE_SORT_INDEX
    auto index = database.getSortIndex(this->selection.relation, this->selection.column);
    if (index != nullptr)
    {
        uint64_t minValue = index->minValue;
        uint64_t maxValue = index->maxValue;

        if (this->oper == '=' && (maxValue < this->value || minValue > this->value))
        {
            return true;
        }
        else if (this->oper == '>' && maxValue <= this->value)
        {
            return true;
        }
        else if (this->oper == '<' && minValue >= this->value)
        {
            return true;
        }
    }
#endif

    return false;
}

bool Query::isAggregable() const
{
#ifndef AGGREGATE_PUSH
    return false;
#endif

    if (this->aggregable) return true;

    std::unordered_map<uint32_t, uint32_t> columnMap;
    for (auto& join : this->joins)
    {
        for (auto& predicate : join)
        {
            for (auto& selection : predicate.selections)
            {
                if (columnMap.find(selection.binding) == columnMap.end())
                {
                    columnMap[selection.binding] = selection.column;
                }

                if (columnMap[selection.binding] != selection.column)
                {
                    return false;
                }

                if (database.getAggregateIndex(selection.relation, selection.column) == nullptr)
                {
                    return false;
                }
            }
        }
    }

    this->aggregable = true;
    return true;
}

bool Query::isInJoin(const Selection& selection) const
{
    return false;
}
