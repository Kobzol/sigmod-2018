#include "query.h"
#include "database.h"

bool Filter::isSkippable() const
{
    uint64_t minValue = database.getMinValue(this->selection.relation, this->selection.column);
    uint64_t maxValue = database.getMaxValue(this->selection.relation, this->selection.column);

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

    return false;
}

size_t Filter::getInterval() const
{
    if (this->oper == '=') return 0;
    return 1;

    uint64_t minValue = database.getMinValue(this->selection.relation, this->selection.column);
    uint64_t maxValue = database.getMaxValue(this->selection.relation, this->selection.column);

    if (this->oper == '<')
    {
        if (minValue >= this->value) return 0;
        return this->value - minValue;
    }
    else
    {
        if (maxValue <= this->value) return 0;
        return maxValue - this->value;
    }
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

#ifdef USE_AGGREGATE_INDEX
                if (database.getAggregateIndex(selection.relation, selection.column) == nullptr)
                {
                    return false;
                }
#else
                if (!database.hasIndexedIterator(selection))
                {
                    return false;
                }
#endif
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
