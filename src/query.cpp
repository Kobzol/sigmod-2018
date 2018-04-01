#include <unordered_set>
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
    else if (this->oper == 'r')
    {
        return maxValue <= this->value || minValue >= this->valueMax;
    }

    return false;
}

size_t Filter::getInterval() const
{
    if (this->oper == '=') return 1;

    uint64_t minValue = database.getMinValue(this->selection.relation, this->selection.column);
    uint64_t maxValue = database.getMaxValue(this->selection.relation, this->selection.column);

    if (this->oper == '<')
    {
        if (minValue >= this->value) return 0;
        return this->value - minValue;
    }
    else if (this->oper == '>')
    {
        if (maxValue <= this->value) return 0;
        return maxValue - this->value;
    }
    else return this->valueMax - this->value;
}

bool Query::isAggregable() const
{
#ifndef AGGREGATE_PUSH
    return false;
#endif

    if (this->aggregable) return true;
    if (this->joins.empty()) return false;

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
                if (database.isUnique(selection)) return false;
            }
        }
    }

    this->aggregable = true;
    return true;
}

void Query::fillBindings(std::vector<uint32_t>& bindings)
{
    std::unordered_set<uint32_t> bindingMap;
    for (auto& join: this->joins)
    {
        for (auto& pred: join)
        {
            for (auto& sel: pred.selections)
            {
                bindingMap.insert(sel.binding);
            }
        }
    }

    for (auto& filter: this->filters)
    {
        bindingMap.insert(filter.selection.binding);
    }

    for (auto& sel: this->selections)
    {
        bindingMap.insert(sel.binding);
    }

    for (auto& kv: bindingMap)
    {
        bindings.push_back(kv);
    }
}

void Query::dump(std::ostream& os)
{
    for (auto& join: this->joins)
    {
        for (auto& pred: join)
        {
            for (int i = 0; i < 2; i++)
            {
                os << pred.selections[i].binding << "." << pred.selections[i].column;
                if (i == 0) os << "=";
            }
            os << "&";
        }
    }

    os << "|";
    for (auto& filter: this->filters)
    {
        os << filter.selection.binding << "." << filter.selection.column << filter.oper << filter.value << " ";
    }

    os << "|";
    for (auto& sel: this->selections)
    {
        os << sel.binding << "." << sel.column << " ";
    }
}

bool Selection::isUnique() const
{
    return database.isUnique(*this);
}
