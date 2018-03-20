#include "query.h"
#include "database.h"

bool Filter::isSkippable() const
{
#ifdef USE_SORT_INDEX
    auto index = database.getSortIndex(this->selection.relation, this->selection.column);
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
#endif

    return false;
}
