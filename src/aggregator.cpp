#include "aggregator.h"

// TODO: group by relations
std::string Aggregator::aggregate(const Selection& selection, View& view)
{
    int64_t sum = 0;
    view.reset();
    if (!view.getNext()) return "NULL";

    do
    {
        sum += view.getValue(selection, view.rowIndex);
    }
    while (view.getNext());

    return std::to_string(sum);
}
