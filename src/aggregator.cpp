#include "aggregator.h"

// TODO: group by relations
std::string Aggregator::aggregate(const Selection& selection, View& view)
{
    int64_t sum = -1;
    view.reset();

    while (view.getNext())
    {
        sum += view.getValue(selection);
    }

    return sum == -1 ? "NULL" : std::to_string(sum + 1);
}
