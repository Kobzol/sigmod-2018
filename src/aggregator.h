#pragma once

#include <string>

#include "relation/view.h"
#include "query.h"

class Aggregator
{
public:
    std::string aggregate(const Selection& selection, View& view);
};
