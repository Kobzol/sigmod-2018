#pragma once

#include <cstdint>
#include <vector>

#include <iostream>
#include <cassert>
#include <algorithm>

#include "../query.h"

using FilterFn = bool(*)(uint64_t);

class FilterCompiler
{
public:
    FilterFn compile(std::vector<Filter> filters);
};
