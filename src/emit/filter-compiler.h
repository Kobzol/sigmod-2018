#pragma once

#include <cstdint>
#include <vector>

#include "../query.h"

using FilterFn = bool(*)(uint64_t);

class FilterCompiler
{
public:
    FilterFn compile(const Filter& filter);
};
