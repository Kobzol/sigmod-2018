#pragma once

#include <memory>
#include "../relation/column-relation.h"

class HashJoiner
{
public:
    std::unique_ptr<View> join(View& r1, View& r2, const std::vector<Join>& joins);
};
