#pragma once

#include <vector>

#include "relation/column-relation.h"

class Database
{
public:
    std::vector<ColumnRelation> relations;
};
