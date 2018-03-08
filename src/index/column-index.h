#pragma once

#include <atomic>
#include "../util.h"

class ColumnRelation;

class ColumnIndex
{
public:
    ColumnIndex(ColumnRelation& relation, uint32_t column);

    void create();

    std::atomic_flag buildStarted;
    std::atomic<bool> buildCompleted { false };

    ColumnRelation& relation;
    uint32_t column;
};
