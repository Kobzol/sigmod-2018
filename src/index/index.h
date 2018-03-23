#pragma once

#include <atomic>
#include <limits>

#include "../util.h"

class ColumnRelation;

class Index
{
public:
    Index(ColumnRelation& relation, uint32_t column);
    DISABLE_COPY(Index);

    virtual ~Index() = default;

    virtual bool build() = 0;

    bool take()
    {
        return !this->buildStarted.test_and_set();
    }

    std::atomic_flag buildStarted = ATOMIC_FLAG_INIT;
    std::atomic<bool> buildCompleted { false };

    ColumnRelation& relation;
    uint32_t column;

    uint64_t minValue = (std::numeric_limits<uint64_t>::max)();
    uint64_t maxValue = 0;
    int columns;
};
