#pragma once

#include <cstdint>
#include "../query.h"

class Iterator
{
public:
    virtual ~Iterator() = default;

    virtual bool getNext() = 0;
    virtual void reset()
    {
        this->rowIndex = -1;
    }

    virtual uint64_t getValue(const Selection& selection) = 0;
    virtual uint64_t getColumn(uint32_t column) = 0;

    int rowIndex = -1;
};
