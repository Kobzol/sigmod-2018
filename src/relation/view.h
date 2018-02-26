#pragma once

#include <cstdint>
#include "../query.h"

class View
{
public:
    void reset()
    {
        this->rowIndex = -1;
    }
    virtual bool getNext() = 0;

    virtual uint64_t getColumnCount() = 0;
    virtual uint64_t getRowCount() = 0;
    virtual uint64_t getValue(const Selection& selection, int row = -1) = 0;

    int rowIndex = -1;
};
