#pragma once

#include <cstdint>
#include "../query.h"

class View
{
public:
    virtual ~View() = default;

    void reset()
    {
        this->rowIndex = -1;
    }
    virtual bool getNext()
    {
        if (this->rowIndex < this->getRowCount())
        {
            this->rowIndex++;
            return this->rowIndex < this->getRowCount();
        }

        return false;
    }

    virtual int64_t getColumnCount() = 0;
    virtual int64_t getRowCount() = 0;
    virtual uint64_t getValue(const Selection& selection, int row) = 0;

    int64_t rowIndex = -1;
};
