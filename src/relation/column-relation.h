#pragma once

#include <cstdint>
#include <cstddef>
#include "view.h"

class ColumnRelation: public View
{
public:
    uint64_t tupleCount;
    uint64_t columnCount;
    uint64_t* data;
    uint64_t id;

    uint64_t getValue(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }

    virtual bool getNext() override
    {
        return false;
    }

    virtual uint64_t getColumnCount() override
    {
        return this->columnCount;
    }

    virtual uint64_t getRowCount() override
    {
        return this->tupleCount;
    }

    virtual uint64_t getValue(const Selection& selection, int row) override
    {
        return this->getValue(row, selection.column);
    }
};
