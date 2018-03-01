#pragma once

#include <cstdint>
#include <cstddef>
#include "view.h"
#include "../util.h"

class ColumnRelation: public View
{
public:
    ColumnRelation()
    {

    }
    DISABLE_COPY(ColumnRelation);
    ENABLE_MOVE(ColumnRelation);

    uint64_t tupleCount;
    uint64_t columnCount;
    uint64_t* data;
    uint64_t id;

    uint64_t getValue(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }
    uint64_t& getValueMutable(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }

    virtual int64_t getColumnCount() override
    {
        return this->columnCount;
    }

    virtual int64_t getRowCount() override
    {
        return this->tupleCount;
    }

    virtual uint64_t getValue(const Selection& selection, int row) override
    {
        return this->getValue(row, selection.column);
    }
};
