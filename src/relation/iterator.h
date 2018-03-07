#pragma once

#include <cstdint>
#include "../query.h"
#include "../util.h"

class Iterator
{
public:
    Iterator() = default;
    virtual ~Iterator() = default;
    DISABLE_COPY(Iterator);

    virtual bool getNext() = 0;
    virtual void reset()
    {
        this->rowIndex = -1;
    }

    virtual int32_t getColumnCount() = 0;
    virtual uint64_t getValue(const Selection& selection) = 0;
    virtual bool getValueMaybe(const Selection& selection, uint64_t& value) = 0;
    virtual uint64_t getColumn(uint32_t column) = 0;
    virtual void fillRow(uint64_t* row) = 0;
    virtual void sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns) = 0;

    virtual uint32_t getColumnForSelection(const Selection& selection) = 0;
    virtual SelectionId getSelectionIdForColumn(uint32_t column) = 0;

    virtual void fillBindings(std::vector<uint32_t>& ids) = 0;

    int rowIndex = -1;
};
