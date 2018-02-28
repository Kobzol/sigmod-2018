#pragma once

#include <cstddef>
#include <unordered_map>
#include <cassert>
#include "view.h"
#include "../util.h"

class RowRelation: public View
{
public:
    explicit RowRelation(size_t columnCount): columnCount(columnCount)
    {
        this->data.resize(1000);
    }
    DISABLE_COPY(RowRelation);

    uint64_t* addRow()
    {
        size_t count = this->rowCount * this->columnCount;
        if (count >= this->data.size())
        {
            this->data.resize(std::max(this->columnCount, this->data.size() * 2));
        }

        auto data = &this->data[this->rowCount * this->columnCount];
        this->rowCount++;
        return data;
    }
    void setColumn(const Selection& selection, uint32_t index)
    {
        this->columnMap[selection.getId()] = index;
    }

    bool getNext() override
    {
        return false;
    }
    int64_t getColumnCount() override
    {
        return this->columnCount;
    }
    int64_t getRowCount() override
    {
        return this->rowCount;
    }

    uint64_t getValue(const Selection& selection, int row) override
    {
        uint32_t column = this->columnMap[selection.getId()];
        return this->data[row * this->columnCount + column];
    }

private:
    size_t rowCount = 0;
    size_t columnCount = 0;
    std::vector<uint64_t> data;
    std::unordered_map<uint32_t, uint32_t> columnMap;
};
