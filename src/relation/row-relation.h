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
        if (count + this->columnCount >= this->data.size())
        {
            this->data.resize(std::max(this->columnCount, (this->data.size() * 2) + this->columnCount));
        }

        auto data = &this->data[this->rowCount * this->columnCount];
        this->rowCount++;
        return data;
    }
    void setColumn(uint32_t id, uint32_t column)
    {
        this->columnMap[id] = column;
        this->columnMapReverted[column] = id;
    }
    uint32_t getColumnId(uint32_t relation, uint32_t column) override
    {
        return this->columnMapReverted[column];
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
        return this->getValueAt(column, row);
    }
    uint64_t getValueAt(uint32_t column, int row) override
    {
        return this->data[row * this->columnCount + column];
    }

private:
    size_t rowCount = 0;
    size_t columnCount = 0;
    std::vector<uint64_t> data;
    std::unordered_map<uint32_t, uint32_t> columnMap;
    std::unordered_map<uint32_t, uint32_t> columnMapReverted;
};
