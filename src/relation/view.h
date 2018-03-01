#pragma once

#include <cstdint>
#include <iostream>
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
    virtual uint64_t getValueAt(uint32_t column, int row)
    {
        return this->getValue(Selection(0, column), row);
    }
    virtual uint32_t getColumnId(uint32_t relation, uint32_t index)
    {
        return Selection::getId(relation, index);
    }

    void dump(uint32_t relation)
    {
        for (int i = 0; i < this->getRowCount(); i++)
        {
            for (int j = 0; j < this->getColumnCount(); j++)
            {
                std::cout << this->getValue(Selection(relation, static_cast<uint32_t>(j)), i) << " ";
            }
            std::cout << std::endl;
        }

        std::cout << std::endl;
    }

    int64_t rowIndex = -1;
};
