#pragma once

#include <cstdint>
#include <iostream>
#include <memory>

#include "../query.h"
#include "iterator.h"

class View
{
public:
    virtual ~View() = default;

    virtual int64_t getColumnCount() = 0;
    virtual int64_t getRowCount() = 0;
    virtual uint64_t getValue(const Selection& selection, int row) = 0;

    virtual uint32_t getSelectionIdForColumn(uint32_t index) = 0;

    virtual std::unique_ptr<Iterator> createIterator() = 0;
    virtual void fillRelationIds(std::vector<uint32_t>& ids) = 0;

    void dump(uint32_t relation, uint32_t binding = 0)
    {
        for (int i = 0; i < this->getRowCount(); i++)
        {
            for (int j = 0; j < this->getColumnCount(); j++)
            {
                std::cout << this->getValue(Selection(relation, binding, static_cast<uint32_t>(j)), i) << " ";
            }
            std::cout << std::endl;
        }

        std::cout << std::endl;
    }
};
