#pragma once

#include <cassert>
#include <utility>
#include <unordered_map>
#include <algorithm>

#include "../util.h"
#include "../relation/iterator.h"

class Joiner: public Iterator
{
public:
    Joiner(Iterator* left, Iterator* right, Join& join)
            : left(left), right(right),
              join(join),
              leftCols(left->getColumnCount()), rightCols(right->getColumnCount()),
              totalCols(leftCols + rightCols),
              columnMap(static_cast<size_t>(totalCols))
    {
        this->setColumnMappings();
    }
    DISABLE_COPY(Joiner);

    int32_t getColumnCount() override
    {
        return this->left->getColumnCount() + this->right->getColumnCount();
    }

    void fillBindings(std::vector<uint32_t>& ids) override
    {
        this->left->fillBindings(ids);
        this->right->fillBindings(ids);
    }

    bool getValueMaybe(const Selection& selection, uint64_t& value) override
    {
        auto id = selection.getId();
        for (int i = 0; i < this->totalCols; i++)
        {
            if (this->columnMap[i] == id)
            {
                value = this->getColumn(i);
                return true;
            }
        }

        return false;
    }

    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return this->columnMap[column];
    }
    uint32_t getColumnForSelection(const Selection& selection) override
    {
        auto id = selection.getId();
        for (int i = 0; i < this->totalCols; i++)
        {
            if (this->columnMap[i] == id) return static_cast<uint32_t>(i);
        }
        assert(false);
        return 0;
    }

    void setColumnMappings();
    void setColumn(SelectionId selectionId, uint32_t column);

    Iterator* left;
    Iterator* right;

    Join& join;

    int32_t leftCols;
    int32_t rightCols;
    int32_t totalCols;

    std::vector<SelectionId> columnMap;
};
