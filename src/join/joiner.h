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
              join(join)
    {

    }

    int32_t getColumnCount() final
    {
        return this->left->getColumnCount() + this->right->getColumnCount();
    }

    void fillBindings(std::vector<uint32_t>& ids) final
    {
        this->left->fillBindings(ids);
        this->right->fillBindings(ids);
    }

    uint32_t getColumnForSelection(const Selection& selection) final
    {
        auto id = selection.getId();
        for (int i = 0; i < this->columnMapCols; i++)
        {
            if (this->columnMap[i] == id) return static_cast<uint32_t>(i);
        }

        return this->right->getColumnForSelection(selection) + this->columnMapCols;
    }

    void setColumn(SelectionId selectionId, uint32_t column);

    Iterator* left;
    Iterator* right;

    Join& join;

    int32_t columnMapCols = 0;
    std::vector<SelectionId> columnMap;
};
