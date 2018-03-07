#pragma once

#include <cassert>
#include <utility>
#include <unordered_map>
#include "../util.h"
#include "../relation/iterator.h"

class Joiner: public Iterator
{
public:
    Joiner(Iterator* left, Iterator* right, std::vector<Join> joins)
            : left(left), right(right),
              leftCols(left->getColumnCount()), rightCols(right->getColumnCount()),
              joins(std::move(joins))
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

    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return this->columnMapReverted[column];
    }

    void setColumnMappings();
    void setColumn(SelectionId selectionId, uint32_t column);

    Iterator* left;
    Iterator* right;

    int32_t leftCols;
    int32_t rightCols;

    std::vector<Join> joins;

    std::unordered_map<SelectionId, uint32_t> columnMap;
    std::unordered_map<uint32_t, SelectionId> columnMapReverted;
};
