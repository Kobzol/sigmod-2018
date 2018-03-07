#pragma once

#include <cassert>
#include <utility>
#include <unordered_map>
#include "../relation/view.h"
#include "../util.h"

class Joiner: public View
{
public:
    Joiner(View* left, View* right, std::vector<Join> joins): left(left), right(right), joins(std::move(joins))
    {
        this->setColumnMappings();
    }
    DISABLE_COPY(Joiner);

    int64_t getColumnCount() override
    {
        return this->left->getColumnCount() + this->right->getColumnCount();
    }

    int64_t getRowCount() override
    {
        assert(false);
        return -1;
    }

    void fillRelationIds(std::vector<uint32_t>& ids) override
    {
        this->left->fillRelationIds(ids);
        this->right->fillRelationIds(ids);
    }

    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return this->columnMapReverted[column];
    }

    void setColumnMappings();
    void setColumn(SelectionId selectionId, uint32_t column);

    View* left;
    View* right;
    std::vector<Join> joins;

    std::unordered_map<SelectionId, uint32_t> columnMap;
    std::unordered_map<uint32_t, SelectionId> columnMapReverted;
};
