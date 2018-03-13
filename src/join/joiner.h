#pragma once

#include <cassert>
#include <utility>
#include <unordered_map>
#include <algorithm>

#include "../util.h"
#include "../relation/iterator.h"
#include "../relation/column-relation.h"

class Joiner: public Iterator
{
public:
    Joiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
            : left(left), right(right),
              join(join), leftIndex(leftIndex),
              rightIndex(1 - leftIndex),
              joinSize(static_cast<int32_t>(this->join.size()))
    {

    }

    int32_t getColumnCount() override
    {
        return this->left->getColumnCount() + this->right->getColumnCount();
    }

    bool isJoin() final
    {
        return true;
    }

    uint32_t getColumnForSelection(const Selection& selection) override
    {
        auto id = selection.getId();
        for (int i = 0; i < this->columnMapCols; i++)
        {
            if (this->columnMap[i] == id) return static_cast<uint32_t>(i);
        }

        return this->right->getColumnForSelection(selection) + this->columnMapCols;
    }

    void setColumn(SelectionId selectionId, uint32_t column);

    bool hasSelection(const Selection& selection) override;

    std::unique_ptr<Iterator> createIndexedIterator() final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashMap<uint64_t, std::vector<uint64_t>>& hashTable,
                       BloomFilter<BLOOM_FILTER_SIZE>& filter) override;

    void dumpPlan(std::stringstream& ss) final
    {
        ss << this->getJoinName() << "(";
        this->left->dumpPlan(ss);
        ss << ", ";
        this->right->dumpPlan(ss);
        ss << ", " << this->rowCount << ")";
    }

    virtual std::string getJoinName() = 0;

    Iterator* left;
    Iterator* right;

    Join& join;

    uint32_t leftIndex;
    uint32_t rightIndex;

    std::vector<SelectionId> columnMap;
    int32_t columnMapCols = 0;
    int32_t joinSize;
};
