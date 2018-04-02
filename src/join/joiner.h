#pragma once

#include <cassert>
#include <utility>
#include <unordered_map>
#include <algorithm>

#include "../util.h"
#include "../relation/iterator.h"
#include "../relation/column-relation.h"
#include "../hash-table.h"

class Joiner: public Iterator
{
public:
    Joiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
            : left(left), right(right),
              join(join), leftIndex(leftIndex),
              rightIndex(1 - leftIndex),
              leftSelection(this->join[0].selections[this->leftIndex]),
              rightSelection(this->join[0].selections[this->rightIndex]),
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

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashTable& hashTable) override;

    void fillBindings(std::vector<uint32_t>& bindings) final
    {
        this->left->fillBindings(bindings);
        this->right->fillBindings(bindings);
    }

    void initializeSelections(std::unordered_map<SelectionId, Selection>& selections);

    uint64_t getValue(const Selection& selection) override;
    uint64_t getColumn(uint32_t column) override;
    uint32_t getColumnForSelection(const Selection& selection) override;

    bool getValueMaybe(const Selection& selection, uint64_t& value) override;
    bool hasSelection(const Selection& selection) override;

    int64_t predictSize() override;

    void assignJoinSize(Database& database) final;

    bool hasBinding(uint32_t binding) final;

    bool isImpossible() final
    {
        return this->left->isImpossible() || this->right->isImpossible();
    }

    bool hasIndexJoin() override
    {
        return this->left->hasIndexJoin() || this->right->hasIndexJoin();
    }

    void dumpPlan(std::ostream& ss) final
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

    uint32_t leftColSize;

    Selection leftSelection;
    Selection rightSelection;

    std::vector<uint32_t> leftColumns;
    std::vector<uint32_t> rightColumns;

    int32_t joinSize;

    size_t rowCount = 0;
};
