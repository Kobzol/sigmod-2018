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

    std::unique_ptr<Iterator> createIndexedIterator() final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashMap<uint64_t, std::vector<uint64_t>>& hashTable,
                       BloomFilter<BLOOM_FILTER_SIZE>& filter) override;

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

    void dumpPlan(std::stringstream& ss) final
    {
#ifdef STATISTICS
        ss << this->getJoinName() << "(";
        this->left->dumpPlan(ss);
        ss << ", ";
        this->right->dumpPlan(ss);
        ss << ", " << this->rowCount << ")";
#endif
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
