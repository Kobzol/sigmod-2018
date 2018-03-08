#pragma once

#include <memory>
#include <unordered_map>
#include "../relation/column-relation.h"
#include "joiner.h"
#include "../relation/iterator.h"

class HashJoiner: public Joiner
{
public:
    // leftIndex - index of join selection that is on the left side
    HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() override;
    uint64_t getValue(const Selection& selection) override;

    uint64_t getColumn(uint32_t column) override;

    bool getValueMaybe(const Selection& selection, uint64_t& value) override;
    bool hasSelection(const Selection& selection) override;

    void requireSelections(std::unordered_map<SelectionId, Selection>& selections) override;
    void prepareColumnMappings(const std::unordered_map<SelectionId, Selection>& selections,
                               std::vector<Selection>& leftSelections);

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) override;
    void sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& selections) override;

private:
    bool findRowByHash();
    bool checkRowPredicates();

    uint64_t* getCurrentRow()
    {
        return &(*this->activeRow)[this->activeRowIndex * this->columnMapCols];
    }

    std::vector<uint64_t>* activeRow = nullptr;
    int32_t activeRowCount = 0;
    int activeRowIndex = -1;

    uint32_t leftIndex;
    uint32_t rightIndex;

    int joinSize;
    std::unordered_map<uint64_t, std::vector<uint64_t>> hashTable;
};
