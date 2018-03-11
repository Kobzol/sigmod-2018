#pragma once

#include <memory>
#include <unordered_map>

#include "../relation/column-relation.h"
#include "joiner.h"
#include "../relation/iterator.h"
#include "../hash/sparse_map.h"

template <bool HAS_MULTIPLE_JOINS>
class HashJoiner: public Joiner
{
public:
    // leftIndex - index of join selection that is on the left side
    HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() override;

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;

    bool getValueMaybe(const Selection& selection, uint64_t& value) final;
    bool hasSelection(const Selection& selection) final;

    void requireSelections(std::unordered_map<SelectionId, Selection>& selections) final;
    void prepareColumnMappings(const std::unordered_map<SelectionId, Selection>& selections,
                               std::vector<Selection>& leftSelections);

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) final;
    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashMap<uint64_t, std::vector<uint64_t>>& hashTable) final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;

private:
    bool findRowByHash();
    bool checkRowPredicates();

    const uint64_t* getCurrentRow()
    {
        return &(*this->activeRow)[this->activeRowIndex * this->columnMapCols];
    }

    const std::vector<uint64_t>* activeRow = nullptr;
    int32_t activeRowCount = 0;
    int activeRowIndex = -1;

    uint32_t leftIndex;
    uint32_t rightIndex;

    int joinSize;
    HashMap<uint64_t, std::vector<uint64_t>> hashTable;

    std::vector<uint64_t> rightValues;
};

template class HashJoiner<false>;
template class HashJoiner<true>;
