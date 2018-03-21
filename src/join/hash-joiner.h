#pragma once

#include <memory>
#include <unordered_map>
#include <atomic>

#include "../relation/column-relation.h"
#include "joiner.h"
#include "../relation/iterator.h"
#include "../bloom-filter.h"
#include "../hash-table.h"

/**
 * Joins two iterators using a hash join.
 * @tparam HAS_MULTIPLE_JOINS true if there are multiple column pairs in the join.
 */
template <bool HAS_MULTIPLE_JOINS>
class HashJoiner: public Joiner
{
public:
    // leftIndex - index of join selection that is on the left side
    HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join, bool last);

    bool getNext() final;

    int32_t getColumnCount() final
    {
        return this->columnMapCols + this->right->getColumnCount();
    }

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;

    uint32_t getColumnForSelection(const Selection& selection) override;
    void setColumn(SelectionId selectionId, uint32_t column);
    bool hasSelection(const Selection& selection) override;

    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) final;
    void prepareColumnMappings(const std::unordered_map<SelectionId, Selection>& selections,
                               std::vector<Selection>& leftSelections);

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashTable& hashTable) final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) final;

    std::vector<uint64_t>* getFromMap(uint64_t value)
    {
        return this->hashTable.getRow(value);
    }

    std::string getJoinName() final
    {
        return "HS";
    }

private:
    bool findRowByHash();
    bool checkRowPredicates();

    const uint64_t* getCurrentRow()
    {
        return &(*this->activeRow)[this->activeRowIndex * this->columnMapCols];
    }

    void workerAggregate(Iterator* iterator, std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         std::atomic<size_t>& count);

    void aggregateDirect(std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         size_t& count);

    const std::vector<uint64_t>* activeRow = nullptr;
    int32_t activeRowCount = 0;
    int activeRowIndex = -1;

    HashTable hashTable;
    std::vector<uint64_t> rightValues;

    std::vector<SelectionId> columnMap;
    int32_t columnMapCols = 0;
    bool last;
};

template class HashJoiner<false>;
template class HashJoiner<true>;
