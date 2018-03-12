#pragma once

#include "joiner.h"

/**
 * Joins two iterators using merge sort join.
 * Requires both iterators to be sorted, so they have to either be sorted joins or SortFilterIterator.
 */
class MergeSortJoiner: public Joiner
{
public:
    MergeSortJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() final;

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;
    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    bool hasSelection(const Selection& selection) final;
    uint32_t getColumnForSelection(const Selection& selection) final;
    void requireSelections(std::unordered_map<SelectionId, Selection>& selections) final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                               HashMap<uint64_t, std::vector<uint64_t>>& hashTable) final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;

    bool moveLeft();
    bool moveRight();

    bool checkJoins();
    bool findSameRow();

    uint64_t leftValue;
    uint64_t rightValue;
    uint64_t iteratingRight;

    uint32_t leftColSize;

    uint32_t leftColumn;
    uint32_t rightColumn;

    bool hasRight = false;
    bool hasItems = false;
};
