#pragma once

#include "joiner.h"

/**
 * Joins two iterators using merge sort join.
 * Requires both iterators to be sorted, so they have to either be sorted joins or SortFilterIterator.
 */
template <bool HAS_MULTIPLE_JOINS>
class MergeSortJoiner: public Joiner
{
public:
    MergeSortJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() final;
    bool getBlock(std::vector<uint64_t*>& cols, size_t& rows) final;

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) final;
    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;
    bool isSortedOn(const Selection& selection) final;
    void aggregateDirect(std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         size_t& count);

    std::string getJoinName() final
    {
        return "MS";
    }

    bool move(int index);

    bool checkJoins();
    bool findSameRow();

    uint64_t value[2];
    uint64_t iteratingRight;

    bool hasRight = false;
    bool hasItems = false;

    std::vector<uint64_t*> blockData[2];
    size_t blockRowCount[2] = { 0 };
    int blockRow[2] = { -1, -1 };
};

template class MergeSortJoiner<false>;
template class MergeSortJoiner<true>;
