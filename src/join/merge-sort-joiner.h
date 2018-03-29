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

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) final;
    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) final;
    bool isSortedOn(const Selection& selection) final;
    void aggregateDirect(std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         size_t& count);

    void split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count) final;
    void splitToBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                               std::vector<uint64_t>& bounds, size_t count) final;

    std::string getJoinName() final
    {
        return "MS";
    }

    bool moveLeft();
    bool moveRight();

    bool skipLeftToRight();
    bool skipRightToLeft();

    bool checkJoins();
    bool findSameRow();

    uint64_t leftValue;
    uint64_t rightValue;
    uint64_t iteratingRight;

    bool hasRight = false;
    bool hasItems = false;
};

template class MergeSortJoiner<false>;
template class MergeSortJoiner<true>;
