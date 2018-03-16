#pragma once

#include "joiner.h"

/**
 * Joins two iterators using NLJ on the left iterator and indexed access to the right iterator.
 */
template <bool HAS_MULTIPLE_JOINS>
class IndexJoiner: public Joiner
{
public:
    IndexJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;
    void requireSelections(std::unordered_map<SelectionId, Selection> selections) final;
    void aggregateDirect(std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         size_t& count);


    std::string getJoinName() final
    {
        return "NL";
    }
};

template class IndexJoiner<false>;
template class IndexJoiner<true>;
