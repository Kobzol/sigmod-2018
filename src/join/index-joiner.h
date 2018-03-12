#pragma once

#include "joiner.h"

/**
 * Joins two iterators using NLJ on the left iterator and indexed access to the right iterator.
 */
class IndexJoiner: public Joiner
{
public:
    IndexJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() final;

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;

    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    bool hasSelection(const Selection& selection) final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                               HashMap<uint64_t, std::vector<uint64_t>>& hashTable) override;

    uint32_t getColumnForSelection(const Selection& selection) final;

    void requireSelections(std::unordered_map<SelectionId, Selection>& selections) final;

    uint32_t leftColSize;
};
