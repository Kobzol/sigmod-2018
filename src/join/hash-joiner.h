#pragma once

#include <memory>
#include <unordered_map>

#include "../relation/column-relation.h"
#include "joiner.h"
#include "../relation/iterator.h"
#include "../bloom-filter.h"

/**
 * Joins two iterators using a hash join.
 * @tparam HAS_MULTIPLE_JOINS true if there are multiple column pairs in the join.
 */
template <bool HAS_MULTIPLE_JOINS>
class HashJoiner: public Joiner
{
public:
    // leftIndex - index of join selection that is on the left side
    HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    bool getNext() final;

    int32_t getColumnCount() final
    {
        return this->columnMapCols + this->right->getColumnCount();
    }

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;

    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    void requireSelections(std::unordered_map<SelectionId, Selection>& selections) final;
    void prepareColumnMappings(const std::unordered_map<SelectionId, Selection>& selections,
                               std::vector<Selection>& leftSelections);

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashMap<uint64_t, std::vector<uint64_t>>& hashTable,
                       BloomFilter<BLOOM_FILTER_SIZE>& filter) final;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;

    HashMap<uint64_t, std::vector<uint64_t>>::iterator getFromMap(uint64_t value)
    {
#ifdef USE_BLOOM_FILTER
        if (!this->bloomFilter.has(value))
        {
            return this->hashTable.end();
        }
#endif
        return this->hashTable.find(value);
    }

    std::string getJoinName() final
    {
        return "HS";
    }

	void printPlan(unsigned int level) override;

private:
    bool findRowByHash();
    bool checkRowPredicates();

    const uint64_t* getCurrentRow()
    {
        return &(*this->activeRow)[this->activeRowIndex * this->columnMapCols];
    }

    void aggregateDirect(std::vector<uint64_t>& results,
                         const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                         const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                         size_t& count);

    const std::vector<uint64_t>* activeRow = nullptr;
    int32_t activeRowCount = 0;
    int activeRowIndex = -1;
    uint32_t rightColumn;

    HashMap<uint64_t, std::vector<uint64_t>> hashTable;
    std::vector<uint64_t> rightValues;

    Selection rightSelection;

    BloomFilter<BLOOM_FILTER_SIZE> bloomFilter;
};

template class HashJoiner<false>;
template class HashJoiner<true>;
