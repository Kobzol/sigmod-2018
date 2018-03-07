#pragma once

#include <memory>
#include <unordered_map>
#include "../relation/column-relation.h"
#include "joiner.h"

class HashJoiner: public Joiner
{
public:
    // leftIndex - index of join selection that is on the left side
    HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join);

    void reset() override;
    bool getNext() override;
    uint64_t getValue(const Selection& selection) override;
    uint64_t getColumn(uint32_t column) override;

    void fillRow(uint64_t* row) override;
    void sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns) override;

private:
    void fillHashTable();

    bool findRowByHash();
    bool checkRowPredicates();

    uint64_t activeValue = 0;
    int32_t activeRowCount = 0;
    int activeRowIndex = -1;

    uint32_t leftIndex;
    uint32_t rightIndex;

    int joinSize;
    std::unordered_map<uint64_t, std::vector<std::vector<uint64_t>>> hashTable;
};
