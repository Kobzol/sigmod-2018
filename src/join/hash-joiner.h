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

private:
    void initialize();
    void fillHashTable();

    std::vector<uint32_t> activeRows;
    int activeRowIndex = -1;

    uint32_t leftIndex;
    uint32_t rightIndex;

    std::vector<uint64_t> row;

    bool initialized = false;
    std::unordered_map<uint64_t, std::vector<uint32_t>> hashTable;
    std::unordered_map<uint32_t, std::vector<uint64_t>> rowData;

    bool findRowByHash();

    bool checkRowPredicates();
};
