#pragma once

#include <memory>
#include <unordered_map>
#include "../relation/column-relation.h"
#include "joiner.h"

class HashJoiner: public Joiner
{
    class HashJoinerIterator: public Iterator
    {
    public:
        explicit HashJoinerIterator(HashJoiner& joiner);

        void reset() override;
        bool getNext() override;
        uint64_t getValue(const Selection& selection) override;
        uint64_t getColumn(uint32_t column) override;

    private:
        void initialize();
        void fillHashTable();

        std::vector<uint32_t> activeRows;
        int activeRowIndex = -1;

        HashJoiner& joiner;
        std::vector<uint64_t> row;
        View* r1;
        View* r2;
        bool initialized = false;
        std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> hashes;
        std::unordered_map<uint32_t, std::vector<uint64_t>> rowData;
        std::unique_ptr<Iterator> iterators[2];
    };

public:
    HashJoiner(View* left, View* right, const std::vector<Join>& joins): Joiner(left, right, joins)
    {

    }

    std::unique_ptr<Iterator> createIterator() override
    {
        return std::make_unique<HashJoinerIterator>(*this);
    }
};
