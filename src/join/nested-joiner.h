#pragma once

#include "joiner.h"

/*class NestedJoiner: public Joiner
{
    class NestedJoinIterator: public Iterator
    {
    public:
        explicit NestedJoinIterator(NestedJoiner& joiner)
                : joiner(joiner),
                  row(static_cast<size_t>(joiner.left->getColumnCount() + joiner.right->getColumnCount())),
                  r1(joiner.left),
                  r2(joiner.right)
        {

        }

        bool getNext() override;
        uint64_t getValue(const Selection& selection) override;
        uint64_t getColumn(uint32_t column) override;

        bool initialized = false;

        bool rightExhausted = true;
        std::unique_ptr<Iterator> iterators[2];
        NestedJoiner& joiner;
        std::vector<uint64_t> row;
        View* r1;
        View* r2;

        void initialize();
    };

public:
    NestedJoiner(View* left, View* right, const std::vector<Join>& joins);

    std::unique_ptr<Iterator> createIterator() override
    {
        return std::make_unique<NestedJoinIterator>(*this);
    }
};
*/