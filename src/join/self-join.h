#pragma once

#include "../relation/wrapper-iterator.h"

/**
 * Iterator that filters rows based on self-join on a single relation.
 */
class SelfJoin: public WrapperIterator
{
public:
    SelfJoin(Iterator* inner, std::vector<Selection> selections);

    bool getNext() final;
    void dumpPlan(std::ostream& ss) final;

    std::vector<Selection> selections;
    int32_t selectionSize;
};
