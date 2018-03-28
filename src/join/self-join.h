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

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override;

    void split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count) final;
    void splitToBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                               std::vector<uint64_t>& bounds, size_t count) final;
    void splitUsingBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                                  const std::vector<uint64_t>& bounds) final;

    std::vector<Selection> selections;
    int32_t selectionSize;
};
