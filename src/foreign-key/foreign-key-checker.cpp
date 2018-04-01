#include "foreign-key-checker.h"
#include "../database.h"

#include <cassert>

static bool isForeignKey(Iterator* left, Iterator* right, const Selection& primary, const Selection& foreign)
{
    if (!left->getNext()) return true;
    if (!right->getNext()) return true;

    uint64_t leftVal = left->getValue(primary);
    uint64_t rightVal = right->getValue(foreign);

    bool leftExhausted = false;
    bool rightExhausted = false;

    while (!leftExhausted && !rightExhausted)
    {
        if (leftVal == rightVal)
        {
            while (leftVal == rightVal)
            {
                if (!right->getNext())
                {
                    rightExhausted = true;
                    break;
                }
                else rightVal = right->getValue(foreign);
            }

            if (!left->getNext())
            {
                leftExhausted = true;
            }
            else leftVal = left->getValue(primary);
        }
        else if (leftVal < rightVal)
        {
            if (!left->getNext())
            {
                leftExhausted = true;
            }
            else leftVal = left->getValue(primary);
        }
        else return false;
    }

    return !(leftExhausted && !rightExhausted);
}

bool ForeignKeyChecker::isForeignKey(const Selection& primary, const Selection& foreign)
{
    assert(primary.isUnique());
    assert(database.hasIndexedIterator(primary));
    assert(database.hasIndexedIterator(foreign));

    auto leftMin = database.getMinValue(primary.relation, primary.column);
    auto leftMax = database.getMaxValue(primary.relation, primary.column);
    auto rightMin = database.getMinValue(foreign.relation, foreign.column);
    auto rightMax = database.getMaxValue(foreign.relation, foreign.column);

    if (leftMin > rightMin || rightMin > leftMax || rightMax > leftMax || rightMax < leftMin) return false;

    auto left = database.createIndexedIterator(primary, {});
    auto right = database.createIndexedIterator(foreign, {});

    left->prepareSortedAccess(primary);
    right->prepareSortedAccess(foreign);

    std::vector<std::unique_ptr<Iterator>> container;
    std::vector<Iterator*> leftGroups, rightGroups;
    std::vector<uint64_t> bounds;

    left->splitToBounds(container, leftGroups, bounds, FK_CHECK_SPLIT);
    right->splitUsingBounds(container, rightGroups, bounds);

    auto count = static_cast<int32_t>(leftGroups.size());
    std::vector<uint32_t> valid(static_cast<size_t>(count));

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < count; i++)
    {
        auto l = leftGroups[i];
        auto r = rightGroups[i];

        valid[i] = ::isForeignKey(l, r, primary, foreign) ? 1 : 0;
        if (!valid[i])
        {
            #pragma omp cancel for
        }
    }

    for (auto& v: valid)
    {
        if (v == 0) return false;
    }

    return true;
}
