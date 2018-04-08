#include <iostream>
#include <cstring>
#include "index-joiner.h"
#include "../database.h"

template <bool HAS_MULTIPLE_JOINS>
IndexJoiner<HAS_MULTIPLE_JOINS>::IndexJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join,
                                             bool hasLeftIndex)
        : Joiner(left, right, leftIndex, join), hasLeftIndex(hasLeftIndex)
{

}

template <bool HAS_MULTIPLE_JOINS>
bool IndexJoiner<HAS_MULTIPLE_JOINS>::getNext()
{
    while (true)
    {
        if (!this->right->getNext())
        {
            if (!this->left->getNext())
            {
                return false;
            }
            uint64_t value = this->left->getColumn(this->leftColumns[0]);
            this->right->iterateValue(this->rightSelection, value);
            continue;
        }

        if (HAS_MULTIPLE_JOINS)
        {
            bool ok = true;
            for (int i = 1; i < joinSize; i++)
            {
                if (this->left->getColumn(this->leftColumns[i]) !=
                    this->right->getColumn(this->rightColumns[i]))
                {
                    ok = false;
                    break;
                }
            }
            if (ok)
            {
#ifdef COLLECT_JOIN_SIZE
                this->rowCount++;
#endif
                return true;
            }
        }
        else
        {
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
            return true;
        }
    }
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::requireSelections(std::unordered_map<SelectionId, Selection> selections)
{
    this->initializeSelections(selections);

    if (this->hasLeftIndex)
    {
        this->left->prepareSortedAccess(this->leftSelection);
    }

    this->right->prepareIndexedAccess(this->rightSelection);
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                                              const std::vector<Selection>& selections, size_t& count)
{
    using Pair = std::pair<uint32_t, uint32_t>;
    std::vector<Pair> leftColumns; // column, result index
    std::vector<Pair> rightColumns;
    auto colSize = static_cast<int32_t>(columnIds.size());

    for (int i = 0; i < colSize; i++)
    {
        auto column = columnIds[i];
        if (column < this->leftColSize)
        {
            leftColumns.emplace_back(columnIds[i], i);
        }
        else rightColumns.emplace_back(columnIds[i] - this->leftColSize, i);
    }

    std::sort(leftColumns.begin(), leftColumns.end(), [](const Pair& lhs, const Pair& rhs) {
        return lhs.first < rhs.first;
    });
    std::sort(rightColumns.begin(), rightColumns.end(), [](const Pair& lhs, const Pair& rhs) {
        return lhs.first < rhs.first;
    });

    if (!HAS_MULTIPLE_JOINS)
    {
        this->aggregateDirect(results, leftColumns, rightColumns, count);
    }
    else
    {
        while (this->getNext())
        {
            for (auto& c: leftColumns)
            {
                results[c.second] += this->left->getColumn(c.first);
            }
            for (auto& c: rightColumns)
            {
                results[c.second] += this->right->getColumn(c.first);
            }
            count++;
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
        }
    }
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::aggregateDirect(std::vector<uint64_t>& results,
                                  const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                                  const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns, size_t& count)
{
    if (!this->left->getNext()) return;

    const auto resultCount = static_cast<int32_t>(results.size());
    std::vector<uint64_t> rightResults(static_cast<size_t>(resultCount));

    while (true)
    {
        uint64_t leftValue = this->left->getColumn(this->leftColumns[0]);
        this->right->iterateValue(this->rightSelection, leftValue);

        std::memset(rightResults.data(), 0, sizeof(uint64_t) * rightResults.size());
        size_t rightCount = 0;

        if (!rightColumns.empty())
        {
            while (this->right->getNext())
            {
                for (auto& c: rightColumns)
                {
                    rightResults[c.second] += this->right->getColumn(c.first);
                }
                rightCount++;
            }
        }
        else rightCount = this->right->iterateCount();

        bool hasNext = true;
        int32_t leftCount = 0;

        if (rightCount > 0)
        {
            uint64_t value;
            do
            {
                for (auto& c: leftColumns)
                {
                    results[c.second] += this->left->getColumn(c.first) * rightCount;
                }
                leftCount++;
                if (!this->left->getNext())
                {
                    hasNext = false;
                    break;
                }

                value = this->left->getColumn(this->leftColumns[0]);
            }
            while (value == leftValue);
        }
        else if (!this->left->getNext()) return;

        for (int i = 0; i < resultCount; i++)
        {
            results[i] += rightResults[i] * leftCount;
        }

        count += leftCount * rightCount;
#ifdef COLLECT_JOIN_SIZE
        this->rowCount += leftCount * rightCount;
#endif
        if (!hasNext) return;
    }
}

template<bool HAS_MULTIPLE_JOINS>
bool IndexJoiner<HAS_MULTIPLE_JOINS>::isSortedOn(const Selection& selection)
{
    return this->left->isSortedOn(selection);
}

template<bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::split(std::vector<std::unique_ptr<Iterator>>& container,
                                            std::vector<Iterator*>& groups, size_t count)
{
    std::vector<Iterator*> subGroups;
    this->left->split(container, subGroups, count);
    for (auto& it: subGroups)
    {
        container.push_back(this->right->createIndexedIterator(container, this->rightSelection));
        container.push_back(std::make_unique<IndexJoiner<HAS_MULTIPLE_JOINS>>(
                        it, container.back().get(),
                        this->leftIndex, this->join, this->hasLeftIndex)
        );
        groups.push_back(container.back().get());
    }
}

template<bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::splitToBounds(std::vector<std::unique_ptr<Iterator>>& container,
                                                    std::vector<Iterator*>& groups, std::vector<uint64_t>& bounds,
                                                    size_t count)
{
    std::vector<Iterator*> subGroups;
    this->left->splitToBounds(container, subGroups, bounds, count);
    for (auto& it: subGroups)
    {
        container.push_back(this->right->createIndexedIterator(container, this->rightSelection));
        container.push_back(std::make_unique<IndexJoiner<HAS_MULTIPLE_JOINS>>(
                it, container.back().get(),
                this->leftIndex, this->join, this->hasLeftIndex)
        );
        groups.push_back(container.back().get());
    }
}
