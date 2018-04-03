#include "merge-sort-joiner.h"
#include <cstring>
#include <iostream>

template <bool HAS_MULTIPLE_JOINS>
MergeSortJoiner<HAS_MULTIPLE_JOINS>::MergeSortJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, leftIndex, join)
{

}

template <bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::moveLeft()
{
    if (!this->left->getNext()) return false;
    this->leftValue = this->left->getColumn(this->leftColumns[0]);
    return true;
}
template <bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::moveRight()
{
    if (!this->right->getNext()) return false;
    this->rightValue = this->right->getColumn(this->rightColumns[0]);
    return true;
}

template<bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::skipLeftToRight()
{
    if (!this->left->skipTo(this->leftSelection, this->rightValue)) return false;
    this->leftValue = this->left->getColumn(this->leftColumns[0]);
    return true;
}

template<bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::skipRightToLeft()
{
    if (!this->right->skipTo(this->rightSelection, this->leftValue)) return false;
    this->rightValue = this->right->getColumn(this->rightColumns[0]);
    return true;
}

template <bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::checkJoins()
{
    for (int i = 1; i < joinSize; i++)
    {
        if (this->left->getColumn(this->leftColumns[i]) !=
            this->right->getColumn(this->rightColumns[i]))
        {
            return false;
        }
    }
    return true;
}

template <bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::findSameRow()
{
    if (NOEXPECT(!this->hasItems))
    {
        if (!this->moveLeft()) return false;
        if (!this->moveRight()) return false;
        this->hasItems = true;
    }

    while (this->leftValue != this->rightValue)
    {
        if (this->leftValue < this->rightValue)
        {
            if (!this->skipLeftToRight()) return false;
        }
        if (this->leftValue > this->rightValue)
        {
            if (!this->skipRightToLeft()) return false;
        }
    }

    this->iteratingRight = this->rightValue;
    this->right->save();
    this->hasRight = true;

    return true;
}

template <bool HAS_MULTIPLE_JOINS>
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::getNext()
{
    while (true)
    {
        if (this->hasRight)
        {
            if (!this->moveRight() || this->leftValue != this->rightValue)
            {
                if (!this->moveLeft()) return false;
                if (this->leftValue == this->iteratingRight)
                {
                    this->right->restore();
                }
                else
                {
                    this->hasRight = false;
                    continue;
                }
            }
        }
        else if (!this->findSameRow()) return false;

        if (HAS_MULTIPLE_JOINS)
        {
            // row is ready, joins must be checked
            if (!this->checkJoins())
            {
                continue;
            }
        }

#ifdef COLLECT_JOIN_SIZE
        this->rowCount++;
#endif
        return true;
    }
}

template <bool HAS_MULTIPLE_JOINS>
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::requireSelections(std::unordered_map<SelectionId, Selection> selections)
{
    this->initializeSelections(selections);

    this->left->prepareSortedAccess(this->join[0].selections[this->leftIndex]);
    this->right->prepareSortedAccess(this->join[0].selections[this->rightIndex]);
}

template <bool HAS_MULTIPLE_JOINS>
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::sumRows(std::vector<uint64_t>& results,
                                                  const std::vector<uint32_t>& columnIds,
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
        else rightColumns.emplace_back(columnIds[i] - this->left->getColumnCount(), i);
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
bool MergeSortJoiner<HAS_MULTIPLE_JOINS>::isSortedOn(const Selection& selection)
{
    return selection == this->join[0].selections[this->leftIndex];
}

template <bool HAS_MULTIPLE_JOINS>
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::aggregateDirect(std::vector<uint64_t>& results,
                                      const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                                      const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                                      size_t& count)
{
    std::vector<uint64_t> rightResults(results.size());
    while (true)
    {
        if (NOEXPECT(!this->findSameRow())) return;

        std::memset(rightResults.data(), 0, sizeof(uint64_t) * rightResults.size());
        uint64_t value = this->rightValue;
        int32_t rightCount = 0;
        bool hasNext = true;
        do
        {
            rightCount++;
            for (auto& c: rightColumns)
            {
                rightResults[c.second] += this->right->getColumn(c.first);
            }

            if (!this->moveRight())
            {
                hasNext = false;
                break;
            }
        }
        while (value == this->rightValue);

        value = this->leftValue;
        int32_t leftCount = 0;

        if (EXPECT(rightCount > 0))
        {
            do
            {
                for (auto& c: leftColumns)
                {
                    results[c.second] += this->left->getColumn(c.first) * rightCount;
                }
                leftCount++;
                if (!this->moveLeft())
                {
                    hasNext = false;
                    break;
                }
            }
            while (value == this->leftValue);
        }

        for (auto& c: rightColumns)
        {
            results[c.second] += rightResults[c.second] * leftCount;
        }

        count += leftCount * rightCount;
#ifdef COLLECT_JOIN_SIZE
        this->rowCount += leftCount * rightCount;
#endif

        if (NOEXPECT(!hasNext)) return;
    }
}

template<bool HAS_MULTIPLE_JOINS>
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::split(std::vector<std::unique_ptr<Iterator>>& container,
                                                std::vector<Iterator*>& groups, size_t count)
{
    std::vector<Iterator*> leftGroups;
    std::vector<uint64_t> bounds;
    this->left->splitToBounds(container, leftGroups, bounds, count);

    std::vector<Iterator*> rightGroups;
    this->right->splitUsingBounds(container, rightGroups, bounds);

    for (int i = 0; i < static_cast<int32_t>(rightGroups.size()); i++)
    {
        container.push_back(std::make_unique<MergeSortJoiner<HAS_MULTIPLE_JOINS>>(
            leftGroups[i],
            rightGroups[i],
            this->leftIndex,
            this->join
        ));
        groups.push_back(container.back().get());
    }
}

template<bool HAS_MULTIPLE_JOINS>
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::splitToBounds(std::vector<std::unique_ptr<Iterator>>& container,
                                                        std::vector<Iterator*>& groups, std::vector<uint64_t>& bounds,
                                                        size_t count)
{
    std::vector<Iterator*> leftGroups;
    this->left->splitToBounds(container, leftGroups, bounds, count);

    std::vector<Iterator*> rightGroups;
    this->right->splitUsingBounds(container, rightGroups, bounds);

    for (int i = 0; i < static_cast<int32_t>(rightGroups.size()); i++)
    {
        container.push_back(std::make_unique<MergeSortJoiner<HAS_MULTIPLE_JOINS>>(
                leftGroups[i],
                rightGroups[i],
                this->leftIndex,
                this->join
        ));
        groups.push_back(container.back().get());
    }
}
