#include <iostream>
#include "merge-sort-joiner.h"

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
        while (this->leftValue < this->rightValue)
        {
            if (!this->moveLeft()) return false;
        }
        while (this->leftValue > this->rightValue)
        {
            if (!this->moveRight()) return false;
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
void MergeSortJoiner<HAS_MULTIPLE_JOINS>::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count)
{
    std::vector<std::pair<uint32_t, uint32_t>> leftColumns; // column, result index
    std::vector<std::pair<uint32_t, uint32_t>> rightColumns;
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

    this->aggregateDirect(results, leftColumns, rightColumns, count);
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
    std::unordered_map<uint64_t, uint32_t> leftColumnCounts[3];
    std::unordered_map<uint64_t, uint32_t> rightColumnCounts[3];

    while (true)
    {
        if (NOEXPECT(!this->findSameRow())) return;

        for (int j = 1; j < this->joinSize; j++)
        {
            leftColumnCounts[j - 1].clear();
            rightColumnCounts[j - 1].clear();
        }

        uint64_t value = this->rightValue;
        int32_t rightCount = 0;
        bool hasNext = true;
        do
        {
            if (HAS_MULTIPLE_JOINS)
            {
                for (int j = 1; j < this->joinSize; j++)
                {
                    rightColumnCounts[j - 1][this->right->getColumn(this->rightColumns[j])]++;
                }
            }
            rightCount++;

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
                auto multiplier = static_cast<uint64_t>(rightCount);
                if (HAS_MULTIPLE_JOINS)
                {
                    for (int j = 1; j < this->joinSize; j++)
                    {
                        uint64_t joinValue = this->left->getColumn(this->leftColumns[j]);
                        multiplier = std::min(static_cast<uint32_t>(multiplier),
                                              rightColumnCounts[j - 1][joinValue]);
                        leftColumnCounts[j - 1][joinValue]++;
                    }
                }

                if (multiplier > 0)
                {
                    for (auto& c: leftColumns)
                    {
                        results[c.second] += this->left->getColumn(c.first) * multiplier;
                    }
                    leftCount++;
                }

                if (!this->moveLeft())
                {
                    hasNext = false;
                    break;
                }
            }
            while (value == this->leftValue);
        }

        if (!rightColumns.empty() && leftCount > 0)
        {
            this->right->restore();

            auto total = static_cast<int32_t>(rightCount);
            for (int i = 0; i < total; i++)
            {
                auto multiplier = static_cast<uint64_t>(leftCount);
                for (int j = 1; j < this->joinSize; j++)
                {
                    uint64_t joinValue = this->right->getColumn(this->rightColumns[j]);
                    multiplier = std::min(static_cast<uint32_t>(multiplier),
                                          leftColumnCounts[j - 1][joinValue]);
                }

                if (multiplier > 0)
                {
                    for (auto& c: rightColumns)
                    {
                        results[c.second] += this->right->getColumn(c.first) * multiplier;
                    }
                }
                else rightCount--;

                this->moveRight();
            }
        }

        count += leftCount * rightCount;
#ifdef COLLECT_JOIN_SIZE
        this->rowCount += leftCount * rightCount;
#endif

        if (NOEXPECT(!hasNext)) return;
    }
}
