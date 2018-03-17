#include <iostream>
#include <cstring>
#include "index-joiner.h"

template <bool HAS_MULTIPLE_JOINS>
IndexJoiner<HAS_MULTIPLE_JOINS>::IndexJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, leftIndex, join)
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

    this->left->prepareSortedAccess(this->leftSelection);
    this->right->prepareIndexedAccess();
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count)
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
        else rightColumns.emplace_back(columnIds[i] - this->leftColSize, i);
    }

    if (!HAS_MULTIPLE_JOINS)
    {
        this->aggregateDirect(results, leftColumns, rightColumns, count);
    }
    else this->aggregateDirectMultiple(results, leftColumns, rightColumns, count);
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::aggregateDirectMultiple(
        std::vector<uint64_t>& results,
        const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
        const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns, size_t& count
)
{
    if (!this->left->getNext()) return;

    std::unordered_map<uint64_t, uint32_t> leftColumnCounts[3];
    std::unordered_map<uint64_t, uint32_t> rightColumnCounts[3];

    while (true)
    {
        for (int j = 1; j < this->joinSize; j++)
        {
            leftColumnCounts[j - 1].clear();
            rightColumnCounts[j - 1].clear();
        }

        uint64_t leftValue = this->left->getColumn(this->leftColumns[0]);
        this->right->iterateValue(this->rightSelection, leftValue);
        this->right->save();

        size_t rightCount = 0;
        while (this->right->getNext())
        {
            for (int j = 1; j < this->joinSize; j++)
            {
                rightColumnCounts[j - 1][this->right->getColumn(this->rightColumns[j])]++;
            }
            rightCount++;
        }

        bool hasNext = true;
        int32_t leftCount = 0;
        if (rightCount > 0)
        {
            uint64_t value;
            do
            {
                uint64_t multiplier = rightCount;
                for (int j = 1; j < this->joinSize; j++)
                {
                    uint64_t joinValue = this->left->getColumn(this->leftColumns[j]);
                    multiplier = std::min(static_cast<uint32_t>(multiplier),
                                          rightColumnCounts[j - 1][joinValue]);
                    leftColumnCounts[j - 1][joinValue]++;
                }

                if (multiplier > 0)
                {
                    for (auto& c: leftColumns)
                    {
                        results[c.second] += this->left->getColumn(c.first) * multiplier;
                    }
                    leftCount++;
                }

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

        if (!rightColumns.empty() && leftCount > 0)
        {
            this->right->restore();
            while (this->right->getNext())
            {
                auto multiplier = static_cast<uint64_t>(leftCount);
                for (int j = 1; j < this->joinSize; j++)
                {
                    uint64_t joinValue = this->right->getColumn(this->rightColumns[j]);
                    multiplier = std::min(static_cast<uint32_t>(multiplier),
                                          leftColumnCounts[j - 1][joinValue]);
                }

                if (multiplier == 0)
                {
                    rightCount--;
                    continue;
                }

                for (auto& c: rightColumns)
                {
                    results[c.second] += this->right->getColumn(c.first) * multiplier;
                }
            }
        }

        count += leftCount * rightCount;
#ifdef COLLECT_JOIN_SIZE
        this->rowCount += leftCount * rightCount;
#endif
        if (!hasNext) return;
    }
}

template <bool HAS_MULTIPLE_JOINS>
void IndexJoiner<HAS_MULTIPLE_JOINS>::aggregateDirect(std::vector<uint64_t>& results,
                                  const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                                  const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns, size_t& count)
{
    if (!this->left->getNext()) return;

    std::vector<uint64_t> rightResults(results.size());
    while (true)
    {
        uint64_t leftValue = this->left->getColumn(this->leftColumns[0]);
        this->right->iterateValue(this->rightSelection, leftValue);

        std::memset(rightResults.data(), 0, sizeof(uint64_t) * rightResults.size());
        size_t rightCount = 0;

        while (this->right->getNext())
        {
            for (auto& c: rightColumns)
            {
                rightResults[c.second] += this->right->getColumn(c.first);
            }
            rightCount++;
        }

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

        for (int i = 0; i < static_cast<int32_t>(rightResults.size()); i++)
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
