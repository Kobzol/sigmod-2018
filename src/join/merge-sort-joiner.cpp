#include <iostream>
#include "merge-sort-joiner.h"

MergeSortJoiner::MergeSortJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, leftIndex, join)
{

}

bool MergeSortJoiner::moveLeft()
{
    if (!this->left->getNext()) return false;
    this->leftValue = this->left->getColumn(this->leftColumn);
    return true;
}
bool MergeSortJoiner::moveRight()
{
    if (!this->right->getNext()) return false;
    this->rightValue = this->right->getColumn(this->rightColumn);
    return true;
}
bool MergeSortJoiner::checkJoins()
{
    for (int i = 1; i < joinSize; i++)
    {
        if (this->left->getValue(this->join[i].selections[this->leftIndex]) !=
            this->right->getValue(this->join[i].selections[this->rightIndex]))
        {
            return false;
        }
    }
    return true;
}

bool MergeSortJoiner::findSameRow()
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

bool MergeSortJoiner::getNext()
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

        // row is ready, joins must be checked
        if (!this->checkJoins())
        {
            continue;
        }

#ifdef COLLECT_JOIN_SIZE
        this->rowCount++;
#endif
        return true;
    }
}

uint64_t MergeSortJoiner::getValue(const Selection& selection)
{
    uint64_t value;
    if (this->left->getValueMaybe(selection, value)) return value;
    return this->right->getValue(selection);
}

uint64_t MergeSortJoiner::getColumn(uint32_t column)
{
    if (column < static_cast<uint32_t>(this->leftColSize))
    {
        return this->left->getColumn(column);
    }
    return this->right->getColumn(column - this->leftColSize);
}

bool MergeSortJoiner::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (this->left->getValueMaybe(selection, value)) return true;
    return this->right->getValueMaybe(selection, value);
}

uint32_t MergeSortJoiner::getColumnForSelection(const Selection& selection)
{
    if (this->left->hasSelection(selection))
    {
        return this->left->getColumnForSelection(selection);
    }
    return this->right->getColumnForSelection(selection) + this->leftColSize;
}

void MergeSortJoiner::requireSelections(std::unordered_map<SelectionId, Selection>& selections)
{
    for (auto& j: this->join)
    {
        selections[j.selections[0].getId()] = j.selections[0];
        selections[j.selections[1].getId()] = j.selections[1];
    }

    this->left->requireSelections(selections);

    this->left->prepareSortedAccess(this->join[0].selections[this->leftIndex]);
    this->right->prepareSortedAccess(this->join[0].selections[this->rightIndex]);

    this->leftColSize = static_cast<uint32_t>(this->left->getColumnCount());

    this->leftColumn = this->left->getColumnForSelection(this->join[0].selections[this->leftIndex]);
    this->rightColumn = this->right->getColumnForSelection(this->join[0].selections[this->rightIndex]);
}

void MergeSortJoiner::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count)
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

    if (this->joinSize == 1)
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

bool MergeSortJoiner::hasSelection(const Selection& selection)
{
    return this->left->hasSelection(selection) || this->right->hasSelection(selection);
}

bool MergeSortJoiner::isSortedOn(const Selection& selection)
{
    return selection == this->join[0].selections[this->leftIndex];
}

void MergeSortJoiner::aggregateDirect(std::vector<uint64_t>& results,
                                      const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                                      const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                                      size_t& count)
{
    while (true)
    {
        if (NOEXPECT(!this->findSameRow())) return;

        uint64_t value = this->rightValue;
        int32_t rightCount = 0;
        bool hasNext = true;
        do
        {
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

        if (!rightColumns.empty() && leftCount > 0)
        {
            this->right->restore();

            for (int i = 0; i < rightCount; i++)
            {
                for (auto& c: rightColumns)
                {
                    results[c.second] += this->right->getColumn(c.first) * leftCount;
                }
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
