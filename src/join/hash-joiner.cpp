#include <iostream>
#include "hash-joiner.h"

HashJoiner::HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, join),
          leftIndex(leftIndex),
          rightIndex(1 - leftIndex),
          joinSize(static_cast<int>(join.size()))
{
    this->fillHashTable();
}

void HashJoiner::reset()
{
    Iterator::reset();

    //this->left->reset();
    //this->right->reset();
    //this->initialized = false;
}

bool HashJoiner::findRowByHash()
{
    auto iterator = this->right;
    if (this->activeRowIndex == -1)
    {
        if (!iterator->getNext()) return false;

        while (true)
        {
            uint64_t value = iterator->getValue(this->join[0].selections[this->rightIndex]);
            auto it = this->hashTable.find(value);
            if (it == this->hashTable.end())
            {
                if (!iterator->getNext()) return false;
                continue;
            }
            else
            {
                this->activeValue = value;
                this->activeRowCount = static_cast<int32_t>(this->hashTable[value].size());
                break;
            }
        }

        this->activeRowIndex = 0;
        return true;
    }

    return true;
}

bool HashJoiner::checkRowPredicates()
{
    auto iterator = this->right;
    while (this->activeRowIndex < this->activeRowCount)
    {
        auto& data = this->hashTable[this->activeValue][this->activeRowIndex];

        bool rowOk = true;
        for (int i = 1; i < this->joinSize; i++)
        {
            auto& leftSel = this->join[i].selections[this->leftIndex];
            auto& rightSel = this->join[i].selections[this->rightIndex];

            uint64_t leftVal = data[this->left->getColumnForSelection(leftSel)];
            uint64_t rightVal = iterator->getValue(rightSel);
            if (leftVal != rightVal)
            {
                rowOk = false;
                break;
            }
        }

        if (rowOk) return true;
        this->activeRowIndex++;
    }

    return false;
}

bool HashJoiner::getNext()
{
    this->activeRowIndex++;
    if (this->activeRowIndex == this->activeRowCount)
    {
        this->activeRowIndex = -1;
    }

    while (true)
    {
        if (!this->findRowByHash()) return false;
        if (this->joinSize == 1) break;
        if (!this->checkRowPredicates())
        {
            this->activeRowIndex = -1;
            continue;
        }
        else break;
    }

    return true;
}

uint64_t HashJoiner::getValue(const Selection& selection)
{
    auto& data = this->hashTable[this->activeValue][this->activeRowIndex];

    uint64_t value;
    if (this->right->getValueMaybe(selection, value))
    {
        return value;
    }

    return data[this->left->getColumnForSelection(selection)];
}
uint64_t HashJoiner::getColumn(uint32_t column)
{
    auto& data = this->hashTable[this->activeValue][this->activeRowIndex];
    if (column < static_cast<uint32_t>(this->leftCols))
    {
        return data[column];
    }
    else return this->right->getColumn(column - this->leftCols);
}
void HashJoiner::fillRow(uint64_t* row)
{
    auto& data = this->hashTable[this->activeValue][this->activeRowIndex];

    for (int i = 0; i < this->leftCols; i++)
    {
        *row++ = data[i];
    }
    this->right->fillRow(row);
}
void HashJoiner::sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns)
{
    auto& data = this->hashTable[this->activeValue][this->activeRowIndex];
    /*std::vector<uint32_t> left;
    std::vector<uint32_t> right;

    for (auto col: columns)
    {
        if (col < this->leftCols) left.push_back(col);
        else right.push_back(col - this->leftCols);
    }

    auto leftSize = static_cast<int32_t>(left.size());
    for (int i = 0; i < leftSize; i++)
    {
        sums
    }

    this->right->sumRow(sums, right);*/

    for (int i = 0; i < columns.size(); i++)
    {
        auto col = columns[i];
        if (col < this->leftCols)
        {
            sums[i] += data[col];
        }
        else sums[i] += this->right->getColumn(static_cast<uint32_t>(col - this->leftCols));
    }
}

void HashJoiner::fillHashTable()
{
    auto iterator = this->left;
    auto& predicate = this->join[0];
    auto selection = predicate.selections[this->leftIndex];

    while (iterator->getNext())
    {
        uint64_t value = iterator->getValue(selection);

        auto it = this->hashTable.find(value);
        if (it == this->hashTable.end())
        {
            it = this->hashTable.insert({ value, {} }).first;
        }

        // materialize rows
        it->second.emplace_back(this->leftCols);
        auto& rowData = it->second.back();
        iterator->fillRow(rowData.data());
    }
}
