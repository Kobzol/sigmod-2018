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
        this->activeRows.clear();

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
                this->activeRows = it->second;
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
    while (this->activeRowIndex < static_cast<int32_t>(this->activeRows.size()))
    {
        auto row = this->activeRows[this->activeRowIndex];
        auto& data = this->rowData[row];

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
    if (this->activeRowIndex == static_cast<int32_t>(this->activeRows.size()))
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
    auto row = this->activeRows[this->activeRowIndex];
    auto& data = this->rowData[row];

    uint64_t value;
    if (this->right->getValueMaybe(selection, value))
    {
        return value;
    }

    return data[this->left->getColumnForSelection(selection)];
}
uint64_t HashJoiner::getColumn(uint32_t column)
{
    auto row = this->activeRows[this->activeRowIndex];
    auto& data = this->rowData[row];
    if (column < static_cast<uint32_t>(this->leftCols))
    {
        return data[column];
    }
    else return this->right->getColumn(column - this->leftCols);
}

void HashJoiner::fillHashTable()
{
    uint32_t row = 0;
    auto iterator = this->left;
    iterator->reset();
    while (iterator->getNext())
    {
        // materialize rows
        auto it = this->rowData.insert({ row, std::vector<uint64_t>(static_cast<size_t>(this->leftCols)) }).first;
        for (int i = 0; i < this->leftCols; i++)
        {
            it->second[i] = iterator->getColumn(i);
        }

        auto& predicate = this->join[0];
        uint64_t value = iterator->getValue(predicate.selections[this->leftIndex]);
        this->hashTable[value].push_back(row);

        row++;
    }
}

void HashJoiner::initialize()
{

}
