#include "hash-joiner.h"

HashJoiner::HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, join),
          leftIndex(leftIndex),
          rightIndex(1 - leftIndex),
          row(static_cast<size_t>(left->getColumnCount() + right->getColumnCount()))
{

}

void HashJoiner::reset()
{
    Iterator::reset();

    this->left->reset();
    this->right->reset();
    this->initialized = false; // TODO: switch to true
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
    auto joinSize = static_cast<int>(this->join.size());
    while (this->activeRowIndex < static_cast<int32_t>(this->activeRows.size()))
    {
        auto row = this->activeRows[this->activeRowIndex];
        auto& data = this->rowData[row];

        bool rowOk = true;
        for (int i = 1; i < joinSize; i++)
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
    if (!this->initialized)
    {
        this->initialize();
    }

    while (true)
    {
        if (!this->findRowByHash()) return false;
        if (!this->checkRowPredicates())
        {
            this->activeRowIndex = -1;
            continue;
        }
        else break;
    }

    auto iterator = this->right;
    auto row = this->activeRows[this->activeRowIndex];
    auto& data = this->rowData[row];
    auto* rowData = &this->row[0];
    for (int i = 0; i < this->leftCols; i++)
    {
        *rowData++ = data[i];
    }
    for (int i = 0; i < this->rightCols; i++)
    {
        *rowData++ = iterator->getColumn(static_cast<uint32_t>(i));
    }

    this->activeRowIndex++;
    if (this->activeRowIndex == static_cast<int32_t>(this->activeRows.size()))
    {
        this->activeRowIndex = -1;
    }

    return true;
}

uint64_t HashJoiner::getValue(const Selection& selection)
{
    uint32_t column = this->columnMap[selection.getId()];
    return this->row[column];
}

void HashJoiner::fillHashTable()
{
    uint32_t row = 0;
    auto iterator = this->left;
    iterator->reset();
    while (iterator->getNext())
    {
        // materialize rows
        auto it = this->rowData.insert({ row, {} }).first;
        for (int i = 0; i < this->leftCols; i++)
        {
            it->second.push_back(iterator->getColumn(i));
        }

        auto& predicate = this->join[0];
        uint64_t value = iterator->getValue(predicate.selections[this->leftIndex]);
        this->hashTable[value].push_back(row);

        row++;
    }
}

void HashJoiner::initialize()
{
    this->left->reset();
    this->right->reset();

    this->hashTable.clear();

    this->fillHashTable();

    this->initialized = true;
}

uint64_t HashJoiner::getColumn(uint32_t column)
{
    return this->row[column];
}
