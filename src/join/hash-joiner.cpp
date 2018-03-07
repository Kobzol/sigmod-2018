#include "hash-joiner.h"

static std::vector<uint32_t> intersect(const std::vector<uint32_t> active, const std::vector<uint32_t> added)
{
    if (active.empty()) return added;

    int indexL = 0;
    int indexR = 0;
    std::vector<uint32_t> result;

    auto activeSize = static_cast<int>(active.size());
    auto addedSize = static_cast<int>(added.size());

    while (true)
    {
        if (indexL >= activeSize) return result;
        if (indexR >= addedSize) return result;

        if (active[indexL] == added[indexR])
        {
            result.push_back(active[indexL]);
            indexL++, indexR++;
        }
        else if (active[indexL] < added[indexR]) indexL++;
        else indexR++;
    }
}

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

bool HashJoiner::getNext()
{
    if (!this->initialized)
    {
        this->initialize();
    }

    auto joinSize = static_cast<int>(this->join.size());
    auto iterator = this->right;
    if (this->activeRowIndex == -1)
    {
        if (!iterator->getNext()) return false;

        while (true)
        {
            this->activeRows.clear();
            bool matches = true;
            for (int i = 0; i < joinSize; i++)
            {
                uint64_t value = iterator->getValue(this->join[i].selections[this->rightIndex]);
                auto it = hashes[i].find(value);
                if (it == hashes[i].end())
                {
                    matches = false;
                    break;
                }
                else this->activeRows = intersect(this->activeRows, it->second);
            }

            if (matches && !this->activeRows.empty())
            {
                break;
            }
            else if (!iterator->getNext()) return false;
        }

        this->activeRowIndex = 0;
    }

    auto row = this->activeRows[this->activeRowIndex];
    auto* rowData = &this->row[0];
    auto& data = this->rowData[row];
    for (int i = 0; i < this->leftCols; i++)
    {
        *rowData++ = data[i];
    }
    for (int i = 0; i < this->rightCols; i++)
    {
        *rowData++ = iterator->getColumn(static_cast<uint32_t>(i));
    }

    this->activeRowIndex++;
    if (this->activeRowIndex == (int) this->activeRows.size())
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
    auto joinSize = (int) this->join.size();

    auto iterator = this->left;
    iterator->reset();
    while (iterator->getNext())
    {
        auto it = this->rowData.insert({ row, {} }).first;
        for (int i = 0; i < this->leftCols; i++)
        {
            it->second.push_back(iterator->getColumn(i));
        }

        for (int i = 0; i < joinSize; i++)
        {
            auto& predicate = this->join[i];
            uint64_t value = iterator->getValue(predicate.selections[this->leftIndex]);
            hashes[i][value].push_back(row);
        }

        row++;
    }
}

void HashJoiner::initialize()
{
    this->left->reset();
    this->right->reset();

    this->hashes.clear();
    this->hashes.resize(this->join.size());

    this->fillHashTable();

    this->initialized = true;
}

uint64_t HashJoiner::getColumn(uint32_t column)
{
    return this->row[column];
}
