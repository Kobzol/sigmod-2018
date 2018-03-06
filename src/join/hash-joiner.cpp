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

HashJoiner::HashJoinerIterator::HashJoinerIterator(HashJoiner& joiner)
        : joiner(joiner),
          row(static_cast<size_t>(joiner.left->getColumnCount() + joiner.right->getColumnCount())),
          r1(joiner.left),
          r2(joiner.right)
{

}

void HashJoiner::HashJoinerIterator::reset()
{
    Iterator::reset();
    this->initialized = false;
}

bool HashJoiner::HashJoinerIterator::getNext()
{
    if (!this->initialized)
    {
        this->initialize();
    }

    auto joins = this->joiner.joins;
    auto joinSize = static_cast<int>(joins.size());
    auto iterator = this->iterators[1].get();
    if (this->activeRowIndex == -1)
    {
        if (!iterator->getNext()) return false;

        while (true)
        {
            this->activeRows.clear();
            bool matches = true;
            for (int i = 0; i < joinSize; i++)
            {
                uint64_t value = iterator->getValue(joins[i].selections[1]);
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
    for (int i = 0; i < this->r1->getColumnCount(); i++)
    {
        *rowData++ = data[i];
    }
    for (int i = 0; i < r2->getColumnCount(); i++)
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

uint64_t HashJoiner::HashJoinerIterator::getValue(const Selection& selection)
{
    uint32_t column = this->joiner.columnMap[selection.getId()];
    return this->row[column];
}

void HashJoiner::HashJoinerIterator::fillHashTable()
{
    uint32_t row = 0;
    auto& joins = this->joiner.joins;
    auto joinSize = (int) joins.size();

    auto iterator = this->iterators[0].get();
    while (iterator->getNext())
    {
        auto it = this->rowData.insert({ row, {} }).first;
        for (int i = 0; i < this->r1->getColumnCount(); i++)
        {
            it->second.push_back(iterator->getColumn(i));
        }

        for (int i = 0; i < joinSize; i++)
        {
            auto& join = joins[i];
            uint64_t value = iterator->getValue(join.selections[0]);
            hashes[i][value].push_back(row);
        }

        row++;
    }
}

void HashJoiner::HashJoinerIterator::initialize()
{
    this->iterators[0] = this->r1->createIterator();
    this->iterators[1] = this->r2->createIterator();

    this->hashes.clear();
    this->hashes.resize(this->joiner.joins.size());

    this->fillHashTable();

    this->initialized = true;
}

uint64_t HashJoiner::HashJoinerIterator::getColumn(uint32_t column)
{
    return this->row[column];
}
