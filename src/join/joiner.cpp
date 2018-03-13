#include "merge-sort-joiner.h"
#include <iostream>
#include "joiner.h"

void Joiner::setColumn(SelectionId selectionId, uint32_t column)
{
    this->columnMap[column] = selectionId;
}

bool Joiner::hasSelection(const Selection& selection)
{
    if (this->right->hasSelection(selection)) return true;

    auto id = selection.getId();
    for (int i = 0; i < this->columnMapCols; i++)
    {
        if (this->columnMap[i] == id)
        {
            return true;
        }
    }

    return false;
}

std::unique_ptr<Iterator> Joiner::createIndexedIterator()
{
    assert(false);
    return std::unique_ptr<Iterator>();
}

void Joiner::fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                                    HashMap<uint64_t, std::vector<uint64_t>>& hashTable,
                                    BloomFilter<BLOOM_FILTER_SIZE>& filter)
{
    auto columnMapCols = static_cast<int32_t>(selections.size());
    auto countSub = static_cast<size_t>(selections.size() - 1);

    std::vector<std::pair<uint32_t, uint32_t>> leftColumns; // column, result index
    std::vector<std::pair<uint32_t, uint32_t>> rightColumns;

    for (int i = 0; i < columnMapCols; i++)
    {
        if (this->left->hasSelection(selections[i]))
        {
            leftColumns.emplace_back(this->left->getColumnForSelection(selections[i]), i);
        }
        else rightColumns.emplace_back(this->right->getColumnForSelection(selections[i]), i);
    }

    uint32_t hashColumn = this->getColumnForSelection(hashSelection);
    while (this->getNext())
    {
        uint64_t value = this->getColumn(hashColumn);
        auto& vec = hashTable[value];
        filter.set(value);

        // materialize rows
        vec.resize(vec.size() + columnMapCols);
        auto rowData = &vec.back() - countSub;
        for (auto c: leftColumns)
        {
            rowData[c.second] += this->left->getColumn(c.first);
        }
        for (auto c: rightColumns)
        {
            rowData[c.second] += this->right->getColumn(c.first);
        }
    }
}