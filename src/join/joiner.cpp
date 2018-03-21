#include "merge-sort-joiner.h"
#include <iostream>
#include "joiner.h"
#include "../database.h"
#include "../hash-table.h"

std::unique_ptr<Iterator> Joiner::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                        const Selection& selection)
{
    assert(false);
    return std::unique_ptr<Iterator>();
}

void Joiner::fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                           HashTable& hashTable)
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
        auto vec = hashTable.insertRow(value, static_cast<uint32_t>(columnMapCols));
        // TODO: check current value before reinserting row

        // materialize rows
        vec->resize(vec->size() + columnMapCols);
        auto rowData = &vec->back() - countSub;

        for (auto c: leftColumns)
        {
            rowData[c.second] = this->left->getColumn(c.first);
        }
        for (auto c: rightColumns)
        {
            rowData[c.second] = this->right->getColumn(c.first);
        }
    }
}

int64_t Joiner::predictSize()
{
#ifdef COLLECT_JOIN_SIZE
    auto it = database.joinSizeMap.find(database.createJoinKey(this->join));
    if (it == database.joinSizeMap.end())
    {
        return this->left->predictSize() *
                this->right->predictSize();
    }
    return it->second;
#else
    return this->left->predictSize() *
            this->right->predictSize();
#endif
}

void Joiner::assignJoinSize(Database& database)
{
    if (this->left->isJoin())
    {
        this->left->assignJoinSize(database);
    }
    else if (this->left->getFilterReduction() == 0 && this->right->getFilterReduction() == 0)
    {
        database.addJoinSize(this->join, this->rowCount);
    }
}

bool Joiner::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (this->left->getValueMaybe(selection, value)) return true;
    return this->right->getValueMaybe(selection, value);
}

uint64_t Joiner::getValue(const Selection& selection)
{
    uint64_t value;
    if (this->left->getValueMaybe(selection, value)) return value;
    return this->right->getValue(selection);
}

bool Joiner::hasSelection(const Selection& selection)
{
    return this->left->hasSelection(selection) || this->right->hasSelection(selection);
}

void Joiner::initializeSelections(std::unordered_map<SelectionId, Selection>& selections)
{
    for (auto& j: this->join)
    {
        selections[j.selections[0].getId()] = j.selections[0];
        selections[j.selections[1].getId()] = j.selections[1];
    }

    this->left->requireSelections(selections);
    this->right->requireSelections(selections);

    for (auto& j : this->join)
    {
        this->leftColumns.push_back(this->left->getColumnForSelection(j.selections[this->leftIndex]));
        this->rightColumns.push_back(this->right->getColumnForSelection(j.selections[this->rightIndex]));
    }

    this->leftColSize = static_cast<uint32_t>(this->left->getColumnCount());
}

uint64_t Joiner::getColumn(uint32_t column)
{
    if (column < static_cast<uint32_t>(this->leftColSize))
    {
        return this->left->getColumn(column);
    }
    return this->right->getColumn(column - this->leftColSize);
}

uint32_t Joiner::getColumnForSelection(const Selection& selection)
{
    if (this->left->hasSelection(selection))
    {
        return this->left->getColumnForSelection(selection);
    }
    return this->right->getColumnForSelection(selection) + this->leftColSize;
}

bool Joiner::hasBinding(uint32_t binding)
{
    return this->left->hasBinding(binding) || this->right->hasBinding(binding);
}
