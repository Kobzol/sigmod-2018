#include "hash-joiner.h"

template <bool HAS_MULTIPLE_JOINS>
HashJoiner<HAS_MULTIPLE_JOINS>::HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, leftIndex, join),
          rightValues(join.size())
{

}

template <bool HAS_MULTIPLE_JOINS>
bool HashJoiner<HAS_MULTIPLE_JOINS>::findRowByHash()
{
    auto iterator = this->right;
    if (this->activeRowIndex == -1)
    {
        if (!iterator->getNext()) return false;

        while (true)
        {
            uint64_t value = iterator->getColumn(this->rightColumn);
#ifdef USE_BLOOM_FILTER
            if (!this->bloomFilter.has(value))
            {
                if (!iterator->getNext()) return false; // use skipSameValue?
                continue;
            }
#endif
            auto it = this->hashTable.find(value);
            if (it == this->hashTable.end())
            {
                if (!iterator->getNext()) return false; // use skipSameValue?
                continue;
            }
            else
            {
                this->activeRow = &it->second;
                this->activeRowCount = static_cast<int32_t>(this->activeRow->size() / this->columnMapCols);
                break;
            }
        }

        this->activeRowIndex = 0;
        return true;
    }

    return true;
}

template <bool HAS_MULTIPLE_JOINS>
bool HashJoiner<HAS_MULTIPLE_JOINS>::checkRowPredicates()
{
    auto& vec = *this->activeRow;
    auto iterator = this->right;

    for (int i = 1; i < this->joinSize; i++)
    {
        rightValues[i] = iterator->getValue(this->join[i].selections[this->rightIndex]);
    }

    while (this->activeRowIndex < this->activeRowCount)
    {
        auto data = &vec[this->activeRowIndex * this->columnMapCols];

        bool rowOk = true;
        for (int i = 1; i < this->joinSize; i++)
        {
            auto& leftSel = this->join[i].selections[this->leftIndex];
            uint64_t leftVal = data[this->getColumnForSelection(leftSel)];
            if (leftVal != rightValues[i])
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

template <bool HAS_MULTIPLE_JOINS>
bool HashJoiner<HAS_MULTIPLE_JOINS>::getNext()
{
    this->activeRowIndex++;
    if (this->activeRowIndex == this->activeRowCount)
    {
        this->activeRowIndex = -1;
    }

    while (true)
    {
        if (!this->findRowByHash()) return false;
        if (HAS_MULTIPLE_JOINS)
        {
            if (!this->checkRowPredicates())
            {
                this->activeRowIndex = -1;
                continue;
            }
        }
        break;
    }

    return true;
}

template <bool HAS_MULTIPLE_JOINS>
uint64_t HashJoiner<HAS_MULTIPLE_JOINS>::getValue(const Selection& selection)
{
    uint64_t value;
    if (this->right->getValueMaybe(selection, value))
    {
        return value;
    }

    auto data = this->getCurrentRow();
    return data[this->getColumnForSelection(selection)];
}

// assumes left deep tree, doesn't initialize right child
template <bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::requireSelections(std::unordered_map<SelectionId, Selection>& selections)
{
    for (auto& j: this->join)
    {
        selections[j.selections[0].getId()] = j.selections[0];
        selections[j.selections[1].getId()] = j.selections[1];
    }
    this->left->requireSelections(selections);

    std::vector<Selection> leftSelections;
    this->prepareColumnMappings(selections, leftSelections);

    auto iterator = this->left;
    auto& predicate = this->join[0];
    auto selection = predicate.selections[this->leftIndex];

    this->rightColumn = this->right->getColumnForSelection(this->join[0].selections[this->rightIndex]);

    iterator->fillHashTable(selection, leftSelections, this->hashTable, this->bloomFilter);
}

template <bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::prepareColumnMappings(
        const std::unordered_map<SelectionId, Selection>& selections,
        std::vector<Selection>& leftSelections)
{
    for (auto& kv: selections)
    {
        if (this->left->hasSelection(kv.second))
        {
            leftSelections.push_back(kv.second);
        }
    }

    this->columnMapCols = static_cast<int32_t>(leftSelections.size());
    this->columnMap.resize(leftSelections.size());

    uint32_t columnId = 0;
    for (auto& sel: leftSelections)
    {
        this->setColumn(sel.getId(), columnId++);
    }
}

template <bool HAS_MULTIPLE_JOINS>
bool HashJoiner<HAS_MULTIPLE_JOINS>::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (this->right->getValueMaybe(selection, value))
    {
        return true;
    }

    auto id = selection.getId();
    for (int i = 0; i < this->columnMapCols; i++)
    {
        if (this->columnMap[i] == id)
        {
            auto data = this->getCurrentRow();
            value = data[i];
            return true;
        }
    }

    return false;
}

template <bool HAS_MULTIPLE_JOINS>
uint64_t HashJoiner<HAS_MULTIPLE_JOINS>::getColumn(uint32_t column)
{
    if (column < static_cast<uint32_t>(this->columnMapCols))
    {
        auto data = this->getCurrentRow();
        return data[column];
    }
    else return this->right->getColumn(column - this->columnMapCols);
}

template <bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                                             size_t& count)
{
    std::vector<std::pair<uint32_t, uint32_t>> leftColumns; // column, result index
    std::vector<std::pair<uint32_t, uint32_t>> rightColumns;
    auto colSize = static_cast<int32_t>(columnIds.size());

    for (int i = 0; i < colSize; i++)
    {
        if (columnIds[i] < static_cast<uint32_t>(this->columnMapCols))
        {
            leftColumns.emplace_back(columnIds[i], i);
        }
        else rightColumns.emplace_back(columnIds[i] - this->columnMapCols, i);
    }

    _mm_prefetch(results.data(), _MM_HINT_T0);
    if (!leftColumns.empty())
    {
        while (this->getNext())
        {
            auto data = this->getCurrentRow();
            for (auto c: leftColumns)
            {
                results[c.second] += data[c.first];
            }
            for (auto c: rightColumns)
            {
                results[c.second] += this->right->getColumn(c.first);
            }
            count++;
        }
    }
    else
    {
        while (this->getNext())
        {
            int index = 0;
            for (auto c: rightColumns)
            {
                results[index++] += this->right->getColumn(c.first);
            }
            count++;
        }
    }
}

template<bool HAS_MULTIPLE_JOINS>
void  HashJoiner<HAS_MULTIPLE_JOINS>::fillHashTable(const Selection& hashSelection,
                                                    const std::vector<Selection>& selections,
                                                    HashMap<uint64_t, std::vector<uint64_t>>& hashTable,
                                                    BloomFilter<BLOOM_FILTER_SIZE>& filter)
{
    auto columnMapCols = selections.size();
    auto countSub = static_cast<size_t>(selections.size() - 1);

    if (!this->getNext()) return;

    uint64_t value = this->getValue(hashSelection);
#ifdef USE_BLOOM_FILTER
    filter.set(value);
#endif
    auto* vec = &hashTable[value];

    std::vector<std::pair<uint32_t, uint32_t>> leftSelections; // column, index
    std::vector<std::pair<uint32_t, uint32_t>> rightSelections;

    uint32_t index = 0;
    for (auto& sel: selections)
    {
        auto col = this->getColumnForSelection(sel);
        if (col < static_cast<uint32_t>(this->columnMapCols))
        {
            leftSelections.emplace_back(col, index);
        }
        else rightSelections.emplace_back(col - this->columnMapCols, index);
        index++;
    }

    if (!leftSelections.empty())
    {
        while (true)
        {
            vec->resize(vec->size() + columnMapCols);
            auto rowData = &vec->back() - countSub;

            auto data = this->getCurrentRow();
            for (auto& sel: leftSelections)
            {
                rowData[sel.second] = data[sel.first];
            }
            for (auto& sel: rightSelections)
            {
                rowData[sel.second] = this->right->getColumn(sel.first);
            }

            if (!this->getNext()) return;
            uint64_t current = this->getValue(hashSelection);
            if (current != value)
            {
                value = current;
#ifdef USE_BLOOM_FILTER
                filter.set(value);
#endif
                vec = &hashTable[value];
            }
        }
    }
    else
    {
        while (true)
        {
            vec->resize(vec->size() + columnMapCols);
            auto rowData = &vec->back() - countSub;
            for (auto& sel: rightSelections)
            {
                rowData[sel.second] = this->right->getColumn(sel.first);
            }

            if (!this->getNext()) return;
            uint64_t current = this->getValue(hashSelection);
            if (current != value)
            {
                value = current;
#ifdef USE_BLOOM_FILTER
                filter.set(value);
#endif
                vec = &hashTable[value];
            }
        }
    }
}
