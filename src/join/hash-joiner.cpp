#include "hash-joiner.h"
#include <atomic>
#include <omp.h>

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
            uint64_t value = iterator->getColumn(this->rightColumns[0]);
            auto it = this->getFromMap(value);
            if (it == this->hashTable.end())
            {
                if (!iterator->skipSameValue(this->rightSelection)) return false; // use skipSameValue?
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
        rightValues[i] = iterator->getColumn(this->rightColumns[i]);
    }

    while (this->activeRowIndex < this->activeRowCount)
    {
        auto data = &vec[this->activeRowIndex * this->columnMapCols];

        bool rowOk = true;
        for (int i = 1; i < this->joinSize; i++)
        {
            uint64_t leftVal = data[this->hashColumns[i]];
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
#ifdef COLLECT_JOIN_SIZE
    this->rowCount++;
#endif
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
void HashJoiner<HAS_MULTIPLE_JOINS>::requireSelections(std::unordered_map<SelectionId, Selection> selections)
{
    this->initializeSelections(selections);

    std::vector<Selection> leftSelections;
    this->prepareColumnMappings(selections, leftSelections);

    this->left->fillHashTable(this->leftSelection, leftSelections, this->hashTable, this->bloomFilter);
    this->right->prepareSortedAccess(this->rightSelection);
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

    for (int i = 0; i < this->joinSize; i++)
    {
        this->hashColumns.push_back(this->getColumnForSelection(this->join[i].selections[this->leftIndex]));
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

#ifdef INDEX_AVAILABLE
    this->aggregateDirect(results, leftColumns, rightColumns, count);
#else
    _mm_prefetch(results.data(), _MM_HINT_T0);

    if (!leftColumns.empty())
    {
        while (this->getNext())
        {
            auto data = this->getCurrentRow();
            for (auto& c: leftColumns)
            {
                results[c.second] += data[c.first];
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
    else
    {
        while (this->getNext())
        {
            int index = 0;
            for (auto& c: rightColumns)
            {
                results[index++] += this->right->getColumn(c.first);
            }
            count++;
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
        }
    }
#endif
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

    uint32_t hashColumn = this->getColumnForSelection(hashSelection);
    uint64_t value = this->getColumn(hashColumn);
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
            uint64_t current = this->getColumn(hashColumn);
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
            uint64_t current = this->getColumn(hashColumn);
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

template<bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::workerAggregate(
        Iterator* iterator,
        std::vector<uint64_t>& results,
        const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
        const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
        std::atomic<size_t>& count)
{
    std::vector<uint64_t>* activeRow = nullptr;
    int32_t activeRowCount = 0;
    uint64_t rightValue;

    size_t localCount = 0;

    std::unordered_map<uint64_t, uint32_t> leftColumnCounts[3];
    std::unordered_map<uint64_t, uint32_t> rightColumnCounts[3];

    while (true)
    {
        if (HAS_MULTIPLE_JOINS)
        {
            for (int j = 1; j < this->joinSize; j++)
            {
                leftColumnCounts[j - 1].clear();
                rightColumnCounts[j - 1].clear();
            }
        }

        // find value that is on the left
        while (activeRow == nullptr)
        {
            if (!iterator->getNext())
            {
                count += localCount;
                return;
            }
            rightValue = iterator->getColumn(this->rightColumns[0]);

            auto it = this->getFromMap(rightValue);
            if (it != this->hashTable.end())
            {
                activeRow = &it->second;
                activeRowCount = static_cast<int32_t>(activeRow->size() / this->columnMapCols);
                break;
            }
        }

        if (HAS_MULTIPLE_JOINS)
        {
            for (int row = 0; row < activeRowCount; row++)
            {
                for (int j = 1; j < this->joinSize; j++)
                {
                    leftColumnCounts[j - 1][(*activeRow)[row * this->columnMapCols + this->hashColumns[j]]]++;
                }
            }
        }

        // iterate all on right
        uint64_t value;
        size_t rightCount = 0;
        bool hasNext = true;
        do
        {
            auto multiplier = static_cast<uint64_t>(activeRowCount);
            if (HAS_MULTIPLE_JOINS)
            {
                for (int j = 1; j < this->joinSize; j++)
                {
                    uint64_t joinValue = iterator->getColumn(this->rightColumns[j]);
                    multiplier = std::min(static_cast<uint32_t>(multiplier), leftColumnCounts[j - 1][joinValue]);
                    rightColumnCounts[j - 1][joinValue]++;
                }

                if (multiplier > 0)
                {
                    rightCount++;
                    for (auto& c: rightColumns)
                    {
                        results[c.second] += iterator->getColumn(c.first) * multiplier;
                    }
                }
            }
            else
            {
                rightCount++;
                for (auto& c: rightColumns)
                {
                    results[c.second] += iterator->getColumn(c.first) * multiplier;
                }
            }

            if (!iterator->getNext())
            {
                hasNext = false;
                break;
            }
            value = iterator->getColumn(this->rightColumns[0]);
        }
        while (value == rightValue);

        auto leftCount = static_cast<size_t>(activeRowCount);
        // iterate all on left
        if (!leftColumns.empty() && rightCount > 0)
        {
            for (int row = 0; row < activeRowCount; row++)
            {
                auto data = &(*activeRow)[row * this->columnMapCols];
                uint64_t multiplier = rightCount;

                if (HAS_MULTIPLE_JOINS)
                {
                    for (int j = 1; j < this->joinSize; j++)
                    {
                        uint64_t joinValue = data[this->hashColumns[j]];
                        multiplier = std::min(static_cast<uint32_t>(multiplier), rightColumnCounts[j - 1][joinValue]);
                    }

                    if (multiplier == 0)
                    {
                        leftCount--;
                        continue;
                    }
                }

                for (auto& c: leftColumns)
                {
                    results[c.second] += data[c.first] * multiplier;
                }
            }
        }

        localCount += rightCount * leftCount;

        if (!hasNext) break;
        auto it = this->getFromMap(value);
        if (it != this->hashTable.end())
        {
            activeRow = &it->second;
            activeRowCount = static_cast<int32_t>(activeRow->size() / this->columnMapCols);
        }
        else activeRow = nullptr;
    }

    count += localCount;
}

template<bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::aggregateDirect(std::vector<uint64_t>& results,
                                                     const std::vector<std::pair<uint32_t, uint32_t>>& leftColumns,
                                                     const std::vector<std::pair<uint32_t, uint32_t>>& rightColumns,
                                                     size_t& count)
{
    size_t threadCount = HASH_AGGREGATE_THREADS;
    std::vector<std::unique_ptr<Iterator>> groups;
    this->right->split(groups, threadCount);

    std::vector<std::vector<uint64_t>> workerResults(threadCount, results);
    std::atomic<size_t> atomicCount{count};

    #pragma omp parallel for num_threads(threadCount)
    for (int i = 0; i < static_cast<int32_t>(groups.size()); i++)
    {
        this->workerAggregate(groups[i].get(), workerResults[omp_get_thread_num()],
                              leftColumns, rightColumns, atomicCount);
    }

    for (int i = 0; i < static_cast<int32_t>(threadCount); i++)
    {
        for (int j = 0; j < static_cast<int32_t>(results.size()); j++)
        {
            results[j] += workerResults[i][j];
        }
    }

#ifdef COLLECT_JOIN_SIZE
    this->rowCount += atomicCount;
#endif

    count = atomicCount;
}

template<bool HAS_MULTIPLE_JOINS>
void HashJoiner<HAS_MULTIPLE_JOINS>::setColumn(SelectionId selectionId, uint32_t column)
{
    this->columnMap[column] = selectionId;
}

template<bool HAS_MULTIPLE_JOINS>
bool HashJoiner<HAS_MULTIPLE_JOINS>::hasSelection(const Selection& selection)
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

template<bool HAS_MULTIPLE_JOINS>
uint32_t HashJoiner<HAS_MULTIPLE_JOINS>::getColumnForSelection(const Selection& selection)
{
    auto id = selection.getId();
    for (int i = 0; i < this->columnMapCols; i++)
    {
        if (this->columnMap[i] == id) return static_cast<uint32_t>(i);
    }

    return this->right->getColumnForSelection(selection) + this->columnMapCols;
}
