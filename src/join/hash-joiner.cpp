#include "hash-joiner.h"

HashJoiner::HashJoiner(Iterator* left, Iterator* right, uint32_t leftIndex, Join& join)
        : Joiner(left, right, join),
          leftIndex(leftIndex),
          rightIndex(1 - leftIndex),
          joinSize(static_cast<int>(join.size())),
          rightValues(join.size())
{

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
                if (!iterator->skipSameValue()) return false;
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

bool HashJoiner::checkRowPredicates()
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
    uint64_t value;
    if (this->right->getValueMaybe(selection, value))
    {
        return value;
    }

    auto data = this->getCurrentRow();
    return data[this->getColumnForSelection(selection)];
}

void HashJoiner::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
    auto data = this->getCurrentRow();

    for (auto& sel: selections)
    {
        uint64_t value;
        if (!this->right->getValueMaybe(sel, value))
        {
            value = data[this->getColumnForSelection(sel)];
        }

//        _mm_stream_si64(reinterpret_cast<long long int*>(row), static_cast<int64_t>(value));
//        row++;
        *row++ = value;
    }
}
void HashJoiner::sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& selections)
{
    auto data = this->getCurrentRow();

    auto colSize = static_cast<int32_t>(selections.size());
    for (int i = 0; i < colSize; i++)
    {
        auto column = selections[i];
        uint64_t value;

        if (column < static_cast<uint32_t>(this->columnMapCols))
        {
            value = data[column];
        }
        else value = this->right->getColumn(column - this->columnMapCols);

        sums[i] += value;
    }
}

// assumes left deep tree, doesn't initialize right child
void HashJoiner::requireSelections(std::unordered_map<SelectionId, Selection>& selections)
{
    for (auto& j: join)
    {
        selections[j.selections[0].getId()] = j.selections[0];
        selections[j.selections[1].getId()] = j.selections[1];
    }
    left->requireSelections(selections);

    std::vector<Selection> leftSelections;
    this->prepareColumnMappings(selections, leftSelections);

    auto iterator = this->left;
    auto& predicate = this->join[0];
    auto selection = predicate.selections[this->leftIndex];

    auto countSub = static_cast<size_t>(this->columnMapCols - 1);

    while (iterator->getNext())
    {
        uint64_t value = iterator->getValue(selection);
        auto& vec = this->hashTable[value];

        // materialize rows
        vec.resize(vec.size() + this->columnMapCols);
        auto rowData = &vec.back() - countSub;
        iterator->fillRow(rowData, leftSelections);
    }
}

void HashJoiner::prepareColumnMappings(
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

bool HashJoiner::getValueMaybe(const Selection& selection, uint64_t& value)
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

bool HashJoiner::hasSelection(const Selection& selection)
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
uint64_t HashJoiner::getColumn(uint32_t column)
{
    if (column < static_cast<uint32_t>(this->columnMapCols))
    {
        auto data = this->getCurrentRow();
        return data[column];
    }
    else return this->right->getColumn(column - this->columnMapCols);
}
