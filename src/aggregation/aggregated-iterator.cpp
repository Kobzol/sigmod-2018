#include <cstring>
#include <unordered_set>
#include "aggregated-iterator.h"

template <bool IS_GROUPBY_SUMMED>
AggregatedIterator<IS_GROUPBY_SUMMED>::AggregatedIterator(Iterator* inner, Selection groupBy,
                                                          std::vector<Selection> sumSelections)
        : WrapperIterator(inner), groupBy(std::move(groupBy)), sumSelections(std::move(sumSelections))
{
    if (IS_GROUPBY_SUMMED)
    {
        for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
        {
            if (this->sumSelections[i] == this->groupBy)
            {
                this->sumSelections.erase(this->sumSelections.begin() + i);
                i--;
            }
        }
    }

    std::unordered_set<SelectionId> visited;
    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        if (visited.find(this->sumSelections[i].getId()) != visited.end())
        {
            this->sumSelections.erase(this->sumSelections.begin() + i);
            i--;
            continue;
        }
        visited.insert(this->sumSelections[i].getId());
    }

    this->values = std::vector<uint64_t>(this->sumSelections.size() + 2);
    this->countSelection = Selection::aggregatedCount(this->groupBy.binding);
}

template <bool IS_GROUPBY_SUMMED>
bool AggregatedIterator<IS_GROUPBY_SUMMED>::getNext()
{
    if (!this->inner->getNext()) return false;

    std::memset(this->values.data() + 2, 0, this->sumSelections.size() * sizeof(uint64_t));
    this->count() = 0;
    this->value() = this->inner->getValue(this->groupBy);

    while (true)
    {
        this->inner->save();

        for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
        {
            this->sum(i) += this->inner->getValue(this->sumSelections[i]);
        }
        this->count()++;

        if (!this->inner->getNext()) return true;

        uint64_t value = this->inner->getValue(this->groupBy);
        if (value != this->value())
        {
            this->inner->restore();
            return true;
        }
    }
}

template <bool IS_GROUPBY_SUMMED>
std::unique_ptr<Iterator> AggregatedIterator<IS_GROUPBY_SUMMED>::createIndexedIterator(
        std::vector<std::unique_ptr<Iterator>>& container, const Selection& selection)
{
    container.push_back(this->inner->createIndexedIterator(container, selection));
    return std::make_unique<AggregatedIterator>(container.back().get(), this->groupBy, this->sumSelections);
}

template <bool IS_GROUPBY_SUMMED>
uint32_t AggregatedIterator<IS_GROUPBY_SUMMED>::getColumnForSelection(const Selection& selection)
{
    if (selection == this->groupBy) return 0;
    if (selection == this->countSelection) return 1;

    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        if (this->sumSelections[i] == selection)
        {
            return static_cast<uint32_t>(i + 2);
        }
    }

    assert(false);
    return 0;
}

template <bool IS_GROUPBY_SUMMED>
uint64_t AggregatedIterator<IS_GROUPBY_SUMMED>::getValue(const Selection& selection)
{
    if (selection == this->groupBy) return this->value();
    if (selection == this->countSelection) return this->count();

    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        if (this->sumSelections[i] == selection)
        {
            return this->sum(i);
        }
    }

    assert(false);
    return 0;
}

template <bool IS_GROUPBY_SUMMED>
uint64_t AggregatedIterator<IS_GROUPBY_SUMMED>::getColumn(uint32_t column)
{
    return this->values[column];
}

template <bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::requireSelections(std::unordered_map<SelectionId, Selection> selections)
{
    WrapperIterator::requireSelections(selections);
    this->inner->prepareSortedAccess(this->groupBy);
}

template <bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::prepareSortedAccess(const Selection& selection)
{
    assert(selection == this->groupBy);
    WrapperIterator::prepareSortedAccess(selection);
}

template <bool IS_GROUPBY_SUMMED>
int32_t AggregatedIterator<IS_GROUPBY_SUMMED>::getColumnCount()
{
    return static_cast<int32_t>(this->values.size());
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
    assert(this->values.size() == selections.size());
    for (auto& selection : selections)
    {
        *row++ = this->getValue(selection);
    }
}

template<bool IS_GROUPBY_SUMMED>
bool AggregatedIterator<IS_GROUPBY_SUMMED>::hasSelection(const Selection& selection)
{
    if (selection == this->groupBy) return true;
    if (selection == this->countSelection) return true;

    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        if (this->sumSelections[i] == selection)
        {
            return true;
        }
    }

    return false;
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::dumpPlan(std::ostream& ss)
{
    ss << "AI(";
    this->inner->dumpPlan(ss);
    ss << ")";
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::fillHashTable(const Selection& hashSelection,
                                                          const std::vector<Selection>& selections,
                                                          HashTable& hashTable)
{
    Iterator::fillHashTable(hashSelection, selections, hashTable);
}

template<bool IS_GROUPBY_SUMMED>
bool AggregatedIterator<IS_GROUPBY_SUMMED>::skipSameValue(const Selection& selection)
{
    return this->getNext();
}

template<bool IS_GROUPBY_SUMMED>
bool AggregatedIterator<IS_GROUPBY_SUMMED>::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (selection == this->groupBy)
    {
        value = this->value();
        return true;
    }
    if (selection == this->countSelection)
    {
        value = this->count();
        return true;
    }

    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        if (this->sumSelections[i] == selection)
        {
            value = this->sum(i);
            return true;
        }
    }

    return false;
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::splitToBounds(std::vector<std::unique_ptr<Iterator>>& container,
                                                          std::vector<Iterator*>& groups, std::vector<uint64_t>& bounds,
                                                          size_t count)
{
    std::vector<Iterator*> subGroups;
    this->inner->splitToBounds(container, subGroups, bounds, count);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<AggregatedIterator>(group, this->groupBy, this->sumSelections));
        groups.push_back(container.back().get());
    }
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::splitUsingBounds(std::vector<std::unique_ptr<Iterator>>& container,
                                                             std::vector<Iterator*>& groups,
                                                             const std::vector<uint64_t>& bounds)
{
    std::vector<Iterator*> subGroups;
    this->inner->splitUsingBounds(container, subGroups, bounds);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<AggregatedIterator>(group, this->groupBy, this->sumSelections));
        groups.push_back(container.back().get());
    }
}

template<bool IS_GROUPBY_SUMMED>
void AggregatedIterator<IS_GROUPBY_SUMMED>::split(std::vector<std::unique_ptr<Iterator>>& container,
                                                  std::vector<Iterator*>& groups, size_t count)
{
    std::vector<Iterator*> subGroups;
    this->inner->split(container, subGroups, count);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<AggregatedIterator>(group, this->groupBy, this->sumSelections));
        groups.push_back(container.back().get());
    }
}
