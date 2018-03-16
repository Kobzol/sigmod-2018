#include "sort-filter-iterator.h"
#include "../database.h"
#include "../index/sort-index.h"

#include <algorithm>
#include <cmath>

RowEntry* toPtr(SortIndex& index, const std::vector<RowEntry>::iterator& iterator)
{
    return index.data.data() + (iterator - index.data.begin());
}

void createIterators(const Filter& filter, RowEntry** start, RowEntry** end)
{
    auto index = &database.getSortIndex(filter.selection.relation, filter.selection.column);

    RowEntry* last = index->data.data() + index->data.size();
    uint64_t value = filter.value;
    if (filter.oper == '<')
    {
        *start = index->data.data();
        *end = toPtr(*index, std::lower_bound(index->data.begin(), index->data.end(), value,
                                              [](const RowEntry& entry, uint64_t val) {
                                                  return entry.value < val;
                                              }));
    }
    else if (filter.oper == '>')
    {
        *start = toPtr(*index, std::upper_bound(index->data.begin(), index->data.end(), value,
                                                [](uint64_t val, const RowEntry& entry) {
                                                    return val < entry.value;
                                                }));
        *end = last;
    }
    else
    {
        *start = toPtr(*index, std::lower_bound(index->data.begin(), index->data.end(), value,
                                                [](const RowEntry& entry, uint64_t val) {
                                                    return entry.value < val;
                                                }));
        *end = toPtr(*index, std::upper_bound(index->data.begin(), index->data.end(), value,
                                              [](uint64_t val, const RowEntry& entry) {
                                                  return val < entry.value;
                                              }));
    }
}

SortFilterIterator::SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters)
        : FilterIterator(relation, binding, filters)
{
    if (!filters.empty())
    {
        this->sortFilter = this->filters[0];
        this->startFilterIndex = 1;
        this->sortSelection = this->sortFilter.selection;
        this->index = &database.getSortIndex(this->sortFilter.selection.relation, this->sortFilter.selection.column);

        createIterators(this->sortFilter, &this->start, &this->end);
    }

    this->start--;
    this->originalStart = this->start;
}

SortFilterIterator::SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters,
                                       RowEntry* start, RowEntry* end)
        : FilterIterator(relation, binding, filters), start(start), end(end)
{
    this->originalStart = start;
}

bool SortFilterIterator::getNext()
{
    this->start++;

    for (; this->start < this->end; this->start++)
    {
        this->rowIndex = this->start->row;
        if (this->passesFilters())
        {
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
            return true;
        }
    }

    return false;
}

bool SortFilterIterator::skipSameValue(const Selection& selection)
{
    if (selection == this->sortFilter.selection)
    {
        uint64_t value = this->relation->getValue(this->rowIndex, this->sortFilter.selection.column);
        for (; this->start < this->end; this->start++)
        {
            this->rowIndex = this->start->row;
            if (this->relation->getValue(this->rowIndex, this->sortFilter.selection.column) != value)
            {
#ifdef COLLECT_JOIN_SIZE
                this->rowCount++;
#endif
                return true;
            }
        }

        return false;
    }
    return this->getNext();
}

void SortFilterIterator::prepareIndexedAccess()
{
    this->start = this->end;
    this->startFilterIndex = 0;
}

void SortFilterIterator::iterateValue(const Selection& selection, uint64_t value)
{
    this->index = &database.getSortIndex(selection.relation, selection.column);
    this->start = toPtr(*this->index, std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                                       [](const RowEntry& entry, uint64_t val) {
                                                           return entry.value < val;
                                                       }));
    this->end = toPtr(*this->index, std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
                                                     [](uint64_t val, const RowEntry& entry) {
                                                         return val < entry.value;
                                                     }));
    this->start--;
    this->originalStart = this->start;
}

void SortFilterIterator::save()
{
    this->startSaved = this->start;
}

void SortFilterIterator::restore()
{
    this->start = this->startSaved;
    this->rowIndex = this->start->row;
}

void SortFilterIterator::prepareSortedAccess(const Selection& selection)
{
    // check if we can sort on a filtered column
    int i = 0;
    for (; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
        if (filter.selection == selection)
        {
            this->index = &database.getSortIndex(selection.relation, selection.column);
            this->sortFilter = filter;
            this->sortSelection = selection;
            createIterators(filter, &this->start, &this->end);
            this->start--;
            this->originalStart = this->start;
            break;
        }
    }

    if (i < this->filterSize)
    {
        std::swap(this->filters[0], this->filters[i]);
        this->startFilterIndex = 1;
        return;
    }

    this->index = &database.getSortIndex(selection.relation, selection.column);
    this->start = this->index->data.data() - 1;
    this->originalStart = this->start;
    this->end = this->index->data.data() + this->index->data.size();

    this->startFilterIndex = 0;
}

int64_t SortFilterIterator::predictSize()
{
    return (this->end - this->originalStart) - 1; // - 1 because originalStart is one before the first element
}

std::unique_ptr<Iterator> SortFilterIterator::createIndexedIterator()
{
    return std::make_unique<SortFilterIterator>(this->relation, this->binding, this->filters,
                                                this->originalStart,
                                                this->end);
}

void SortFilterIterator::split(std::vector<std::unique_ptr<Iterator>>& groups, size_t count)
{
    auto size = (this->end - this->originalStart) - 1;
    auto chunkSize = static_cast<size_t>(std::ceil(size / (double) count));
    RowEntry* iter = this->originalStart + 1;

    size_t left = size;
    while (left > 0)
    {
        size_t chunk = std::min(chunkSize, left);
        left -= chunk;

        groups.push_back(std::make_unique<SortFilterIterator>(
                this->relation,
                this->binding,
                this->filters,
                iter - 1,
                iter + chunk
        ));
        iter += chunk;
    }

    assert(iter == this->end);
}
