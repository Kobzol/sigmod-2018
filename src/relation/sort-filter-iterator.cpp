#include "sort-filter-iterator.h"
#include "../database.h"
#include "../index/sort-index.h"

#include <algorithm>
#include <cmath>

SortFilterIterator::SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters)
        : FilterIterator(relation, binding, filters)
{
    if (!filters.empty())
    {
        this->sortFilter = this->filters[0];
        this->startFilterIndex = 1;
        this->sortSelection = this->sortFilter.selection;
        this->index = &database.getSortIndex(this->sortFilter.selection.relation, this->sortFilter.selection.column);

        RowEntry* last = this->index->data.data() + this->index->data.size();
        uint64_t value = this->sortFilter.value;
        if (this->sortFilter.oper == '<')
        {
            this->start = this->index->data.data();
            this->end = this->toPtr(std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                                     [](const RowEntry& entry, uint64_t val) {
                                                         return entry.value < val;
                                                     }));
        }
        else if (this->sortFilter.oper == '>')
        {
            this->start = this->toPtr(std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
                                                       [](uint64_t val, const RowEntry& entry) {
                                                           return val < entry.value;
                                                       }));
            this->end = last;
        }
        else
        {
            this->start = this->toPtr(std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                                       [](const RowEntry& entry, uint64_t val) {
                                                           return entry.value < val;
                                                       }));
            this->end = this->toPtr(std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
                                                     [](uint64_t val, const RowEntry& entry) {
                                                         return val < entry.value;
                                                     }));
        }
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
        if (passesFilters())
        {
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
            return true;
        }
    }

    return false;
}

RowEntry* SortFilterIterator::toPtr(const std::vector<RowEntry>::iterator& iterator) const
{
    return this->index->data.data() + (iterator - this->index->data.begin());
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
    this->start = this->toPtr(std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                               [](const RowEntry& entry, uint64_t val) {
                                                   return entry.value < val;
                                               }));
    this->end = this->toPtr(std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
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
    this->index = &database.getSortIndex(selection.relation, selection.column);
    this->start = this->index->data.data() - 1;
    this->originalStart = this->start;
    this->end = this->index->data.data() + this->index->data.size();
    this->sortSelection = selection;

    this->startFilterIndex = 0;
}

int64_t SortFilterIterator::predictSize()
{
    return (this->end - this->originalStart) - 1; // + 1 because originalStart is one before the first element
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
