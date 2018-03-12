#include "sort-filter-iterator.h"
#include "../database.h"
#include "../index/sort-index.h"

#include <algorithm>
#include <cassert>

SortFilterIterator::SortFilterIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters)
        : FilterIterator(relation, binding, filters)
{
    if (!filters.empty())
    {
        this->sortFilter = this->filters[0];
        this->startFilterIndex = 1;
        this->index = &database.getSortIndex(this->sortFilter.selection.relation, this->sortFilter.selection.column);

        uint64_t value = this->sortFilter.value;

        RowEntry* last = this->index->data.data() + this->index->data.size();
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
        this->start--;
    }
}

bool SortFilterIterator::getNext()
{
    this->start++;

    for (; this->start < this->end; this->start++)
    {
        this->rowIndex = this->start->row;
        if (passesFilters())
        {
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
            if (this->relation->getValue(this->rowIndex, this->sortFilter.selection.column) != value) return true;
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
}
