#include "primary-index-iterator.h"

PrimaryIndexIterator::PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding,
                                           const std::vector<Filter>& filters)
        : IndexIterator(relation, binding, filters)
{
    if (!this->filters.empty())
    {
        this->index = this->getIndex(this->sortFilter.selection.relation, this->sortFilter.selection.column);
        this->createIterators(this->sortFilter, &this->start, &this->end);
    }
}

PrimaryIndexIterator::PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters,
                                       PrimaryRowEntry* start, PrimaryRowEntry* end)
        : IndexIterator(relation, binding, filters, start, end)
{

}

bool PrimaryIndexIterator::getNext()
{
    this->start++;

    for (; this->start < this->end; this->start++)
    {
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

uint64_t PrimaryIndexIterator::getValue(const Selection& selection)
{
    return this->start->row[selection.column];
}

uint64_t PrimaryIndexIterator::getColumn(uint32_t column)
{
    return this->start->row[column];
}

bool PrimaryIndexIterator::getValueMaybe(const Selection& selection, uint64_t& value)
{
    if (this->binding == selection.binding)
    {
        value = this->start->row[selection.column];
        return true;
    }

    return false;
}

bool PrimaryIndexIterator::passesFilters()
{
    for (int i = this->startFilterIndex; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
        if (!passesFilter(filter, this->start->row[filter.selection.column])) return false;
    }

    return true;
}

PrimaryIndex* PrimaryIndexIterator::getIndex(uint32_t relation, uint32_t column)
{
    return &database.getPrimaryIndex(relation, column);
}

PrimaryRowEntry* PrimaryIndexIterator::lowerBound(uint64_t value)
{
    return toPtr(*this->index, std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                                [this](const PrimaryRowEntry& entry, uint64_t val) {
                                                    return entry.row[this->index->column] < val;
                                                }));
}

PrimaryRowEntry* PrimaryIndexIterator::upperBound(uint64_t value)
{
    return toPtr(*this->index, std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
                                                [this](uint64_t val, const PrimaryRowEntry& entry) {
                                                    return val < entry.row[this->index->column];
                                                }));
}

bool PrimaryIndexIterator::skipSameValue(const Selection& selection)
{
    if (selection == this->sortFilter.selection)
    {
        uint64_t value = this->relation->getValue(this->rowIndex, this->sortFilter.selection.column);
        for (; this->start < this->end; this->start++)
        {
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
