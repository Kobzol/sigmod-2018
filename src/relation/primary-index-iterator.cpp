#include "primary-index-iterator.h"
#include "sort-index-iterator.h"

PrimaryIndexIterator::PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding,
                                           const std::vector<Filter>& filters)
        : IndexIterator(relation, binding, filters)
{
    if (!this->filters.empty())
    {
        this->index = this->getIndex(this->iteratedSelection.relation, this->iteratedSelection.column);
        if (this->index != nullptr)
        {
            this->createIterators(this->filters[0], &this->start, &this->end);
        }
    }

    this->start = PrimaryIndex::move(*this->relation, this->start, -1);
    this->originalStart = this->start;
}

PrimaryIndexIterator::PrimaryIndexIterator(ColumnRelation* relation, uint32_t binding,
                                           const std::vector<Filter>& filters,
                                           PrimaryRowEntry* start, PrimaryRowEntry* end,
                                           Selection iteratedSelection, int startFilterIndex)
        : IndexIterator(relation, binding, filters, start, end, iteratedSelection, startFilterIndex)
{
    if (iteratedSelection.binding != 100)
    {
        this->index = this->getIndex(this->iteratedSelection.relation, this->iteratedSelection.column);
    }
}

bool PrimaryIndexIterator::getNext()
{
    this->start = this->index->inc(this->start);

    for (; this->start < this->end; this->start = this->index->inc(this->start))
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
#ifdef COMPILE_FILTERS
        if (!filter.evaluator(this->start->row[filter.selection.column])) return false;
#else
        if (!passesFilter(filter, this->start->row[filter.selection.column])) return false;
#endif
    }

    return true;
}

PrimaryIndex* PrimaryIndexIterator::getIndex(uint32_t relation, uint32_t column)
{
    return database.getPrimaryIndex(relation, column);
}

bool PrimaryIndexIterator::skipSameValue(const Selection& selection)
{
    if (selection == this->iteratedSelection)
    {
        uint64_t value = this->getColumn(this->iteratedSelection.column);
        for (; this->start < this->end; this->start = this->index->inc(this->start))
        {
            uint64_t newValue = this->getColumn(this->iteratedSelection.column);
            if (value == newValue) continue;
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
    return this->getNext();
}
bool PrimaryIndexIterator::skipTo(const Selection& selection, uint64_t value)
{
    this->start = this->index->inc(this->start);

    for (; this->start < this->end; this->start = this->index->inc(this->start))
    {
        if (this->getValue(selection) >= value && this->passesFilters())
        {
#ifdef COLLECT_JOIN_SIZE
            this->rowCount++;
#endif
            return true;
        }
    }

    return false;
}

std::unique_ptr<Iterator> PrimaryIndexIterator::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                                      const Selection& selection)
{
    if (database.getPrimaryIndex(selection.relation, selection.column) != nullptr)
    {
        return std::make_unique<PrimaryIndexIterator>(this->relation, this->binding, this->filters,
                                                      this->originalStart,
                                                      this->end,
                                                      this->iteratedSelection,
                                                      this->startFilterIndex);
    }
    else return std::make_unique<SortIndexIterator>(this->relation, this->binding, this->filters);
}

PrimaryRowEntry* PrimaryIndexIterator::findNextValue(PrimaryRowEntry* iter,
                                                     PrimaryRowEntry* end,
                                                     const Selection& selection,
                                                     uint64_t value)
{
    auto ptr = iter;
    while (ptr < end && ptr->row[selection.column] == value)
    {
        ptr = this->index->inc(ptr);
    }

    return ptr;
}

int64_t PrimaryIndexIterator::count(PrimaryRowEntry* from, PrimaryRowEntry* to)
{
    return PrimaryIndex::count(*this->relation, from, to);
}

uint64_t PrimaryIndexIterator::getValueForIter(PrimaryRowEntry* iter, const Selection& selection)
{
    return iter->row[selection.column];
}
