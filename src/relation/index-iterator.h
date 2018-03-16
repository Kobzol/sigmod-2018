#pragma once

#include <algorithm>
#include "filter-iterator.h"
#include "../index/index.h"
#include "../database.h"

#include <cmath>

template <typename Entry, typename Index, typename Child>
class IndexIterator: public FilterIterator
{
public:
    IndexIterator(ColumnRelation* relation, uint32_t binding, const std::vector<Filter>& filters)
            : FilterIterator(relation, binding, filters)
    {
        if (!this->filters.empty())
        {
            this->sortFilter = this->filters[0];
            this->startFilterIndex = 1;
            this->sortSelection = this->sortFilter.selection;
        }
    }
    IndexIterator(ColumnRelation* relation, uint32_t binding,
                                           const std::vector<Filter>& filters, Entry* start, Entry* end)
            : FilterIterator(relation, binding, filters), start(start), end(end)
    {
        this->originalStart = start;
    }

    virtual Index* getIndex(uint32_t relation, uint32_t column) = 0;
    virtual Entry* lowerBound(uint64_t value) = 0;
    virtual Entry* upperBound(uint64_t value) = 0;

    Entry* toPtr(Index& index, const typename std::vector<Entry>::iterator& iterator)
    {
        return index.data.data() + (iterator - index.data.begin());
    }

    void createIterators(const Filter& filter, Entry** start, Entry** end)
    {
        auto index = this->getIndex(filter.selection.relation, filter.selection.column);

        Entry* last = index->data.data() + index->data.size();
        uint64_t value = filter.value;
        if (filter.oper == '<')
        {
            *start = index->data.data();
            *end = this->lowerBound(value);
        }
        else if (filter.oper == '>')
        {
            *start = this->upperBound(value);
            *end = last;
        }
        else
        {
            *start = this->lowerBound(value);
            *end = this->upperBound(value);
        }
    }

    void prepareIndexedAccess() final
    {
        this->start = this->end;
        this->startFilterIndex = 0;
    }

    void iterateValue(const Selection& selection, uint64_t value) final
    {
        this->index = this->getIndex(selection.relation, selection.column);
        this->start = this->lowerBound(value);
        this->end = this->upperBound(value);
        this->start--;
        this->originalStart = this->start;
    }

    void save() final
    {
        this->startSaved = this->start;
    }

    void restore() override
    {
        this->start = this->startSaved;
    }

    void prepareSortedAccess(const Selection& selection) final
    {
        // check if we can sort on a filtered column
        int i = 0;
        for (; i < this->filterSize; i++)
        {
            auto& filter = this->filters[i];
            if (filter.selection == selection)
            {
                this->index = this->getIndex(selection.relation, selection.column);
                this->sortFilter = filter;
                this->sortSelection = selection;
                this->createIterators(filter, &this->start, &this->end);
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

        this->index = this->getIndex(selection.relation, selection.column);
        this->start = this->index->data.data() - 1;
        this->originalStart = this->start;
        this->end = this->index->data.data() + this->index->data.size();

        this->startFilterIndex = 0;
    }

    int64_t predictSize() final
    {
        return (this->end - this->originalStart) - 1; // - 1 because originalStart is one before the first element
    }

    void split(std::vector<std::unique_ptr<Iterator>>& groups, size_t count) final
    {
        auto size = (this->end - this->originalStart) - 1;
        auto chunkSize = static_cast<size_t>(std::ceil(size / (double) count));
        Entry* iter = this->originalStart + 1;

        size_t left = size;
        while (left > 0)
        {
            size_t chunk = std::min(chunkSize, left);
            left -= chunk;

            groups.push_back(std::make_unique<Child>(
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

    std::unique_ptr<Iterator> createIndexedIterator() final
    {
        return std::make_unique<Child>(this->relation, this->binding, this->filters,
                                       this->originalStart,
                                       this->end);
    }

    Index* index;
    Filter sortFilter;
    Selection sortSelection;

    Entry* start = nullptr;
    Entry* end = nullptr;

    Entry* startSaved;
    Entry* originalStart;
};
