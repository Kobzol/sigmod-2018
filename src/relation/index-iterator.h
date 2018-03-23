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
            this->startFilterIndex = 1;
            this->iteratedSelection = this->filters[0].selection;
        }
    }
    IndexIterator(ColumnRelation* relation, uint32_t binding,
                  const std::vector<Filter>& filters, Entry* start, Entry* end,
                  Selection iteratedSelection)
            : FilterIterator(relation, binding, filters), start(start), end(end), iteratedSelection(iteratedSelection)
    {
        this->originalStart = start;
    }

    virtual Index* getIndex(uint32_t relation, uint32_t column) = 0;
    Entry* lowerBound(uint64_t value)
    {
        return this->index->lowerBound(value);
    }
    Entry* upperBound(uint64_t value)
    {
        return this->index->upperBound(value);
    }
    virtual Entry* findNextValue(const Selection& selection, uint64_t value)
    {
        return this->upperBound(value);
    }
    virtual int64_t count() = 0;

    void createIterators(const Filter& filter, Entry** start, Entry** end)
    {
        auto index = this->getIndex(filter.selection.relation, filter.selection.column);

        Entry* last = index->end;
        uint64_t value = filter.value;
        if (filter.oper == '<')
        {
            *start = index->begin;
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

    void prepareIndexedAccess(const Selection& selection) final
    {
        this->start = this->end;
        this->startFilterIndex = 0;
        this->index = this->getIndex(selection.relation, selection.column);
    }

    void iterateValue(const Selection& selection, uint64_t value) final
    {
#ifdef CACHE_ITERATE_VALUE
        if (this->iterateValueSelection == selection)
        {
            if (this->iteratedValue == value)
            {
                this->start = this->originalStart;
                return;
            }
        }
        else
        {
            this->iteratedSelection = selection;
            this->iterateValueSelection = selection;
            this->index = this->getIndex(selection.relation, selection.column);
        }

        this->iteratedValue = value;
#else
        this->index = this->getIndex(selection.relation, selection.column);
#endif

        this->start = this->lowerBound(value);
        this->end = this->findNextValue(selection, value);//this->upperBound(value);
        this->start = this->index->dec(this->start);
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
        if (selection == this->iteratedSelection) return;

        // check if we can sort on a filtered column
        int i = 0;
        for (; i < this->filterSize; i++)
        {
            auto& filter = this->filters[i];
            if (filter.selection == selection)
            {
                auto index = this->getIndex(selection.relation, selection.column);
                if (index != nullptr)
                {
                    this->index = index;
                    this->iteratedSelection = selection;
                    this->createIterators(filter, &this->start, &this->end);
                    this->start = this->index->dec(this->start);
                    this->originalStart = this->start;
                    break;
                }
            }
        }

        if (i < this->filterSize)
        {
            std::swap(this->filters[0], this->filters[i]);
            this->startFilterIndex = 1;
            return;
        }

        this->index = this->getIndex(selection.relation, selection.column);
        assert(this->index != nullptr);
        this->start = this->index->dec(this->index->begin);
        this->originalStart = this->start;
        this->end = this->index->end;
        this->iteratedSelection = selection;

        this->startFilterIndex = 0;
    }

    int64_t predictSize() final
    {
        if (!this->filters.empty())
        {
            // - 1 because originalStart is one before the first element
            return this->count();
        }
        return this->relation->getRowCount();
    }

    void split(std::vector<std::unique_ptr<Iterator>>& groups, size_t count) final
    {
        auto size = this->count();
        auto chunkSize = static_cast<size_t>(std::ceil(size / (double) count));
        Entry* iter = this->index->move(this->originalStart, 1);

        size_t left = size;
        while (left > 0)
        {
            size_t chunk = std::min(chunkSize, left);
            left -= chunk;

            groups.push_back(std::make_unique<Child>(
                    this->relation,
                    this->binding,
                    this->filters,
                    this->index->move(iter, -1),
                    this->index->move(iter, chunk),
                    this->iteratedSelection
            ));
            iter = this->index->move(iter, chunk);
        }

        assert(iter == this->end);
    }

    Index* index = nullptr;

    Entry* start = nullptr;
    Entry* end = nullptr;

    Entry* startSaved;
    Entry* originalStart;

    Selection iteratedSelection{100, 100, 100};
    Selection iterateValueSelection{100, 100, 100};
    uint64_t iteratedValue;
};
