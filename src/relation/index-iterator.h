#pragma once

#include <algorithm>
#include "filter-iterator.h"
#include "../index/index.h"
#include "../database.h"
#include "../index/index-builder.h"

#include <cmath>
#include <omp.h>

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
                  Selection iteratedSelection, int startFilterIndex)
            : FilterIterator(relation, binding, filters), start(start), end(end),
              iteratedSelection(iteratedSelection)
    {
        this->originalStart = start;
        this->startFilterIndex = startFilterIndex;
    }

    virtual Index* getIndex(uint32_t relation, uint32_t column) = 0;
    Entry* lowerBound(uint64_t value, Index* index)
    {
        return index->lowerBound(value);
    }
    Entry* upperBound(uint64_t value, Index* index)
    {
        return index->upperBound(value);
    }
    virtual Entry* findNextValue(Entry* iter, Entry* end, const Selection& selection, uint64_t value) = 0;
    virtual int64_t count(Entry* from, Entry* to) = 0;
    virtual uint64_t getValueForIter(Entry* iter, const Selection& selection) = 0;

    void createIterators(const Filter& filter, Entry** start, Entry** end)
    {
        auto index = this->getIndex(filter.selection.relation, filter.selection.column);

        Entry* last = index->end;
        uint64_t value = filter.value;
        if (filter.oper == '<')
        {
            *start = index->begin;
            *end = this->lowerBound(value, index);
        }
        else if (filter.oper == '>')
        {
            *start = this->upperBound(value, index);
            *end = last;
        }
        else if (filter.oper == '=')
        {
            *start = this->lowerBound(value, index);
            *end = this->upperBound(value, index);
        }
        else
        {
            *start = this->upperBound(value, index);
            *end = this->lowerBound(filter.valueMax, index);
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
        if (EXPECT(this->iterateValueSelection == selection))
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

        this->start = this->lowerBound(value, this->index);
        this->end = this->findNextValue(this->start, this->index->end, selection, value);
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
            return this->count(this->originalStart, this->end) - 1;
        }
        return this->relation->getRowCount();
    }

    void split(std::vector<std::unique_ptr<Iterator>>& container,
               std::vector<Iterator*>& groups,
               size_t count) final
    {
        auto size = this->count(this->originalStart, this->end) - 1;
        auto chunkSize = static_cast<size_t>(std::ceil(size / (double) count));
        Entry* iter = this->index->move(this->originalStart, 1);

        size_t left = size;
        while (left > 0)
        {
            size_t chunk = std::min(chunkSize, left);
            left -= chunk;

            container.push_back(std::make_unique<Child>(
                    this->relation,
                    this->binding,
                    this->filters,
                    this->index->move(iter, -1),
                    this->index->move(iter, chunk),
                    this->iteratedSelection,
                    this->startFilterIndex
            ));
            groups.push_back(container.back().get());
            iter = this->index->move(iter, chunk);
        }

        assert(iter == this->end);
    }

    void splitToBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                       std::vector<uint64_t>& bounds, size_t count) final
    {
        assert(this->index != nullptr);

        auto size = this->count(this->originalStart, this->end) - 1;
        auto chunkSize = static_cast<size_t>(std::ceil(size / (double) count));
        Entry* iter = this->index->move(this->originalStart, 1);

        bool first = true;
        size_t left = size;

        while (left > 0 && iter < this->end)
        {
            size_t chunk = std::min(chunkSize, left);
            left -= chunk;

            if (!first)
            {
                bounds.push_back(this->getValueForIter(iter, this->iteratedSelection));
            }

            auto end = this->index->move(iter, chunk);
            if (end < this->index->end)
            {
                auto alignedEnd = this->findNextValue(end, this->end, this->iteratedSelection,
                                                      this->getValueForIter(end, this->iteratedSelection));
                left -= this->count(end, alignedEnd);
                end = alignedEnd;
            }

            container.push_back(std::make_unique<Child>(
                    this->relation,
                    this->binding,
                    this->filters,
                    this->index->move(iter, -1),
                    end,
                    this->iteratedSelection,
                    this->startFilterIndex
            ));
            groups.push_back(container.back().get());
            iter = end;
            if (first) first = false;
        }

        if (groups.empty())
        {
            container.push_back(std::make_unique<Child>(
                    this->relation,
                    this->binding,
                    this->filters,
                    this->end,
                    this->end,
                    this->iteratedSelection,
                    this->startFilterIndex
            ));
            groups.push_back(container.back().get());
        }

        assert(iter == this->end);
    }

    void splitUsingBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                          const std::vector<uint64_t>& bounds) final
    {
        Entry* iter = this->index->move(this->originalStart, 1);
        for (int i = 0; i < static_cast<int32_t>(bounds.size()); i++)
        {
            Entry* end = this->lowerBound(bounds[i], this->index);
            Entry* start = this->index->dec(iter);
            if (end < iter)
            {
                end = iter;
            }
            container.push_back(std::make_unique<Child>(
                    this->relation,
                    this->binding,
                    this->filters,
                    start,
                    end,
                    this->iteratedSelection,
                    this->startFilterIndex
            ));
            groups.push_back(container.back().get());
            iter = end;
        }

        container.push_back(std::make_unique<Child>(
                this->relation,
                this->binding,
                this->filters,
                this->index->move(iter, -1),
                this->end,
                this->iteratedSelection,
                this->startFilterIndex
        ));
        groups.push_back(container.back().get());

        assert(groups.size() == bounds.size() + 1);
    }

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) final
    {
        auto resultCount = static_cast<int32_t>(results.size());
        while (this->getNext())
        {
            for (int i = 0; i < resultCount; i++)
            {
                results[i] += this->getColumn(columnIds[i]);
            }

            count++;
        }
    }

    size_t iterateCount() final
    {
        if (this->startFilterIndex >= this->filterSize)
        {
            size_t count = this->index->count(this->start, this->end);
            if (count == 0) return count;
            return count - 1;
        }
        else return Iterator::iterateCount();
    }

    bool isImpossible() final
    {
        for (auto& filter: this->filters)
        {
            if (database.hasIndexedIterator(filter.selection))
            {
                Entry* from;
                Entry* to;
                this->createIterators(filter, &from, &to);
                if (from >= to)
                {
                    return true;
                }
            }
        }

        return false;
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
