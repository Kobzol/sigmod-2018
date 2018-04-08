#include "filter-iterator.h"
#include "hash-filter-iterator.h"
#include "sort-index-iterator.h"
#include "primary-index-iterator.h"
#include "../database.h"

#include <cassert>
#include <unordered_set>

FilterIterator::FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters)
        : ColumnRelationIterator(relation, binding), filters(std::move(filters))
{
    this->filterSize = static_cast<int>(this->filters.size());
}
FilterIterator::FilterIterator(ColumnRelation* relation, uint32_t binding, std::vector<Filter> filters,
                               int start, int end)
        : ColumnRelationIterator(relation, binding, start, end), filters(std::move(filters))
{
    this->filterSize = static_cast<int>(this->filters.size());
}

bool FilterIterator::getNext()
{
    this->rowIndex++;

    while (this->rowIndex < this->end && !this->passesFilters())
    {
        this->rowIndex++;
    }

#ifdef COLLECT_JOIN_SIZE
    this->rowCount++;
#endif

    return this->rowIndex < this->end;
}

bool FilterIterator::passesFilters()
{
    for (int i = this->startFilterIndex; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
#ifdef COMPILE_FILTERS
        if (!filter.evaluator(this->relation->getValue(filter.selection, this->rowIndex))) return false;
#else
        if (!passesFilter(filter, this->relation->getValue(filter.selection, this->rowIndex))) return false;
#endif
    }

    return true;
}

std::unique_ptr<Iterator> FilterIterator::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                                const Selection& selection)
{
    return database.createIndexedIterator(selection, this->filters);
}

int64_t FilterIterator::predictSize()
{
#ifdef USE_HISTOGRAM
    return database.histograms[this->filters[0].selection.relation].estimateResult(this->filters[0]);
#else
    return (this->end - (this->rowIndex + 1));
#endif
}

void FilterIterator::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                             const std::vector<Selection>& selections, size_t& count)
{
    std::unordered_set<uint32_t> bindings;
    for (auto& sel: selections)
    {
        bindings.insert(sel.binding);
    }

    size_t localCount = 0;
    auto selectionSize = static_cast<int>(selections.size());
    this->startFilterIndex = 0;

    if (bindings.size() == 1 && selectionSize > 1 && this->relation->transposed != nullptr)
    {
        this->rowIndex++;
        while (this->rowIndex < this->end)
        {
            if (FilterIterator::passesFiltersTransposed())
            {
                for (int i = 0; i < selectionSize; i++)
                {
                    results[i] += this->relation->getValueTransposed(this->rowIndex, columnIds[i]);
                }
#ifdef COLLECT_JOIN_SIZE
                this->rowCount++;
#endif
                localCount++;
            }

            this->rowIndex++;
        }
    }
    else
    {
        int start = this->rowIndex;
        for (int i = 0; i < static_cast<int32_t>(results.size()); i++)
        {
            this->rowIndex = start;
            this->rowIndex++;

            while (this->rowIndex < this->end)
            {
                if (FilterIterator::passesFilters())
                {
                    results[i] += FilterIterator::getColumn(columnIds[i]);
                    localCount++;
#ifdef COLLECT_JOIN_SIZE
                    this->rowCount++;
#endif
                }

                this->rowIndex++;
            }

            if (localCount == 0) break;
            else if (i < static_cast<int32_t>(results.size()) - 1) localCount = 0;
        }
    }

    count = localCount;
}

bool FilterIterator::passesFiltersTransposed()
{
    for (int i = this->startFilterIndex; i < this->filterSize; i++)
    {
        auto& filter = this->filters[i];
#ifdef COMPILE_FILTERS
        if (!filter.evaluator(this->relation->getValueTransposed(this->rowIndex, filter.selection.column)))
            return false;
#else
        if (!passesFilter(filter, this->relation->getValueTransposed(this->rowIndex, filter.selection.column)))
            return false;
#endif
    }

    return true;
}
