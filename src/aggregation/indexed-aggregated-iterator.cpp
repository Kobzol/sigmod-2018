#include "indexed-aggregated-iterator.h"
#include "../database.h"

AggregateRow* toPtr(AggregateIndex& index, const std::vector<AggregateRow>::iterator& iterator)
{
    return index.data.data() + (iterator - index.data.begin());
}

template<bool IS_GROUPBY_SUMMED>
IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::IndexedAggregatedIterator(Iterator* inner, const Selection& groupBy,
                                                                        const std::vector<Selection>& sumSelections)
        : AggregatedIterator<IS_GROUPBY_SUMMED>(inner, groupBy, sumSelections)
{
    this->index = database.getAggregateIndex(groupBy.relation, groupBy.column);
    if (this->index != nullptr)
    {
        this->start = this->index->data.data() - 1;
        this->originalStart = this->start;
        this->end = this->index->data.data() + this->index->data.size();
        assert(this->start + 1 < this->end);
    }
}

template<bool IS_GROUPBY_SUMMED>
bool IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::getNext()
{
    this->start++;
    if (this->start >= this->end) return false;

    this->value() = this->start->value;
    this->count() = this->start->count;

    assert(this->count() > 0);

    for (int i = 0; i < static_cast<int32_t>(this->sumSelections.size()); i++)
    {
        this->sum(i) = this->start->sums[this->sumSelections[i].column];
    }

    return true;
}

template<bool IS_GROUPBY_SUMMED>
std::unique_ptr<Iterator> IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::createIndexedIterator(
        std::vector<std::unique_ptr<Iterator>>& container, const Selection& selection)
{
    return std::make_unique<IndexedAggregatedIterator>(this->inner, this->groupBy, this->sumSelections);
}

template<bool IS_GROUPBY_SUMMED>
void IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::dumpPlan(std::ostream& ss)
{
    ss << "IAI(";
    this->inner->dumpPlan(ss);
    ss << ")";
}

template<bool IS_GROUPBY_SUMMED>
void IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::iterateValue(const Selection& selection, uint64_t value)
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
    else this->iterateValueSelection = selection;
    this->iteratedValue = value;
#endif

    this->start = toPtr(*this->index, std::lower_bound(this->index->data.begin(), this->index->data.end(), value,
                                                [this](const AggregateRow& entry, uint64_t val) {
                                                    return entry.value < val;
                                                })) - 1;
    this->end = toPtr(*this->index, std::upper_bound(this->index->data.begin(), this->index->data.end(), value,
                                                [this](uint64_t val, const AggregateRow& entry) {
                                                    return val < entry.value;
                                                }));
    this->originalStart = this->start;
}

template<bool IS_GROUPBY_SUMMED>
void IndexedAggregatedIterator<IS_GROUPBY_SUMMED>::prepareIndexedAccess(const Selection& selection)
{
    this->start = this->end;
}
