#include "aggregator.h"

Aggregator::Aggregator(Iterator* inner, const Query& query)
        : WrapperIterator(inner), query(query)
{

}

void Aggregator::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                         const std::vector<Selection>& selections, size_t& count)
{
    std::vector<uint32_t> childBindings;
    this->inner->fillBindings(childBindings);

    // remove duplicates
    std::sort(childBindings.begin(), childBindings.end());
    childBindings.erase(std::unique(childBindings.begin(), childBindings.end()), childBindings.end());

    std::vector<uint32_t> columns;
    for (auto& sel: selections)
    {
        columns.push_back(static_cast<uint32_t>(this->inner->getColumnForSelection(sel)));
    }
    uint32_t countColumns[4];
    for (auto& binding: childBindings)
    {
        countColumns[binding] = static_cast<uint32_t>(
                                       this->inner->getColumnForSelection(Selection::aggregatedCount(binding)));
    }

    std::vector<uint32_t> multiplyColumns(selections.size(), 4);
    for (auto& join: this->query.joins)
    {
        for (auto& sel: join[0].selections)
        {
            for (int i = 0; i < static_cast<int32_t>(selections.size()); i++)
            {
                if (sel == selections[i])
                {
                    multiplyColumns[i] = sel.binding;
                }
            }
        }
    }

    uint32_t counts[5];
    counts[4] = 1;
    size_t totalCount;
    while (this->inner->getNext())
    {
        totalCount = 1;
        for (auto& kv: childBindings)
        {
            counts[kv] = static_cast<uint32_t>(this->inner->getColumn(countColumns[kv]));
            totalCount *= counts[kv];
        }

        for (int i = 0; i < static_cast<int32_t>(columns.size()); i++)
        {
            uint64_t value = this->getColumn(columns[i]) * counts[multiplyColumns[i]];
            value *= totalCount;
            value /= counts[selections[i].binding];
            results[i] += value;
        }

        count++; // TODO
    }
}

void Aggregator::split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count)
{
    std::vector<Iterator*> subGroups;
    this->inner->split(container, subGroups, count);
    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<Aggregator>(group, this->query));
        groups.push_back(container.back().get());
    }
}
