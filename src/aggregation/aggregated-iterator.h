#pragma once

#include "../relation/wrapper-iterator.h"

template <bool IS_GROUPBY_SUMMED>
class AggregatedIterator: public WrapperIterator
{
public:
    AggregatedIterator(Iterator* inner, Selection groupBy, std::vector<Selection> sumSelections);

    bool getNext() override;
    bool skipSameValue(const Selection& selection) final;

    bool hasSelection(const Selection& selection) final;
    uint32_t getColumnForSelection(const Selection& selection) final;
    uint64_t getValue(const Selection& selection) final;

    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    uint64_t getColumn(uint32_t column) final;
    int32_t getColumnCount() final;

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) final;
    void fillRow(uint64_t* row, const std::vector<Selection>& selections) final;

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                               HashTable& hashTable) override;

    void prepareSortedAccess(const Selection& selection) final;
    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override;

    void split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count) final;
    void splitToBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                               std::vector<uint64_t>& bounds, size_t count) final;
    void splitUsingBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                                  const std::vector<uint64_t>& bounds) final;

    void dumpPlan(std::ostream& ss) override;

protected:
    uint64_t& value()
    {
        return this->values[0];
    }
    uint64_t& count()
    {
        return this->values[1];
    }
    uint64_t& sum(int i)
    {
        return this->values[i + 2];
    }

    std::vector<uint64_t> values;

    Selection groupBy;
    Selection countSelection;
    std::vector<Selection> sumSelections;
};

template class AggregatedIterator<false>;
template class AggregatedIterator<true>;
