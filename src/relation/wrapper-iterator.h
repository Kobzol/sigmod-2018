#pragma once

#include "iterator.h"

class WrapperIterator: public Iterator
{
public:
    explicit WrapperIterator(Iterator* inner): inner(inner)
    {
        
    }

    bool getNext() override
    {
        return this->inner->getNext();
    }
    bool skipSameValue(const Selection& selection) override
    {
        return this->inner->skipSameValue(selection);
    }

    int32_t getColumnCount() override
    {
        return this->inner->getColumnCount();
    }

    int32_t getRowCount() override
    {
        return this->inner->getRowCount();
    }

    uint64_t getValue(const Selection& selection) override
    {
        return this->inner->getValue(selection);
    }

    uint64_t getColumn(uint32_t column) override
    {
        return this->inner->getColumn(column);
    }

    void prepareIndexedAccess(const Selection& selection) override
    {
        this->inner->prepareIndexedAccess(selection);
    }

    void prepareSortedAccess(const Selection& selection) override
    {
        this->inner->prepareSortedAccess(selection);
    }

    bool isSortedOn(const Selection& selection) override
    {
        return this->inner->isSortedOn(selection);
    }

    bool isJoin() override
    {
        return this->inner->isJoin();
    }

    void iterateValue(const Selection& selection, uint64_t value) override
    {
        this->inner->iterateValue(selection, value);
    }

    bool getValueMaybe(const Selection& selection, uint64_t& value) override
    {
        return this->inner->getValueMaybe(selection, value);
    }

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) override
    {
        this->inner->fillRow(row, selections);
    }

    uint32_t getColumnForSelection(const Selection& selection) override
    {
        return this->inner->getColumnForSelection(selection);
    }

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) override
    {
        this->inner->requireSelections(selections);
    }

    bool hasSelection(const Selection& selection) override
    {
        return this->inner->hasSelection(selection);
    }

    uint32_t getFilterReduction() override
    {
        return this->inner->getFilterReduction();
    }

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                               HashTable& hashTable) override
    {
        this->inner->fillHashTable(hashSelection, selections, hashTable);
    }

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                     const std::vector<Selection>& selections, size_t& count) override
    {
        this->inner->sumRows(results, columnIds, selections, count);
    }

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override
    {
        return this->inner->createIndexedIterator(container, selection);
    }

    void save() override
    {
        this->inner->save();
    }

    void restore() override
    {
        this->inner->restore();
    }

    void fillBindings(std::vector<uint32_t>& bindings) override
    {
        this->inner->fillBindings(bindings);
    }

    void dumpPlan(std::ostream& ss) override
    {
        this->inner->dumpPlan(ss);
    }

    int64_t predictSize() override
    {
        return this->inner->predictSize();
    }

    void assignJoinSize(Database& database) override
    {
        this->inner->assignJoinSize(database);
    }

    void split(std::vector<std::unique_ptr<Iterator>>& groups, size_t count) override
    {
        this->inner->split(groups, count);
    }

    bool hasBinding(uint32_t binding) override
    {
        return this->inner->hasBinding(binding);
    }

    Iterator* inner;
};
