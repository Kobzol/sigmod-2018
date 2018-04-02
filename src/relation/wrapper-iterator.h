#pragma once

#include "iterator.h"
#include "../timer.h"

#include <algorithm>
#include <atomic>
#include <utility>

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

    void split(std::vector<std::unique_ptr<Iterator>>& container,
               std::vector<Iterator*>& groups,
               size_t count) override
    {
        this->inner->split(container, groups, count);
    }

    bool hasBinding(uint32_t binding) override
    {
        return this->inner->hasBinding(binding);
    }

    bool isImpossible() override
    {
        return this->inner->isImpossible();
    }

    bool hasIndexJoin() override
    {
        return this->inner->hasIndexJoin();
    }

    Iterator* inner;
};

class MultiWrapperIterator: public Iterator
{
public:
    explicit MultiWrapperIterator(std::vector<Iterator*> iterators, int index)
            : iterators(std::move(iterators)), index(index)
    {

    }

    void setSelections(std::vector<uint32_t> columnIds,
                       std::vector<Selection> selections)
    {
        this->columnIds = std::move(columnIds);
        this->selections = std::move(selections);
        this->results.resize(this->selections.size());
    }

    bool getNext() override
    {
        assert(false);
        return false;
    }
    bool skipSameValue(const Selection& selection) override
    {
        assert(false);
        return false;
    }

    int32_t getColumnCount() override
    {
        assert(false);
        return 0;
    }

    int32_t getRowCount() override
    {
        assert(false);
        return 0;
    }

    uint64_t getValue(const Selection& selection) override
    {
        assert(false);
        return 0;
    }

    uint64_t getColumn(uint32_t column) override
    {
        assert(false);
        return 0;
    }

    void prepareIndexedAccess(const Selection& selection) override
    {
        assert(false);
    }

    void prepareSortedAccess(const Selection& selection) override
    {
        assert(false);
    }

    bool isSortedOn(const Selection& selection) override
    {
        assert(false);
        return false;
    }

    bool isJoin() override
    {
        assert(false);
        return true;
    }

    void iterateValue(const Selection& selection, uint64_t value) override
    {
        assert(false);
    }

    bool getValueMaybe(const Selection& selection, uint64_t& value) override
    {
        assert(false);
        return false;
    }

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) override
    {
        assert(false);
    }

    uint32_t getColumnForSelection(const Selection& selection) override
    {
        if (this->iterators.empty()) return 0;
        return this->iterators[0]->getColumnForSelection(selection);
    }

    void requireSelections(std::unordered_map<SelectionId, Selection> selections) override
    {
        for (auto& it: this->iterators)
        {
            it->requireSelections(selections);
        }
    }

    bool hasSelection(const Selection& selection) override
    {
        assert(false);
        return false;
    }

    uint32_t getFilterReduction() override
    {
        return 0;
        assert(false);
    }

    void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashTable& hashTable) override
    {
        assert(false);
    }

    void execute()
    {
#ifdef STATISTICS
        Timer timer;
#endif
        this->count = 0;
        this->iterators[this->index]->sumRows(this->results, this->columnIds, this->selections, this->count);
#ifdef STATISTICS
        this->time = timer.get();
#endif
    }
    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) override
    {
        count = this->count;
        for (int i = 0; i < static_cast<int32_t>(results.size()); i++)
        {
            results[i] += this->results[i];
        }
    }

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override
    {
        assert(false);
        return std::unique_ptr<Iterator>();
    }

    void save() override
    {
        assert(false);
    }

    void restore() override
    {
        assert(false);
    }

    void fillBindings(std::vector<uint32_t>& bindings) override
    {
        this->iterators[0]->fillBindings(bindings);
    }

    void dumpPlan(std::ostream& ss) override
    {
        ss << "MWI(";
        if (!this->iterators.empty())
        {
            this->iterators[0]->dumpPlan(ss);
        }
        ss << ")";
    }

    int64_t predictSize() override
    {
        return this->iterators[this->index]->predictSize();
    }

    void assignJoinSize(Database& database) override
    {
        this->iterators[0]->assignJoinSize(database);
    }

    void split(std::vector<std::unique_ptr<Iterator>>& container,
               std::vector<Iterator*>& groups,
               size_t count) override
    {
        assert(false);
    }

    bool hasBinding(uint32_t binding) override
    {
        return std::any_of(this->iterators.begin(), this->iterators.end(), [binding](Iterator* it) {
            return it->hasBinding(binding);
        });
    }

    std::vector<Iterator*> iterators;
    int index;
    std::vector<uint32_t> columnIds;
    std::vector<Selection> selections;
    std::vector<uint64_t> results;
    size_t count = 0;
    double time;
};
