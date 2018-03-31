#pragma once

#include "iterator.h"

#include <algorithm>
#include <atomic>
#include <omp.h>

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

    Iterator* inner;
};

class MultiWrapperIterator: public Iterator
{
public:
    explicit MultiWrapperIterator(std::vector<Iterator*> iterators): iterators(std::move(iterators))
    {

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

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) override
    {
        std::vector<std::vector<uint64_t>> subResults(PARALLEL_JOIN_THREADS);
        for (auto& subResult: subResults)
        {
            subResult.resize(results.size());
        }

        std::atomic<size_t> localCount{0};

#pragma omp parallel for num_threads(PARALLEL_JOIN_THREADS) schedule(dynamic)
        for (int i = 0; i < static_cast<int32_t>(this->iterators.size()); i++)
        {
            size_t c = 0;
            this->iterators[i]->sumRows(subResults[omp_get_thread_num()], columnIds, selections, c);
            localCount += c;
        }

        for (auto& subResult: subResults)
        {
            for (int r = 0; r < static_cast<int32_t>(results.size()); r++)
            {
                results[r] += subResult[r];
            }
        }

        count = localCount;
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
        assert(false);
        return 0;
    }

    void assignJoinSize(Database& database) override
    {
        assert(false);
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
};
