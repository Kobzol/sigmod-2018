#pragma once

#include <cstdint>
#include <unordered_map>
#include <memory>

#include "../query.h"
#include "../util.h"

class Iterator
{
public:
    Iterator() = default;
    virtual ~Iterator() = default;
    DISABLE_COPY(Iterator);

    /**
     * Moves to the next row.
     * After this method is called, getValue and getColumn should return values from the current row.
     * @return True if a row was found, false if no rows are available
     */
    virtual bool getNext() = 0;
    virtual bool skipSameValue()
    {
        return this->getNext();
    }

    virtual int32_t getColumnCount() = 0;
    virtual int32_t getRowCount()
    {
        return 0;
    }
    virtual uint64_t getValue(const Selection& selection) = 0;
    virtual uint64_t getColumn(uint32_t column) = 0;

    virtual void prepareIndexedAccess()
    {

    }

    /**
     * Resets the iterator and tells it to iterate rows where the given selection equals the given value.
     */
    virtual void iterateValue(const Selection& selection, uint64_t value)
    {

    }

    virtual bool getValueMaybe(const Selection& selection, uint64_t& value) = 0;

    virtual void fillRow(uint64_t* row, const std::vector<Selection>& selections)
    {

    }

    virtual uint32_t getColumnForSelection(const Selection& selection) = 0;

    /**
     * Initializes the selection to column mappings and tells the iterator which selections are needed.
     * After this method completes, getColumnForSelection may be called on this iterator.
     */
    virtual void requireSelections(std::unordered_map<SelectionId, Selection>& selections)
    {

    }
    virtual bool hasSelection(const Selection& selection) = 0;

    /*
     * Returns a value that states how much this iterator is filtered.
     */
    virtual uint32_t getFilterReduction()
    {
        return 0;
    }

    // assumes sorted rows (has to be used with hash or sort index)
    virtual void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
                       HashMap<uint64_t, std::vector<uint64_t>>& hashTable)
    {
        auto columnMapCols = selections.size();
        auto countSub = static_cast<size_t>(selections.size() - 1);

        if (!this->getNext()) return;

        uint64_t value = this->getValue(hashSelection);
        auto* vec = &hashTable[value];

        while (true)
        {
            vec->resize(vec->size() + columnMapCols);
            auto rowData = &vec->back() - countSub;
            this->fillRow(rowData, selections);

            if (!this->getNext()) return;
            uint64_t current = this->getValue(hashSelection);
            if (current != value)
            {
                value = current;
                vec = &hashTable[value];
            }
        }
    }

    virtual void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count)
    {

    }

    /**
     * Creates an indexed version of this iterator.
     * It must support iterateValue.
     */
    virtual std::unique_ptr<Iterator> createIndexedIterator() = 0;

    int rowIndex = -1;
};
