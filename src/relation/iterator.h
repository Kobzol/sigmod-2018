#pragma once

#include <cstdint>
#include <unordered_map>
#include "../query.h"
#include "../util.h"

class Iterator
{
public:
    Iterator() = default;
    virtual ~Iterator() = default;
    DISABLE_COPY(Iterator);

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

    virtual bool getValueMaybe(const Selection& selection, uint64_t& value) = 0;

    virtual void fillRow(uint64_t* row, const std::vector<Selection>& selections) = 0;

    virtual uint32_t getColumnForSelection(const Selection& selection) = 0;
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

    virtual void fillBindings(std::vector<uint32_t>& ids) = 0;

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

    int rowIndex = -1;
};
