#pragma once

#include <cstdint>
#include <unordered_map>
#include <memory>
#include <cassert>
#include <sstream>

#include "../query.h"
#include "../util.h"
#include "../bloom-filter.h"
#include "../hash-table.h"

class Database;

class Iterator
{
public:
    Iterator();
    virtual ~Iterator() = default;
    DISABLE_COPY(Iterator);

    /**
     * Moves to the next row.
     * After this method is called, getValue and getColumn should return values from the current row.
     * @return True if a row was found, false if no rows are available
     */
    virtual bool getNext() = 0;
    virtual bool skipSameValue(const Selection& selection)
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
    virtual void prepareSortedAccess(const Selection& selection)
    {

    }
    virtual bool isSortedOn(const Selection& selection)
    {
        return false;
    }
	virtual void prepareIterateValue()
	{

	}

    virtual bool isJoin()
    {
        return false;
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
    virtual void requireSelections(std::unordered_map<SelectionId, Selection> selections)
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
                               HashTable& hashTable)
    {
        auto columnMapCols = selections.size();
        auto countSub = static_cast<size_t>(selections.size() - 1);

        if (!this->getNext()) return;

        uint64_t value = this->getValue(hashSelection);
        auto vec = hashTable.insertRow(value, static_cast<uint32_t>(columnMapCols));

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
                vec = hashTable.insertRow(value, static_cast<uint32_t>(columnMapCols));
            }
        }
    }

    virtual void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                         const std::vector<Selection>& selections, size_t& count)
    {

    }

    /**
     * Creates an indexed version of this iterator.
     * It must support iterateValue.
     */
    virtual std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container) = 0;

	virtual bool supportsIterateValue()
	{
		return false;
	}

    /**
     * Saves the state of the iterator.
     */
    virtual void save()
    {
        this->savedRowIndex = this->rowIndex;
    }

    /**
     * Restores the state of the iterator;
     */
    virtual void restore()
    {
        this->rowIndex = this->savedRowIndex;
    }

    virtual void fillBindings(std::vector<uint32_t>& bindings)
    {

    }

    virtual void dumpPlan(std::ostream& ss)
    {

    }

    virtual int64_t predictSize() = 0;

    virtual void assignJoinSize(Database& database)
    {

    }

    /**
     * Splits the iterator in up to @p count subiterators and store them into @p groups.
     */
    virtual void split(std::vector<std::unique_ptr<Iterator>>& groups, size_t count)
    {
        assert(false);
    }

    virtual bool hasBinding(uint32_t binding)
    {
        return false;
    }

    int rowIndex = -1;
    int savedRowIndex;


	void printIndent(unsigned int level)
	{
		for (unsigned int i = 0; i < level; i++)
		{
			std::cout << "  ";
		}
	}



	virtual void printPlan(unsigned int level)
    {

    }

	void printResult();

	virtual void writeRowToFile();

	static unsigned int operatorIndexCounter;
	unsigned int operatorIndex;
};
