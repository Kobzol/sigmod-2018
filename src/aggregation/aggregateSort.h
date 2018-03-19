#pragma once

#include <assert.h>
#include <array>
#include <cstdint>

#include "../relation/iterator.h"
#include "../relation/column-relation.h"
#include "../database.h"

#include "aggregateAbstract.h"

class AggregateSort: public AggregateAbstract
{
public:
	explicit AggregateSort(ColumnRelation* relation, Selection groupBy, const std::vector<Selection>& sum, uint32_t binding);

	bool getNext() override;

	uint64_t getValue(const Selection& selection) final
	{
		return currentRow[selection.column];
	}

	uint64_t getColumn(uint32_t column) final
	{
		return currentRow[column];
	}

	void fillRow(uint64_t* row, const std::vector<Selection>& selections) final;
	
	uint32_t getColumnForSelection(const Selection& selection) final
	{
		return selection.column;
	}

	bool hasSelection(const Selection& selection) final
	{
		return this->binding == selection.binding;
	}

	bool getValueMaybe(const Selection& selection, uint64_t& value) final;

	int32_t getColumnCount() final
	{
		return 2 + sum.size();
	}

	void fillBindings(std::vector<uint32_t>& ids) final
	{
		ids.push_back(this->binding);
	}

	std::unique_ptr<Iterator> createIndexedIterator() override;

	uint32_t getCountColumnIndex() final
	{
		return 1;
	}

	void printPlan(unsigned int level) override;


	bool firstIt;
	uint32_t binding;
	Selection groupBy;
	std::vector<Selection> sum;
	
	static const unsigned int MAX_ROW_SIZE = 64;
	uint64_t currentRow[MAX_ROW_SIZE];

	ColumnRelation* relation;
	SortIndex* sortIndex;
	RowEntry* start;
	RowEntry* end;
};
