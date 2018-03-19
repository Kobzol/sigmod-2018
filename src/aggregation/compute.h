#pragma once

#include "../relation/iterator.h"

class Compute: public Iterator
{
public:
	static const unsigned int MAX_ROW_SIZE = 64;

	explicit Compute(Iterator* input, std::vector<std::vector<Selection>>& exprs, uint32_t binding);

	bool getNext() override;

	uint64_t getValue(const Selection& selection) final
	{
		return row[selection.column];
	}

	uint64_t getColumn(uint32_t column) final
	{
		return row[column];
	}

	virtual int64_t predictSize() override;

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
		return exprs.size();
	}

	void fillBindings(std::vector<uint32_t>& ids) final
	{
		ids.push_back(this->binding);
	}

	std::unique_ptr<Iterator> createIndexedIterator() override;

	void requireSelections(std::unordered_map<SelectionId, Selection> selections) override;

	void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t& count) final;

	void printPlan(unsigned int level) override;
	

	Iterator* input;
	uint32_t binding;
	std::vector<std::vector<Selection>> exprs;

	uint64_t row[MAX_ROW_SIZE];
};


