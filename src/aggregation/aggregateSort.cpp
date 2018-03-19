#include "aggregateSort.h"
#include <cstring>

AggregateSort::AggregateSort(ColumnRelation* relation, Selection groupBy, const std::vector<Selection>& sum,
                             uint32_t binding)
	: binding(binding), groupBy(groupBy), sum(sum), relation(relation)
{
	firstIt = true;
	
	this->sortIndex = &database.getSortIndex(groupBy.relation, groupBy.column);
	
	this->start = this->sortIndex->data.data();
	this->end = this->sortIndex->data.data() + this->sortIndex->data.size();
	iterateInit = true;

	getFromHashTable = false;
}

bool AggregateSort::getNext()
{
	if (!iterateInit || this->start >= this->end)
	{
		return false;
	}
	
	std::memset(currentRow, 0, sizeof(uint64_t) * MAX_ROW_SIZE);

	uint64_t value = this->start->value;

	//	printf("%d\n", value);
	
	currentRow[0] = value;

	while (this->start->value == value && this->start <= this->end)
	{
		currentRow[1]++;
		unsigned int j = 2;
		for (auto & selection : sum)
		{
			currentRow[j++] += relation->getValue(selection, this->start->row);
		}
		this->start++;
	}

	return true;
}

void AggregateSort::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
	for (auto& sel : selections)
	{
		*row++ = this->getValue(sel);
	}
}

bool AggregateSort::getValueMaybe(const Selection & selection, uint64_t & value)
{
	if (selection.binding != this->binding) return false;
	value = this->getValue(selection);
	return true;
}

std::unique_ptr<Iterator> AggregateSort::createIndexedIterator()
{
	assert(false);
	return std::unique_ptr<Iterator>();
}

void AggregateSort::printPlan(unsigned int level)
{
	bool b;

	printIndent(level);
	std::cout << "AggregateSort <" << operatorIndex << "> [";
	relation->printName();
	std::cout << " AS \"" << this->binding << "\"";
	std::cout << " | group by: ";
	groupBy.print();
	std::cout << " | sum: ";

	b = false;
	for (auto& selection : sum)
	{
		if (b)
		{
			std::cout << ", ";
		}
		selection.print();
		b = true;
	}

	std::cout << "]";
}

RowEntry* AggregateSort::toPtr(const std::vector<RowEntry>::iterator& iterator) const
{
	return this->sortIndex->data.data() + (iterator - this->sortIndex->data.begin());
}

void AggregateSort::iterateValue(const Selection & selection, uint64_t value)
{
	// Podporuju iteraci jen nad vybranou tabulkou.
	assert(selection.relation == relation->name);

	//htIterator = hashTable.find(value);
	//if (htIterator == hashTable.end())
	//{

	this->start = this->toPtr(std::lower_bound(this->sortIndex->data.begin(), this->sortIndex->data.end(), value,
		[](const RowEntry& entry, uint64_t val) {
		return entry.value < val;
	}));

	this->end = this->toPtr(std::upper_bound(this->sortIndex->data.begin(), this->sortIndex->data.end(), value,
		[](uint64_t val, const RowEntry& entry) {
		return val < entry.value;
	}));

	//getFromHashTable = false;
	//find = false;
	//}
	//else
	//{
	//	getFromHashTable = true;
	//	gotFromHashTable = false;
	//}

	iterateInit = true;
}

int64_t AggregateSort::predictSize()
{
    return this->relation->getRowCount();
}
