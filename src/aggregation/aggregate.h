#pragma once

#include <cassert>
#include <array>
#include <cstdint>

#include "../relation/iterator.h"
#include "aggregateAbstract.h"

namespace std
{
	template<typename T, size_t N>
	struct hash<std::array<T, N> >
	{
		typedef std::array<T, N> argument_type;
		typedef size_t result_type;

		result_type operator()(const argument_type& a) const
		{
			hash<T> hasher;
			result_type h = 0;
			for (result_type i = 0; i < N; ++i)
			{
				h = h * 31 + hasher(a[i]);
			}
			return h;
		}
	};
}



template <unsigned int GROUP_SIZE>
class Aggregate: public AggregateAbstract
{
public:
	explicit Aggregate(Iterator* input, const std::vector<Selection>& groupBy, const std::vector<Selection>& sum, uint32_t binding);

	bool getNext() override;

	void buildHashTable();

	void fillHashTable(const Selection& hashSelection, const std::vector<Selection>& selections,
							   HashTable& hashTable) override;

	virtual int64_t predictSize() override;

	uint64_t getValue(const Selection& selection) final
	{
		// Hodnoty group by.
		if (selection.column < GROUP_SIZE)
		{
			return it->first[selection.column];
		}
		else
		// Hodnota pro pocet a sumy.
		{
			return it->second[selection.column - GROUP_SIZE];
		}
	}

	uint64_t getColumn(uint32_t column) final
	{
		// Hodnoty group by.
		if (column < GROUP_SIZE)
		{
			return it->first[column];
		}
		else
			// Hodnota pro pocet a sumy.
		{
			return it->second[column - GROUP_SIZE];
		}
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
		return groupBy.size() + 1 + sum.size();
	}

	void fillBindings(std::vector<uint32_t>& ids) final
	{
		ids.push_back(this->binding);
	}

	std::unique_ptr<Iterator> createIndexedIterator() override;


	void printPlan(unsigned int level) override;
	
	uint32_t getCountColumnIndex() final
	{
		return groupBy.size();
	}

	Iterator * input;
	std::vector<Selection> groupBy;
	std::vector<Selection> sum;
	uint32_t binding;

	std::unordered_map<std::array<uint64_t, GROUP_SIZE>, std::vector<uint64_t>> hashTable;
	typename std::unordered_map<std::array<uint64_t, GROUP_SIZE>, std::vector<uint64_t>>::iterator it;
	bool firstIt;
};

template class Aggregate<1>;
template class Aggregate<2>;
template class Aggregate<3>;
template class Aggregate<4>;
template class Aggregate<5>;
