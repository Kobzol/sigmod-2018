#pragma once

#include <assert.h>
#include <array>

#include "../relation/iterator.h"

namespace std
{
	template array<uint64_t, 1>;
	template array<uint64_t, 2>;
	template array<uint64_t, 3>;
	template array<uint64_t, 4>;
	template array<uint64_t, 5>;

	template<typename T, size_t N>
	struct hash<array<T, N> >
	{
		typedef array<T, N> argument_type;
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

class AggregateAbstract : public Iterator
{
public:
	explicit AggregateAbstract(Iterator* input, const std::vector<Selection>& groupBy, const std::vector<Selection>& sum, uint32_t binding);


	Iterator * input;
	std::vector<Selection> groupBy;
	std::vector<Selection> sum;
	uint32_t binding;
};

template <unsigned int GROUP_SIZE>
class Aggregate: public AggregateAbstract
{
public:
	explicit Aggregate(Iterator* input, const std::vector<Selection>& groupBy, const std::vector<Selection>& sum, uint32_t binding);

	bool getNext() override;

	void buildHashTable();

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
	

	std::unordered_map<std::array<uint64_t, GROUP_SIZE>, std::vector<uint64_t>> hashTable;
	typename std::unordered_map<std::array<uint64_t, GROUP_SIZE>, std::vector<uint64_t>>::iterator it;
	bool firstIt;
};

template class Aggregate<1>;
template class Aggregate<2>;
template class Aggregate<3>;
template class Aggregate<4>;
template class Aggregate<5>;

