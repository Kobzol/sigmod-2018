#include "aggregate.h"

AggregateAbstract::AggregateAbstract(Iterator * input, const std::vector<Selection>& groupBy, const std::vector<Selection>& sum, uint32_t binding)
	: input(input), groupBy(std::move(groupBy)), sum(std::move(sum)), binding(binding)
{
}


template <unsigned int GROUP_SIZE>
Aggregate<GROUP_SIZE>::Aggregate(Iterator * input, const std::vector<Selection>& groupBy, const std::vector<Selection>& sum, uint32_t binding)
	: AggregateAbstract(input, groupBy, sum, binding)
{
	firstIt = true;
}

template <unsigned int GROUP_SIZE>
void Aggregate<GROUP_SIZE>::buildHashTable()
{
	// Musime mit pripraveny vhodny operator.
	assert(groupBy.size() == GROUP_SIZE);

	hashTable.clear();
	while (input->getNext())
	{
		// Nachystame klic do hash tabulky.
		std::array<uint64_t, GROUP_SIZE> key;
		for (uint32_t i = 0; i < GROUP_SIZE; i++)
		{
			key[i] = input->getValue(groupBy[i]);
		}

		auto* vec = (std::vector<uint64_t>*)&hashTable[key];

		// Nastavime vektor na pozadovanou velikost (sumy + jeden sloupec jako pocet).
		vec->resize(sum.size() + 1);

		auto iter = vec->begin();

		// Navyseni poctu.
		(*iter._Ptr)++;
		iter++;
		
		// Pocitani sum.
		for (uint32_t j = 0; j < sum.size(); j++)
		{
			(*iter._Ptr) += input->getValue(sum[j]);
			iter++;
		}
	}
}


template <unsigned int GROUP_SIZE>
bool Aggregate<GROUP_SIZE>::getNext()
{
	if (firstIt)
	{
		buildHashTable();
		it = hashTable.begin();
		firstIt = false;

		if (it != hashTable.end())
		{
			//writeRowToFile();

			return true;
		}
		else
		{
			return false;
		}
	}

	if (it != hashTable.end())
	{
		it++;

		if (it != hashTable.end())
		{
//#ifdef WRITE_INTERMEDIATE_RESULTS
			//writeRowToFile();
//#endif
			return true;
		}
		else
		{
			return false;
		}
	}

	return false;
}

template<unsigned int GROUP_SIZE>
void Aggregate<GROUP_SIZE>::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
	for (auto& sel : selections)
	{
		*row++ = this->getValue(sel);
	}
}

template <unsigned int GROUP_SIZE>
bool Aggregate<GROUP_SIZE>::getValueMaybe(const Selection & selection, uint64_t & value)
{
	if (selection.binding != this->binding) return false;
	value = this->getValue(selection);
	return true;
}

template <unsigned int GROUP_SIZE>
std::unique_ptr<Iterator> Aggregate<GROUP_SIZE>::createIndexedIterator()
{
	assert(false);
	return std::unique_ptr<Iterator>();
}

template <unsigned int GROUP_SIZE>
void Aggregate<GROUP_SIZE>::printPlan(unsigned int level)
{
	bool b;

	printIndent(level);
	std::cout << "Aggregate <" << operatorIndex << "> [group by: ";

	b = false;
	for (auto& selection : groupBy)
	{
		if (b)
		{
			std::cout << ", ";
		}
		selection.print();
		b = true;
	}

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

	std::cout << "] (";

	std::cout << std::endl;
	input->printPlan(level + 1);
	std::cout << std::endl;
	printIndent(level);
	std::cout << ")";
}
