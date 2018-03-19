#include <assert.h>

#include "compute.h"

Compute::Compute(Iterator * input, std::vector<std::vector<Selection>>& exprs, uint32_t binding)
	: input(input), binding(binding), exprs(exprs)
{

}

bool Compute::getNext()
{
	if (input->getNext())
	{
		unsigned int pos = 0;
		for (auto& expr : exprs)
		{
			uint64_t val = 1;
			for (auto& selection : expr)
			{ 
				uint32_t i = input->getValue(selection);
				val *= i;
			}

			row[pos] = val;

			pos++;
		}

#ifdef WRITE_INTERMEDIATE_RESULTS
		writeRowToFile();
#endif

		return true;
	}
	return false;
}


void Compute::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
	for (auto& sel : selections)
	{
		*row++ = this->getValue(sel);
	}
}


bool Compute::getValueMaybe(const Selection & selection, uint64_t & value)
{
	if (selection.binding != this->binding) return false;
	value = this->getValue(selection);
	return true;
}

std::unique_ptr<Iterator> Compute::createIndexedIterator()
{
	assert(false);
	return std::unique_ptr<Iterator>();
}

void Compute::requireSelections(std::unordered_map<SelectionId, Selection> selections)
{
	for (auto& expr : exprs)
	{
		for (auto& selection : expr)
		{
			selections[selection.getId()] = selection;
		}
	}

	input->requireSelections(selections);
}

void Compute::sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds, size_t & count)
{
	while (this->getNext())
	{
		for (int index = 0; index < getColumnCount(); index++)
		{
			results[index] += this->getColumn(static_cast<uint32_t>(index));
		}
		count++;
	}
}

void Compute::printPlan(unsigned int level)
{
	printIndent(level);
	std::cout << "Compute <" << operatorIndex << "> [";

	bool b1 = false;
	for (auto expr : exprs)
	{
		if (b1)
		{
			std::cout << ", ";
		}

		bool b2 = false;
		for (auto selection : expr)
		{
			if (b2)
			{
				std::cout << " x ";
			}
			
			selection.print();

			b2 = true;
		}

		b1 = true;
	}

	std::cout << "] (";

	std::cout << std::endl;
	input->printPlan(level + 1);
	std::cout << std::endl;
	printIndent(level);
	std::cout << ")";
}

int64_t Compute::predictSize()
{
	return this->input->predictSize();
}
