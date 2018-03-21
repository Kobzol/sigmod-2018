#include "self-join.h"

SelfJoin::SelfJoin(Iterator* inner, std::vector<Selection> selections):
    WrapperIterator(inner), selections(std::move(selections))
{
    assert(this->selections.size() > 1);
    this->selectionSize = static_cast<int32_t>(this->selections.size() - 1);
}

bool SelfJoin::getNext()
{
    while (this->inner->getNext())
    {
        uint64_t value = this->inner->getValue(this->selections[0]);
        bool ok = true;
        for (int i = 1; i < this->selectionSize; i++)
        {
            if (this->inner->getValue(this->selections[i]) != value)
            {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }

    return false;
}

void SelfJoin::dumpPlan(std::ostream& ss)
{
    ss << "SJ(";
    this->inner->dumpPlan(ss);
    ss << ")";
}

void SelfJoin::printPlan(unsigned int level)
{
	printIndent(level);
	std::cout << "SelfJoin <" << operatorIndex << "> (";

	// TODO... vypsat podminky.

	std::cout << std::endl;
	inner.printPlan(level + 1);

	std::cout << std::endl;
	printIndent(level);
	std::cout << ")";
}