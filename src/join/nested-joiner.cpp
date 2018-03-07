#include "nested-joiner.h"

/*NestedJoiner::NestedJoiner(View* left, View* right, const std::vector<Join>& joins): Joiner(left, right, joins)
{

}

bool NestedJoiner::NestedJoinIterator::getNext()
{
    if (!this->initialized)
    {
        this->initialize();
    }

    while (true)
    {
        if (this->rightExhausted)
        {
            this->iterators[1]->reset();
            if (!this->iterators[0]->getNext()) return false;
            this->rightExhausted = false;
        }

        if (!this->iterators[1]->getNext())
        {
            this->rightExhausted = true;
            continue;
        }

        bool matches = true;
        for (auto& join : this->joiner.joins)
        {
            auto left = this->iterators[0]->getValue(join.selections[0]);
            auto right = this->iterators[1]->getValue(join.selections[1]);
            if (left != right)
            {
                matches = false;
                break;
            }
        }

        if (!matches)
        {
            continue;
        }

        auto* rowData = &this->row[0];
        for (int i = 0; i < this->r1->getColumnCount(); i++)
        {
            *rowData++ = this->iterators[0]->getColumn(i);
        }
        for (int i = 0; i < r2->getColumnCount(); i++)
        {
            *rowData++ = this->iterators[1]->getColumn(i);
        }

        return true;
    }

    return false;
}

uint64_t NestedJoiner::NestedJoinIterator::getValue(const Selection& selection)
{
    uint32_t column = this->joiner.columnMap[selection.getId()];
    return this->row[column];
}

uint64_t NestedJoiner::NestedJoinIterator::getColumn(uint32_t column)
{
    return this->row[column];
}

void NestedJoiner::NestedJoinIterator::initialize()
{
    this->iterators[0] = this->joiner.left->createIterator();
    this->iterators[1] = this->joiner.right->createIterator();

    this->initialized = true;
}
*/