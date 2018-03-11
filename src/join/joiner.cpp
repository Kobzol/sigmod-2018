#include "joiner.h"

void Joiner::setColumn(SelectionId selectionId, uint32_t column)
{
    this->columnMap[column] = selectionId;
}

bool Joiner::hasSelection(const Selection& selection)
{
    if (this->right->hasSelection(selection)) return true;

    auto id = selection.getId();
    for (int i = 0; i < this->columnMapCols; i++)
    {
        if (this->columnMap[i] == id)
        {
            return true;
        }
    }

    return false;
}

std::unique_ptr<Iterator> Joiner::createIndexedIterator()
{
    assert(false);
    return std::unique_ptr<Iterator>();
}
