#include "joiner.h"

void Joiner::setColumn(SelectionId selectionId, uint32_t column)
{
    this->columnMap[column] = selectionId;
}
