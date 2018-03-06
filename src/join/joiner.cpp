#include "joiner.h"

void Joiner::setColumnMappings()
{
    uint32_t columnIndex = 0;

    auto r1ColumnCount = static_cast<int>(this->left->getColumnCount());
    for (int i = 0; i < r1ColumnCount; i++)
    {
        this->setColumn(this->left->getSelectionIdForColumn(static_cast<uint32_t>(i)), columnIndex++);
    }

    auto r2ColumnCount = static_cast<int>(this->right->getColumnCount());
    for (int i = 0; i < r2ColumnCount; i++)
    {
        this->setColumn(this->right->getSelectionIdForColumn(static_cast<uint32_t>(i)), columnIndex++);
    }
}

void Joiner::setColumn(SelectionId selectionId, uint32_t column)
{
    this->columnMap[selectionId] = column;
    this->columnMapReverted[column] = selectionId;
}
