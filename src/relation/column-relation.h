#pragma once

#include <cstdint>
#include <cstddef>
#include "../util.h"
#include "../query.h"
#include "iterator.h"

class ColumnRelation
{
public:
    ColumnRelation() = default;
    DISABLE_COPY(ColumnRelation);
    ENABLE_MOVE(ColumnRelation);

    uint64_t tupleCount;
    uint32_t columnCount;
    uint64_t* data;
    uint32_t id;

    uint64_t getValue(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }
    uint64_t& getValueMutable(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }

    uint32_t getColumnCount()
    {
        return this->columnCount;
    }

    int64_t getRowCount()
    {
        return this->tupleCount;
    }

    uint64_t getValue(const Selection& selection, int row)
    {
        return this->getValue(row, selection.column);
    }
};

class ColumnRelationIterator: public Iterator
{
public:
    explicit ColumnRelationIterator(ColumnRelation* relation, uint32_t binding)
            : relation(relation), binding(binding)
    {

    }

    bool getNext() override
    {
        this->rowIndex++;
        return this->rowIndex < this->relation->getRowCount();
    }

    uint64_t getValue(const Selection& selection) override
    {
        return this->relation->getValue(this->rowIndex, selection.column);
    }
    uint64_t getColumn(uint32_t column) override
    {
        return this->relation->getValue(this->rowIndex, column);
    }
    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return Selection::getId(this->relation->id, this->binding, column);
    }

    uint32_t getColumnForSelection(const Selection& selection) override
    {
        return selection.column;
    }

    bool getValueMaybe(const Selection& selection, uint64_t& value) override
    {
        if (selection.binding != this->binding) return false;
        value = this->getValue(selection);
        return true;
    }

    int32_t getColumnCount() override
    {
        return this->relation->getColumnCount();
    }

    void fillBindings(std::vector<uint32_t>& ids) override
    {
        ids.push_back(this->binding);
    }

    ColumnRelation* relation;
    uint32_t binding;
};
