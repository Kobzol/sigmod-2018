#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>

#include "../util.h"
#include "../query.h"
#include "../settings.h"
#include "iterator.h"

class ColumnRelation
{
public:
    ColumnRelation() = default;
    DISABLE_COPY(ColumnRelation);
    ENABLE_MOVE(ColumnRelation);

    uint32_t columnCount;
    uint32_t cumulativeColumnId;
    uint64_t tupleCount;
    uint64_t* data;

    uint64_t getValue(size_t row, size_t column)
    {
#ifdef TRANSPOSE_RELATIONS
        return this->data[row * this->columnCount + column];
#else
        return this->data[column * this->tupleCount + row];
#endif
    }
    uint64_t& getValueMutable(size_t row, size_t column)
    {
#ifdef TRANSPOSE_RELATIONS
        return this->data[row * this->columnCount + column];
#else
        return this->data[column * this->tupleCount + row];
#endif
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

    bool getNext() override;

    uint64_t getValue(const Selection& selection) final
    {
        return this->relation->getValue(static_cast<size_t>(this->rowIndex), selection.column);
    }
    uint64_t getColumn(uint32_t column) final
    {
        return this->relation->getValue(static_cast<size_t>(this->rowIndex), column);
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
        return this->relation->getColumnCount();
    }

    void fillBindings(std::vector<uint32_t>& ids) final
    {
        ids.push_back(this->binding);
    }

    ColumnRelation* relation;
    uint32_t binding;
};
