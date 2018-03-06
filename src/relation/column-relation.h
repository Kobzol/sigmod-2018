#pragma once

#include <cstdint>
#include <cstddef>
#include "view.h"
#include "../util.h"

class ColumnRelation: public View
{
    class ColumnRelationIterator: public Iterator
    {
    public:
        explicit ColumnRelationIterator(ColumnRelation* relation): relation(relation)
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

        ColumnRelation* relation;
    };

public:
    ColumnRelation() = default;
    DISABLE_COPY(ColumnRelation);
    ENABLE_MOVE(ColumnRelation);

    uint64_t tupleCount;
    uint64_t columnCount;
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

    int64_t getColumnCount() override
    {
        return this->columnCount;
    }

    int64_t getRowCount() override
    {
        return this->tupleCount;
    }

    uint64_t getValue(const Selection& selection, int row) override
    {
        return this->getValue(row, selection.column);
    }
    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return Selection::getId(this->id, column);
    }

    std::unique_ptr<Iterator> createIterator() override
    {
        return std::make_unique<ColumnRelationIterator>(this);
    }

    void fillRelationIds(std::vector<uint32_t>& ids) override
    {
        ids.push_back(this->id);
    }
};
