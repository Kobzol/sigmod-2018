#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <xmmintrin.h>

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
    uint64_t* transposed = nullptr;
    uint32_t id;

    uint64_t getValue(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }
    uint64_t& getValueMutable(size_t row, size_t column)
    {
        return this->data[column * this->tupleCount + row];
    }
    uint64_t getValueTransposed(size_t row, size_t column)
    {
        return this->transposed[row * this->columnCount + column];
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

/**
 * Simple relation iterator.
 */
class ColumnRelationIterator: public Iterator
{
public:
    explicit ColumnRelationIterator(ColumnRelation* relation, uint32_t binding)
            : relation(relation), binding(binding), end(static_cast<int>(relation->getRowCount()))
    {

    }
    explicit ColumnRelationIterator(ColumnRelation* relation, uint32_t binding, int start, int end)
            : relation(relation), binding(binding), end(end)
    {
        this->rowIndex = start;
    }

    bool getNext() override
    {
        this->rowIndex++;

#ifdef TRANSPOSE_RELATIONS
        _mm_prefetch(this->relation->data + this->rowIndex * this->relation->columnCount, _MM_HINT_T0);
#endif
        return this->rowIndex < this->end;
    }

    uint64_t getValue(const Selection& selection) override
    {
        return this->relation->getValue(static_cast<size_t>(this->rowIndex), selection.column);
    }
    uint64_t getColumn(uint32_t column) override
    {
        return this->relation->getValue(static_cast<size_t>(this->rowIndex), column);
    }

    int32_t getRowCount() override;

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) final;

    uint32_t getColumnForSelection(const Selection& selection) final
    {
        return selection.column;
    }

    bool hasSelection(const Selection& selection) final
    {
        return this->binding == selection.binding;
    }

    bool getValueMaybe(const Selection& selection, uint64_t& value) override;

    int32_t getColumnCount() final
    {
        return this->relation->getColumnCount();
    }

    std::unique_ptr<Iterator> createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                    const Selection& selection) override;

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                         const std::vector<Selection>& selections, size_t& count) override;

    void fillBindings(std::vector<uint32_t>& bindings) final
    {
        bindings.push_back(this->binding);
    }

    int64_t predictSize() override
    {
        return this->relation->getRowCount();
    }

    bool hasBinding(uint32_t binding) final
    {
        return binding == this->binding;
    }

    virtual std::unique_ptr<ColumnRelationIterator> createForRange(int start, int end)
    {
        return std::make_unique<ColumnRelationIterator>(this->relation, this->binding, start, end);
    }

    void split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
               size_t count) override;

    void dumpPlan(std::ostream& ss) override
    {
        ss << "CI(" << this->relation->getRowCount() << ")";
    }

    ColumnRelation* relation;
    uint32_t binding;
    int end;
};
