#pragma once

#include "../relation/iterator.h"

class SelfJoin: public Iterator
{
public:
    SelfJoin(Iterator& inner, std::vector<Selection> selections);

    bool getNext() final;

    int32_t getColumnCount() final;

    uint64_t getValue(const Selection& selection) final;
    uint64_t getColumn(uint32_t column) final;
    bool getValueMaybe(const Selection& selection, uint64_t& value) final;

    void fillRow(uint64_t* row, const std::vector<Selection>& selections) final;
    void sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns) final;

    uint32_t getColumnForSelection(const Selection& selection) final;

    bool hasSelection(const Selection& selection) final;
    void fillBindings(std::vector<uint32_t>& ids) final;

    Iterator& inner;
    std::vector<Selection> selections;
    int32_t selectionSize;
};
