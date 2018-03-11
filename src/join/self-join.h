#pragma once

#include "../relation/iterator.h"

class SelfJoin: public Iterator
{
public:
    SelfJoin(Iterator& inner, std::vector<Selection> selections);

    bool getNext() override;

    virtual int32_t getColumnCount() override;

    virtual uint64_t getValue(const Selection& selection) override;

    virtual uint64_t getColumn(uint32_t column) override;

    virtual bool getValueMaybe(const Selection& selection, uint64_t& value) override;

    virtual void fillRow(uint64_t* row, const std::vector<Selection>& selections) override;

    virtual void sumRow(std::vector<size_t>& sums, const std::vector<uint32_t>& columns) override;

    virtual uint32_t getColumnForSelection(const Selection& selection) override;

    virtual bool hasSelection(const Selection& selection) override;

    virtual void fillBindings(std::vector<uint32_t>& ids) override;

    Iterator& inner;
    std::vector<Selection> selections;
    int32_t selectionSize;
};
