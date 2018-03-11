#include <cassert>
#include "self-join.h"

SelfJoin::SelfJoin(Iterator& inner, std::vector<Selection> selections):
    inner(inner), selections(std::move(selections))
{
    assert(selections.size() > 1);
    this->selectionSize = static_cast<int32_t>(selections.size() - 1);
}

bool SelfJoin::getNext()
{
    while (this->inner.getNext())
    {
        uint64_t value = this->inner.getValue(this->selections[0]);
        bool ok = true;
        for (int i = 1; i < this->selectionSize; i++)
        {
            if (this->inner.getValue(this->selections[i]) != value)
            {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }

    return false;
}

int32_t SelfJoin::getColumnCount()
{
    return this->inner.getColumnCount();
}

uint64_t SelfJoin::getValue(const Selection& selection)
{
    return this->inner.getValue(selection);
}

uint64_t SelfJoin::getColumn(uint32_t column)
{
    return this->inner.getColumn(column);
}

bool SelfJoin::getValueMaybe(const Selection& selection, uint64_t& value)
{
    return this->inner.getValueMaybe(selection, value);
}

void SelfJoin::fillRow(uint64_t* row, const std::vector<Selection>& selections)
{
    this->inner.fillRow(row, selections);
}

uint32_t SelfJoin::getColumnForSelection(const Selection& selection)
{
    return this->inner.getColumnForSelection(selection);
}

bool SelfJoin::hasSelection(const Selection& selection)
{
    return this->inner.hasSelection(selection);
}

void SelfJoin::fillBindings(std::vector<uint32_t>& ids)
{
    this->inner.fillBindings(ids);
}
