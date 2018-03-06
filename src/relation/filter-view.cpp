#include "filter-view.h"

#include <cassert>

static bool passesFilter(const Filter& filter, uint64_t value)
{
    switch (filter.oper)
    {
        case '=': return value == filter.value;
        case '<': return value < filter.value;
        case '>': return value > filter.value;
        default: assert(false);
    }

    return false;
}

FilterView::FilterViewIterator::FilterViewIterator(FilterView* filter) : filter(filter)
{

}

bool FilterView::FilterViewIterator::getNext()
{
    this->rowIndex++;

    auto rowCount = this->filter->view->getRowCount();
    while (this->rowIndex < rowCount && !this->passesFilters())
    {
        this->rowIndex++;
    }

    return this->rowIndex < rowCount;
}

uint64_t FilterView::FilterViewIterator::getValue(const Selection& selection)
{
    return this->filter->view->getValue(selection, this->rowIndex);
}
bool FilterView::FilterViewIterator::passesFilters()
{
    for (auto& filter: this->filter->filters)
    {
        if (!passesFilter(filter, this->filter->view->getValue(filter.selection, this->rowIndex))) return false;
    }

    return true;
}

uint64_t FilterView::FilterViewIterator::getColumn(uint32_t column)
{
    return this->filter->view->getValue(this->rowIndex, column);
}
