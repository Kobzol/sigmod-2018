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
}

bool FilterView::getNext()
{
    // TODO: vectorize
    while (this->rowIndex < this->view->getRowCount())
    {
        this->rowIndex++;
        if (this->rowIndex < this->view->getRowCount() && this->passesFilters())
        {
            return true;
        }
    }

    return false;
}

bool FilterView::passesFilters()
{
    for (auto& filter: this->filters)
    {
        if (!passesFilter(filter, this->getValue(filter.selection, this->rowIndex))) return false;
    }

    return true;
}
