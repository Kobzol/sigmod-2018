#pragma once

#include "../query.h"
#include "view.h"
#include "../util.h"
#include "column-relation.h"

class FilterView : public View
{
public:
    explicit FilterView(ColumnRelation* view): view(view)
    {

    }
    DISABLE_COPY(FilterView);

    bool getNext() override;
    bool passesFilters();

    int64_t getColumnCount() override
    {
        return this->view->getColumnCount();
    }

    int64_t getRowCount() override
    {
        return this->view->getRowCount();
    }

    uint64_t getValue(const Selection& index, int row) override
    {
        return this->view->getValue(row, index.column);
    }

    ColumnRelation* view;
    std::vector<Filter> filters;
};
