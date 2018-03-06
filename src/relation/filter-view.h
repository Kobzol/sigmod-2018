#pragma once

#include "../query.h"
#include "view.h"
#include "../util.h"
#include "column-relation.h"

class FilterView: public View
{
    class FilterViewIterator: public Iterator
    {
    public:
        explicit FilterViewIterator(FilterView* filter);

        bool getNext() override;
        uint64_t getValue(const Selection& selection) override;
        uint64_t getColumn(uint32_t column) override;

    private:
        bool passesFilters();

        FilterView* filter;
    };

public:
    explicit FilterView(ColumnRelation* view): view(view)
    {

    }
    DISABLE_COPY(FilterView);

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

    std::unique_ptr<Iterator> createIterator() override
    {
        return std::make_unique<FilterViewIterator>(this);
    }
    SelectionId getSelectionIdForColumn(uint32_t column) override
    {
        return this->view->getSelectionIdForColumn(column);
    }

    void fillRelationIds(std::vector<uint32_t>& ids) override
    {
        this->view->fillRelationIds(ids);
    }

    ColumnRelation* view;
    std::vector<Filter> filters;
};
