#pragma once

#include <cstdint>
#include <vector>

class Selection
{
public:
    Selection() = default;
    Selection(uint32_t relation, uint32_t column) : relation(relation), column(column)
    {

    }

    uint32_t relation;
    uint32_t column;

    uint32_t getId() const
    {
        return this->relation * 1000 + this->column;
    }
};

class Join
{
public:
    Selection selections[2];
};

class Filter
{
public:
    Filter() = default;
    Filter(const Selection& selection, char oper, uint64_t value) : selection(selection), oper(oper), value(value)
    {

    }

    Selection selection;
    char oper;
    uint64_t value;
};

class Query
{
public:
    std::vector<uint32_t> relations;
    std::vector<Join> joins;
    std::vector<Filter> filters;
    std::vector<Selection> selections;
};
