#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "util.h"

using SelectionId = uint32_t;

class Selection
{
public:
    static SelectionId getId(uint32_t relation, uint32_t binding, uint32_t column)
    {
        return relation * 1000 + binding * 100 + column;
    }

    Selection() = default;
    Selection(uint32_t relation, uint32_t binding, uint32_t column)
            : relation(relation), binding(binding), column(column)
    {

    }

    uint32_t relation;
    uint32_t binding; // index of relation in query
    uint32_t column;

    SelectionId getId() const
    {
        return Selection::getId(this->relation, this->binding, this->column);
    }
};

struct SumColumn
{
public:
    SumColumn() = default;
    SumColumn(uint32_t sumIndex, uint32_t columnIndex)
            : sumIndex(sumIndex), columnIndex(columnIndex)
    {

    }

    uint32_t sumIndex;
    uint32_t columnIndex;
};

struct JoinPredicate
{
public:
    Selection selections[2];
};

// join is for two relations (bindings)
// all predicates have to share the same tuple (r_a, r_b) and differ only in columns
using Join = std::vector<JoinPredicate>;

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
    std::vector<uint32_t> relations; // real ids of relations
    std::vector<Join> joins;
    std::vector<Filter> filters;
    std::vector<Selection> selections;
    std::string result;
    size_t count;
};
