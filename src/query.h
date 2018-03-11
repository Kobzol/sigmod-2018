#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>

#include "util.h"
#include "settings.h"

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

    bool operator==(const Selection& other) const
    {
        return this->getId() == other.getId();
    }

    SelectionId getId() const
    {
        return Selection::getId(this->relation, this->binding, this->column);
    }

    uint32_t relation;
    uint32_t binding; // index of relation in query
    uint32_t column;
};

class UnionFind
{
public:
    explicit UnionFind(Selection selection): selection(selection)
    {
        this->parent = this;
    }
    DISABLE_COPY(UnionFind);

    UnionFind* findParent()
    {
        if (this->parent != this)
        {
            this->parent = this->parent->findParent();
        }
        return this->parent;
    }
    void join(UnionFind* other)
    {
        auto a = this->findParent();
        auto b = other->findParent();

        if (a == b) return;

        if (a->rank < b->rank)
        {
            a->parent = b;
        }
        else if (a->rank > b->rank)
        {
            b->parent = a;
        }
        else
        {
            a->parent = b;
            b->rank++;
        }
    }

    uint32_t rank = 0;
    UnionFind* parent;
    Selection selection;
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

    std::unordered_map<uint32_t, std::vector<Selection>> selfJoins; // binding to self-join selections

#ifdef STATISTICS
    size_t count;
    std::string input;
#endif
};
