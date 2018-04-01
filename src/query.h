#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <limits>
#include <sstream>

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
    static Selection aggregatedCount(uint32_t binding)
    {
        return {0, binding, 50};
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

    bool isUnique() const;

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
    Filter(const Selection& selection, uint64_t value, bool(*evaluator)(uint64_t), char oper,
           uint64_t valueMax = std::numeric_limits<uint64_t>::max())
            : selection(selection), value(value), valueMax(valueMax), evaluator(evaluator), oper(oper)
    {

    }

    bool isSkippable() const;
    size_t getInterval() const;

    bool operator<(const Filter& rhs) const {
        return this->getInterval() < rhs.getInterval();
    }
    bool operator==(const Filter& rhs) const {
        return this->selection == rhs.selection &&
               this->oper == rhs.oper &&
               this->value == rhs.value;
    }

    Selection selection;
    uint64_t value;
    uint64_t valueMax;
    bool(*evaluator)(uint64_t);
    char oper;
};

class Query
{
public:
    bool isAggregable() const;
    void fillBindings(std::vector<uint32_t>& bindings);
    void dump(std::ostream& os);

    void setImpossible(bool impossible)
    {
        if (!this->impossible)
        {
            this->impossible = impossible;
        }
    }
    void fillImpossible()
    {
        std::stringstream ss;
        for (int i = 0; i < static_cast<int32_t>(this->selections.size()); i++)
        {
            ss << "NULL ";
        }
        this->result = ss.str();
        this->result[this->result.size() - 1] = '\n';
    }

    std::vector<uint32_t> relations; // real ids of relations
    std::vector<Join> joins;
    std::vector<Filter> filters;
    std::vector<Selection> selections;
    std::string result;

    std::unordered_map<uint32_t, std::vector<Selection>> selfJoins; // binding to self-join selections
    bool impossible = false;

#ifdef STATISTICS
    std::string input;
    std::string plan;
    double time;
    int id;
    uint64_t predicted;
#endif

private:
    mutable bool aggregable = false;
};
