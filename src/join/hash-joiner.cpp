#include "hash-joiner.h"
#include "../relation/row-relation.h"

#include <unordered_map>
#include <iostream>

static std::vector<uint32_t> intersect(const std::vector<uint32_t> active, const std::vector<uint32_t> added)
{
    if (active.empty()) return added;

    int indexL = 0;
    int indexR = 0;
    std::vector<uint32_t> result;

    auto activeSize = static_cast<int>(active.size());
    auto addedSize = static_cast<int>(added.size());

    while (true)
    {
        if (indexL >= activeSize) return result;
        if (indexR >= addedSize) return result;

        if (active[indexL] == added[indexR])
        {
            result.push_back(active[indexL]);
            indexL++, indexR++;
        }
        else if (active[indexL] < added[indexR]) indexL++;
        else indexR++;
    }
}

std::unique_ptr<View> HashJoiner::join(View& r1, View& r2, const std::vector<Join>& joins)
{
    r1.reset();
    r2.reset();

    // TODO: special path for same-relation joins
    auto columnCount = (int) (r1.getColumnCount() + r2.getColumnCount());
    auto result = std::make_unique<RowRelation>(columnCount);
    std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> hashes(joins.size());

    this->setColumnMappings(r1, r2, joins, *result);

    this->fillHashTable(r1, joins, hashes);
    this->createRows(r1, r2, joins, *result, hashes);

    return result;
}

void HashJoiner::setColumnMappings(View& r1, View& r2, const std::vector<Join>& joins, RowRelation& result)
{
    uint32_t columnIndex = 0;

    auto r1ColumnCount = static_cast<int>(r1.getColumnCount());
    for (int i = 0; i < r1ColumnCount; i++)
    {
        result.setColumn(Selection(joins[0].selections[0].relation, static_cast<uint32_t>(i)), columnIndex++);
    }

    auto r2ColumnCount = static_cast<int>(r2.getColumnCount());
    for (int i = 0; i < r2ColumnCount; i++)
    {
        result.setColumn(Selection(joins[0].selections[1].relation, static_cast<uint32_t>(i)), columnIndex++);
    }
}

void HashJoiner::createRows(View& r1, View& r2, const std::vector<Join>& joins, RowRelation& result,
                            const std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>>& hashes)
{
    auto joinSize = (int) joins.size();
    std::vector<uint32_t> activeRows;

    r2.reset();
    while (r2.getNext())
    {
        activeRows.clear();
        bool matches = true;
        for (int i = 0; i < joinSize; i++)
        {
            uint64_t value = r2.getValue(joins[i].selections[1], r2.rowIndex);
            auto it = hashes[i].find(value);
            if (it == hashes[i].end())
            {
                matches = false;
                break;
            }
            else activeRows = intersect(activeRows, it->second);
        }

        if (matches)
        {
            for (auto row: activeRows)
            {
                auto* rowData = result.addRow();
                for (int i = 0; i < r1.getColumnCount(); i++)
                {
                    auto selection = Selection(joins[0].selections[0].relation, i);
                    *rowData = r1.getValue(selection, row);
                    rowData++;
                }
                for (int i = 0; i < r2.getColumnCount(); i++)
                {
                    auto selection = Selection(joins[0].selections[1].relation, i);
                    *rowData = r2.getValue(selection, r2.rowIndex);
                    rowData++;
                }
            }
        }
    }
}

void HashJoiner::fillHashTable(View& relation, const std::vector<Join>& joins,
                               std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>>& hashes)
{
    uint32_t row = 0;
    auto joinSize = (int) joins.size();

    relation.reset();
    while (relation.getNext())
    {
        for (int i = 0; i < joinSize; i++)
        {
            auto& join = joins[i];
            uint64_t value = relation.getValue(join.selections[0], relation.rowIndex);
            hashes[i][value].push_back(row);
        }

        row++;
    }
}
