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
    uint32_t r1id = joins[0].selections[0].relation;
    uint32_t r2id = joins[0].selections[1].relation;

    auto r1ColumnCount = static_cast<int>(r1.getColumnCount());
    for (int i = 0; i < r1ColumnCount; i++)
    {
        result.setColumn(r1.getColumnId(r1id, static_cast<uint32_t>(i)), columnIndex++);
    }

    auto r2ColumnCount = static_cast<int>(r2.getColumnCount());
    for (int i = 0; i < r2ColumnCount; i++)
    {
        result.setColumn(r2.getColumnId(r2id, static_cast<uint32_t>(i)), columnIndex++);
    }
}

void HashJoiner::createRows(View& r1, View& r2, const std::vector<Join>& joins, RowRelation& result,
                            const std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>>& hashes)
{
    auto joinSize = (int) joins.size();
    std::vector<uint32_t> activeRows;
    uint32_t r1id = joins[0].selections[0].relation;
    uint32_t r2id = joins[0].selections[1].relation;

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
                    *rowData++ = r1.getValueAt(static_cast<uint32_t>(i), row);
                }
                for (int i = 0; i < r2.getColumnCount(); i++)
                {
                    *rowData++ = r2.getValueAt(static_cast<uint32_t>(i), r2.rowIndex);
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
