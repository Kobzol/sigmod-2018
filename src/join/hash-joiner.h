#pragma once

#include <memory>
#include <unordered_map>
#include "../relation/column-relation.h"
#include "../relation/row-relation.h"

class HashJoiner
{
public:
    std::unique_ptr<View> join(View& r1, View& r2, const std::vector<Join>& joins);

    void fillHashTable(View& relation, const std::vector<Join>& joins,
                       std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>>& hashes);

    void createRows(View& r1, View& r2, const std::vector<Join>& joins, RowRelation& result,
                        const std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>>& hashes);

    void setColumnMappings(View& r1, View& r2, const std::vector<Join>& joins, RowRelation& result);
};
