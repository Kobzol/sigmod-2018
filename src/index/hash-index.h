#pragma once

#include <unordered_map>
#include <vector>
#include <atomic>

#include "../util.h"
#include "../settings.h"
#include "index.h"

/**
 * Index for a given relation and column.
 * Maps value to a list of row ids.
 */
class HashIndex: public Index
{
public:
    HashIndex(ColumnRelation& relation, uint32_t column);

    bool build() final;

    std::unordered_map<uint64_t, std::vector<uint64_t>> hashTable; // value to rowid
};
