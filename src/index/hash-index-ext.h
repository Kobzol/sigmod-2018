#pragma once

#include <unordered_map>
#include <vector>
#include <atomic>

#include "../util.h"
#include "../settings.h"

class ColumnRelation;

class HashIndexEntry
{
public:
	std::vector<uint64_t> rowIDs;
	std::vector<uint64_t> sum;
};

/**
 * Index for a given relation and column.
 * Maps value to a list of row ids.
 */
class HashIndexExt
{
public:
	HashIndexExt(ColumnRelation& relation, uint32_t column);
    DISABLE_COPY(HashIndexExt);

    void build();

    std::unordered_map<uint64_t, HashIndexEntry> hashTable;
    ColumnRelation& relation;
    uint32_t column;

    std::atomic_flag buildStarted = ATOMIC_FLAG_INIT;
    std::atomic<bool> buildCompleted { false };
};
