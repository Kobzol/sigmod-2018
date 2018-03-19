#pragma once

#include "settings.h"
#include "bloom-filter.h"

using TableRow = std::vector<uint64_t>;

class HashTable
{
public:
    TableRow* insertRow(uint64_t value, uint32_t size)
    {
#ifdef USE_BLOOM_FILTER
        this->bloomFilter.set(value);
#endif
        return &this->table[value];
    }
    TableRow* getRow(uint64_t value)
    {
#ifdef USE_BLOOM_FILTER
        if (!this->bloomFilter.has(value)) return nullptr;
#endif
        auto it = this->table.find(value);
        if (it == this->table.end()) return nullptr;
        return &it->second;
    }

    BloomFilter<BLOOM_FILTER_SIZE> bloomFilter;
    HashMap<uint64_t, TableRow> table;
};
