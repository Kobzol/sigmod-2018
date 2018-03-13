#pragma once

#include <vector>
#include <cstddef>
#include <bitset>

template <size_t size>
class BloomFilter
{
public:
    void set(uint64_t value)
    {
        this->data[value % size] = true;
    }
    bool has(uint64_t value)
    {
        return this->data[value % size];
    }

    std::bitset<size> data;
};
